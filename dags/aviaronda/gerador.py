"""Gerador de leituras simuladas do carrinho AviRonda.

POR QUE SIMULAR: o carrinho ainda não tem histórico de rondas em galpões reais.
O briefing aceita dados simulados desde que a geração seja justificada e documentada
(veja docs/contrato_de_dados.md). A simulação é determinística: a mesma data e a mesma
semente geram o mesmo arquivo, então reexecutar a DAG não muda os dados.

COMO SIMULAR (resumo):
- O clima externo vem da API real Open-Meteo (quando disponível); sem ele, usa-se um perfil sintético.
- A temperatura interna é amortecida em relação à externa (galpão semiclimatizado) e depende da
  idade do lote e da zona (o ar esquenta em direção ao exaustor).
- CO2 cresce com a idade do lote e ao longo do galpão.
- Cerca de 6% das rondas têm um "ponto crítico" numa zona aleatória (calor + CO2 altos).
- Anomalias intencionais (nulos, valores absurdos, duplicados) alimentam as regras de qualidade da Prata.

Os coeficientes são aproximações para fins de demonstração, não calibrações científicas.
"""
from __future__ import annotations

import json
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from . import config


def idade_lote(dia: date, inicio: date) -> tuple[int, int]:
    """Retorna (numero_do_ciclo, idade_em_dias) para o dia informado."""
    decorridos = (dia - inicio).days
    if decorridos < 0:
        raise ValueError(f"{dia} é anterior ao início do lote ({inicio})")
    ciclo = decorridos // config.CICLO_LOTE_DIAS + 1
    idade = decorridos % config.CICLO_LOTE_DIAS + 1
    return ciclo, idade


def temperatura_alvo(idade: int) -> float:
    """Temperatura de conforto aproximada (cai com a idade). Substituir pela tabela da Embrapa."""
    return max(23.0, 32.0 - 0.2 * (idade - 1))


def extrair_externo(clima: dict | None) -> dict[int, tuple[float, float]]:
    """Converte a resposta da Open-Meteo em {hora: (temperatura, umidade)}."""
    if not clima:
        return {}
    horario = clima.get("hourly") or {}
    saida: dict[int, tuple[float, float]] = {}
    for t, temp, umid in zip(
        horario.get("time", []),
        horario.get("temperature_2m", []),
        horario.get("relative_humidity_2m", []),
    ):
        if temp is None or umid is None:
            continue
        saida[datetime.fromisoformat(t).hour] = (float(temp), float(umid))
    return saida


def externo_sintetico(hora: int, regiao: str, rng: random.Random) -> tuple[float, float]:
    """Perfil diário sintético usado quando não há dado real da Open-Meteo (pico às 15h)."""
    media = 18.0 if regiao == "SUL" else 24.0
    temp = media + 5.0 * math.sin((hora - 9) / 24 * 2 * math.pi)
    umid = min(100.0, max(20.0, 75.0 - 2.0 * (temp - media) + rng.gauss(0, 4)))
    return temp, umid


def gerar_dia(
    dia: date,
    regiao: str,
    clima: dict | None = None,
    seed: int | None = None,
    com_amonia: bool | None = None,
) -> list[dict]:
    """Gera todas as leituras de um galpão em um dia."""
    seed = config.SEED if seed is None else seed
    com_amonia = config.COM_AMONIA if com_amonia is None else com_amonia
    local = config.LOCAIS[regiao]
    galpao = local["galpao_id"]
    ciclo, idade = idade_lote(dia, local["lote_inicio"])
    lote_id = f"{galpao}-L{ciclo:02d}"
    alvo = temperatura_alvo(idade)
    externo = extrair_externo(clima)
    rng = random.Random(f"{regiao}|{dia.isoformat()}|{seed}")
    passo_horas = 24 // config.RONDAS_POR_DIA

    registros: list[dict] = []
    for r in range(config.RONDAS_POR_DIA):
        hora = r * passo_horas
        t_ext, u_ext = externo.get(hora) or externo_sintetico(hora, regiao, rng)
        zona_critica = rng.randint(1, config.ZONAS) if rng.random() < 0.06 else None
        inicio = datetime(dia.year, dia.month, dia.day, hora) + timedelta(minutes=rng.randint(0, 10))
        ronda_id = f"{galpao}-{dia.isoformat()}T{hora:02d}00"

        for zona in range(1, config.ZONAS + 1):
            ts = inicio + timedelta(minutes=zona * 2, seconds=rng.randint(0, 40))
            temp = alvo + 0.5 * (t_ext - alvo) + 0.45 * (zona - 1) + rng.gauss(0, 0.4)
            umid = 35 + 0.45 * u_ext + 0.8 * (zona - 1) + rng.gauss(0, 2)
            co2 = 900 + 25 * idade + 90 * (zona - 1) + rng.gauss(0, 80)
            nh3 = None
            if com_amonia:
                nh3 = 0.25 * idade * (1 + 0.15 * (zona - 1)) * (umid / 70) + rng.gauss(0, 1)
            if zona == zona_critica:
                temp += rng.uniform(3, 5)
                umid += 5
                co2 += rng.uniform(900, 1500)
                if nh3 is not None:
                    nh3 += rng.uniform(15, 40)

            registro = {
                "ronda_id": ronda_id,
                "galpao_id": galpao,
                "lote_id": lote_id,
                "idade_lote_dias": idade,
                "zona": zona,
                "ts": ts.isoformat(timespec="seconds"),
                "temp_c": round(temp, 2),
                "umid_pct": round(min(95.0, max(30.0, umid)), 1),
                "co2_ppm": int(round(co2)),
                "nh3_ppm": None if nh3 is None else round(max(0.0, nh3), 1),
                "sensor": "SCD40",
                "origem": "simulado",
            }

            # Anomalias intencionais (alimentam as regras de qualidade da Prata).
            sorteio = rng.random()
            if sorteio < 0.015:
                registro["temp_c"] = None                      # falha de leitura
            elif sorteio < 0.025:
                registro["co2_ppm"] = rng.choice([-1, 65000])  # valor absurdo
            elif sorteio < 0.033:
                registro["temp_c"] = rng.choice([85.0, -40.0])  # valor absurdo

            registros.append(registro)
            if rng.random() < 0.01:
                registros.append(dict(registro))               # duplicado
    return registros


def escrever_jsonl(registros: list[dict], destino: Path) -> Path:
    """Grava JSON Lines (um objeto por linha) de forma atômica."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for registro in registros:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
    tmp.replace(destino)
    return destino
