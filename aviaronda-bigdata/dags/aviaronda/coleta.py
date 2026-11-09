"""Funções de coleta usadas pela DAG gs_coleta.

Cada função é idempotente: reexecutar o mesmo dia sobrescreve o mesmo arquivo.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from . import config, gerador

log = logging.getLogger(__name__)


def _gravar_atomico(destino: Path, texto: str) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".tmp")
    tmp.write_text(texto, encoding="utf-8")
    tmp.replace(destino)
    return destino


def caminho_clima(chegada_dir: str | Path, regiao: str, ds: str) -> Path:
    return Path(chegada_dir) / "clima" / f"clima_{regiao.lower()}_{ds}.json"


def caminho_carrinho(chegada_dir: str | Path, regiao: str, ds: str) -> Path:
    galpao = config.LOCAIS[regiao]["galpao_id"]
    return Path(chegada_dir) / "carrinho" / f"carrinho_{galpao}_{ds}.json"


def baixar_clima(regiao: str, ds: str, chegada_dir: str | Path, tentativas: int = 3) -> str:
    """Baixa o clima horário do dia na Open-Meteo (sem chave) e grava em chegada/clima/."""
    local = config.LOCAIS[regiao]
    params = {
        "latitude": local["latitude"],
        "longitude": local["longitude"],
        "start_date": ds,
        "end_date": ds,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": config.TIMEZONE,
    }
    ultimo_erro: Exception | None = None
    for n in range(1, tentativas + 1):
        try:
            resp = requests.get(config.OPEN_METEO_URL, params=params, timeout=30)
            resp.raise_for_status()
            dados = resp.json()
            if not (dados.get("hourly") or {}).get("time"):
                raise ValueError("resposta sem dados horários")
            break
        except (requests.RequestException, ValueError) as erro:
            ultimo_erro = erro
            log.warning("Open-Meteo %s/%s, tentativa %d/%d falhou: %s", regiao, ds, n, tentativas, erro)
            time.sleep(2 * n)
    else:
        raise RuntimeError(f"Open-Meteo indisponível para {regiao}/{ds}") from ultimo_erro

    dados["origem"] = {
        "local": regiao,
        "municipio": local["municipio"],
        "data_ref": ds,
        "coletado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    destino = _gravar_atomico(caminho_clima(chegada_dir, regiao, ds), json.dumps(dados, ensure_ascii=False))
    log.info("Clima gravado: %s", destino)
    return str(destino)


def gerar_carrinho(ds: str, chegada_dir: str | Path) -> list[str]:
    """Gera as leituras simuladas do carrinho para cada galpão, usando o clima real do dia."""
    dia = date.fromisoformat(ds)
    gravados = []
    for regiao in config.LOCAIS:
        arquivo_clima = caminho_clima(chegada_dir, regiao, ds)
        clima = None
        if arquivo_clima.exists():
            clima = json.loads(arquivo_clima.read_text(encoding="utf-8"))
        else:
            log.warning("Sem clima real para %s/%s; usando perfil sintético", regiao, ds)
        registros = gerador.gerar_dia(dia, regiao, clima)
        destino = gerador.escrever_jsonl(registros, caminho_carrinho(chegada_dir, regiao, ds))
        log.info("Carrinho gravado: %s (%d leituras)", destino, len(registros))
        gravados.append(str(destino))
    return gravados


def conferir_chegada(ds: str, chegada_dir: str | Path) -> dict:
    """Confere se todos os arquivos esperados do dia chegaram e não estão vazios."""
    esperados = []
    for regiao in config.LOCAIS:
        esperados.append(caminho_clima(chegada_dir, regiao, ds))
        esperados.append(caminho_carrinho(chegada_dir, regiao, ds))

    ausentes = [str(p) for p in esperados if not p.exists() or p.stat().st_size == 0]
    if ausentes:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios em {ds}: {ausentes}")

    minimo = config.RONDAS_POR_DIA * config.ZONAS
    resumo = {}
    for regiao in config.LOCAIS:
        arquivo = caminho_carrinho(chegada_dir, regiao, ds)
        linhas = sum(1 for _ in arquivo.open(encoding="utf-8"))
        if linhas < minimo:
            raise ValueError(f"{arquivo.name}: {linhas} leituras, esperado ao menos {minimo}")
        resumo[arquivo.name] = linhas
    log.info("Chegada conferida para %s: %s", ds, resumo)
    return resumo
