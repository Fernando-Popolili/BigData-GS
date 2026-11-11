import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from aviaronda import coleta, config, gerador  # noqa: E402

CAMPOS = {
    "ronda_id", "galpao_id", "lote_id", "idade_lote_dias", "zona", "ts",
    "temp_c", "umid_pct", "co2_ppm", "nh3_ppm", "sensor", "origem",
}


def test_idade_lote():
    inicio = date(2026, 10, 1)
    assert gerador.idade_lote(date(2026, 10, 1), inicio) == (1, 1)
    assert gerador.idade_lote(date(2026, 11, 11), inicio) == (1, 42)
    assert gerador.idade_lote(date(2026, 11, 12), inicio) == (2, 1)


def test_data_anterior_ao_lote_falha():
    with pytest.raises(ValueError):
        gerador.idade_lote(date(2026, 9, 1), date(2026, 10, 1))


def test_deterministico():
    a = gerador.gerar_dia(date(2026, 11, 5), "SUL")
    b = gerador.gerar_dia(date(2026, 11, 5), "SUL")
    assert a == b


def test_volume_e_campos():
    registros = gerador.gerar_dia(date(2026, 11, 5), "SUDESTE")
    assert len(registros) >= config.RONDAS_POR_DIA * config.ZONAS
    assert all(set(r) == CAMPOS for r in registros)


def test_sem_amonia_por_padrao():
    registros = gerador.gerar_dia(date(2026, 11, 5), "SUL")
    assert all(r["nh3_ppm"] is None for r in registros)


def test_com_amonia():
    registros = gerador.gerar_dia(date(2026, 11, 5), "SUL", com_amonia=True)
    assert any(r["nh3_ppm"] is not None for r in registros)


def test_anomalias_para_regras_de_qualidade():
    registros = []
    for i in range(14):
        registros += gerador.gerar_dia(date(2026, 11, 1) + timedelta(days=i), "SUL")
    assert any(r["temp_c"] is None for r in registros)
    assert any(r["co2_ppm"] in (-1, 65000) for r in registros)
    assert any(r["temp_c"] in (85.0, -40.0) for r in registros)
    chaves = [(r["galpao_id"], r["zona"], r["ts"]) for r in registros]
    assert len(chaves) > len(set(chaves))  # há duplicados


def test_usa_clima_real_quando_fornecido():
    clima = {"hourly": {
        "time": [f"2026-11-05T{h:02d}:00" for h in range(24)],
        "temperature_2m": [40.0] * 24,
        "relative_humidity_2m": [50.0] * 24,
    }}
    quente = gerador.gerar_dia(date(2026, 11, 5), "SUL", clima=clima)
    sintetico = gerador.gerar_dia(date(2026, 11, 5), "SUL")
    media = lambda rs: sum(r["temp_c"] for r in rs if r["temp_c"] and 0 < r["temp_c"] < 60) / len(rs)
    assert media(quente) > media(sintetico)


def test_conferir_chegada(tmp_path):
    ds = "2026-11-05"
    with pytest.raises(FileNotFoundError):
        coleta.conferir_chegada(ds, tmp_path)

    for regiao in config.LOCAIS:
        arquivo_clima = coleta.caminho_clima(tmp_path, regiao, ds)
        arquivo_clima.parent.mkdir(parents=True, exist_ok=True)
        arquivo_clima.write_text("{}", encoding="utf-8")
    coleta.gerar_carrinho(ds, tmp_path)
    resumo = coleta.conferir_chegada(ds, tmp_path)
    assert len(resumo) == len(config.LOCAIS)
