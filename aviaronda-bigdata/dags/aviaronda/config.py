"""Configuração central do pipeline AviRonda.

Troque coordenadas, galpões e parâmetros aqui; o restante do código lê deste arquivo.
"""
from datetime import date

# Granjas simuladas. Coordenadas aproximadas dos municípios (podem ser trocadas).
# SUL = risco de umidade/amônia; SUDESTE = risco de calor (os dois modos de risco do El Niño).
LOCAIS = {
    "SUL": {
        "galpao_id": "G-SUL-01",
        "municipio": "Chapecó",
        "uf": "SC",
        "latitude": -27.10,
        "longitude": -52.61,
        "lote_inicio": date(2026, 10, 1),
    },
    "SUDESTE": {
        "galpao_id": "G-SDE-01",
        "municipio": "Rio Claro",
        "uf": "SP",
        "latitude": -22.41,
        "longitude": -47.56,
        "lote_inicio": date(2026, 10, 10),
    },
}

ZONAS = 6                # zona 1 = entrada de ar, zona 6 = perto do exaustor
RONDAS_POR_DIA = 12      # uma ronda a cada 2 horas
CICLO_LOTE_DIAS = 42     # duração simulada de um lote de corte
COM_AMONIA = False       # True somente se o carrinho tiver sensor de amônia
SEED = 2026              # semente: mesma data + mesma semente = mesmos dados (idempotente)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "America/Sao_Paulo"
CHEGADA_PADRAO = "/opt/airflow/chegada"
