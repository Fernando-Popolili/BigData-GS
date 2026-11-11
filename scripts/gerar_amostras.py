"""Gera dados simulados sem Airflow (sem rede): útil para testar e criar a pasta amostras/.

Uso: python scripts/gerar_amostras.py 2026-11-01 2026-11-03 amostras
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from aviaronda import config, gerador  # noqa: E402


def main(inicio: str, fim: str, saida: str) -> None:
    dia, ultimo = date.fromisoformat(inicio), date.fromisoformat(fim)
    while dia <= ultimo:
        for regiao, local in config.LOCAIS.items():
            registros = gerador.gerar_dia(dia, regiao)
            destino = Path(saida) / f"carrinho_{local['galpao_id']}_{dia.isoformat()}.json"
            gerador.escrever_jsonl(registros, destino)
            print(destino, len(registros))
        dia += timedelta(days=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
