"""DAG gs_coleta (Airflow 3): coleta diária que alimenta a pasta chegada/.

inicio >> [coletar_clima_sul, coletar_clima_sudeste] (paralelo) >> gerar_carrinho >> conferir_chegada

Configuração por Airflow Variables (nunca no código):
  gs_chegada_dir   pasta de pouso (padrão /opt/airflow/chegada)
  gs_webhook_url   opcional; recebe aviso de falha (Discord/Slack/Teams compatível com {"content": ...})
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from aviaronda import coleta, config

log = logging.getLogger(__name__)


def _chegada_dir() -> str:
    return Variable.get("gs_chegada_dir", default=config.CHEGADA_PADRAO)


def notificar_falha(context) -> None:
    """Registra a falha e, se houver webhook configurado, avisa a equipe."""
    ti = context.get("task_instance")
    mensagem = f"[AviRonda] Falha na tarefa {getattr(ti, 'task_id', '?')} (run {context.get('run_id')})"
    log.error(mensagem)
    url = Variable.get("gs_webhook_url", default=None)
    if url:
        try:
            requests.post(url, json={"content": mensagem}, timeout=10)
        except requests.RequestException as erro:
            log.error("Não foi possível notificar: %s", erro)


def t_clima(regiao: str, ds: str | None = None, **_):
    return coleta.baixar_clima(regiao, ds, _chegada_dir())


def t_carrinho(ds: str | None = None, **_):
    return coleta.gerar_carrinho(ds, _chegada_dir())


def t_conferir(ds: str | None = None, **_):
    return coleta.conferir_chegada(ds, _chegada_dir())


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária: clima real (Open-Meteo) e leituras do carrinho AviRonda",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notificar_falha,
    },
    tags=["gs", "aviaronda", "bigdata"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    clima_sul = PythonOperator(
        task_id="coletar_clima_sul", python_callable=t_clima, op_kwargs={"regiao": "SUL"}
    )
    clima_sudeste = PythonOperator(
        task_id="coletar_clima_sudeste", python_callable=t_clima, op_kwargs={"regiao": "SUDESTE"}
    )
    carrinho = PythonOperator(task_id="gerar_carrinho", python_callable=t_carrinho)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=t_conferir)

    inicio >> [clima_sul, clima_sudeste] >> carrinho >> conferir
