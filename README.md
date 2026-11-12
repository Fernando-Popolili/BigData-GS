# AviRonda: Big Data & Data Integration (Global Solution FIAP 2026.2, CitizenTech)

Carrinho autônomo que faz a ronda do galpão avícola, mede calor e qualidade do ar na altura das aves e avisa o granjeiro. Este repositório contém o componente **Dados** do MVP.

## Integrantes

> Obrigatório: nome completo e RM de todos.

| Nome completo | RM |
|---|---|
| Fernando Popolili Lameira Pires | RM99919 |
| Lucas Palamartschuk de Toledo   | RM97913 |
| Augusto Fisco Milreu            | RM98245 |
| David Guilherme Barchin Denunci | RM98603 |
| Matheus Zanardi                 | RM98832 |

## Arquitetura

```
Open-Meteo (API) ─┐
                  ├─► Airflow 3 (DAG gs_coleta) ─► chegada/ ─► Volume Databricks ─► Bronze ─► Prata ─► Ouro ─► MVP
Carrinho (simul.) ┘
```

Detalhes das fontes, do JSON e da justificativa da simulação: [docs/contrato_de_dados.md](docs/contrato_de_dados.md).

## Como reproduzir (Linux)

Pré-requisitos: Docker com Docker Compose, Python 3.10+.

```bash
# 1. Sobe o Airflow 3 e monta a pasta chegada/
bash scripts/setup_airflow.sh

# 2. Acesse http://localhost:8080 (airflow / airflow) e ative a DAG gs_coleta
#    Opcional: Admin > Variables: gs_chegada_dir, gs_webhook_url

# 3. Os arquivos aparecem em chegada/clima/ e chegada/carrinho/

# 4. Testes do gerador (sem Airflow)
pip install pytest requests
python -m pytest tests
```

## Segurança

Nenhuma chave, senha ou dado pessoal real neste repositório. Configurações sensíveis ficam em Airflow Variables.

## Databricks (Bronze, Prata e Ouro)

Requer uma conta Databricks Free Edition (serverless).

1. Crie o volume `workspace.gs.chegada` com as pastas `clima` e `carrinho`.
2. Envie os arquivos de `chegada/` ao Volume em 2 lotes, de dias diferentes
   (lote 1: 01 a 06/11; lote 2: 07 a 11/11).
3. Importe os notebooks de `databricks/` e execute na ordem:
   - `01_bronze.py`: Auto Loader incremental (rode após o lote 1, de novo sem novos arquivos e após o lote 2, trocando `ETAPA`)
   - `02_prata.py`: tipos, MERGE e 3 regras de qualidade
   - `03_ouro.py`: modelo estrela, LGPD e as 5 consultas SQL
4. Crie o painel (Dashboards) lendo `workspace.gs.ouro_status_zona`.
