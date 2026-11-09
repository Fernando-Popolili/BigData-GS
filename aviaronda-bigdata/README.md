# AviRonda: Big Data & Data Integration (Global Solution FIAP 2026.2, CitizenTech)

Carrinho autônomo que faz a ronda do galpão avícola, mede calor e qualidade do ar na altura das aves e avisa o granjeiro. Este repositório contém o componente **Dados** do MVP.

## Integrantes

> Obrigatório: nome completo e RM de todos.

| Nome completo | RM |
|---|---|
| (preencher) | (preencher) |

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

## Status

- [x] Passo 1: contrato de dados
- [x] Passo 2: gerador de leituras simuladas
- [x] Passo 3: DAG do Airflow
- [ ] Passo 4: upload ao Volume do Databricks (2 lotes)
- [ ] Passo 5: Bronze
- [ ] Passo 6: Prata
- [ ] Passo 7: Ouro e 5 consultas SQL
- [ ] Passo 8: LGPD e contrato com o MVP

## Segurança

Nenhuma chave, senha ou dado pessoal real neste repositório. Configurações sensíveis ficam em Airflow Variables.
