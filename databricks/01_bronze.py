# Databricks notebook source
# MAGIC %md
# MAGIC # AviRonda · Bronze (requisito 05)
# MAGIC
# MAGIC Carga **incremental** dos arquivos de `chegada/` com **Auto Loader**.
# MAGIC
# MAGIC - Só entram arquivos **novos** (o checkpoint lembra o que já foi lido).
# MAGIC - Rodar de novo **não duplica** nada.
# MAGIC - A Bronze guarda o dado cru: o Auto Loader lê os campos JSON como texto. Os tipos corretos ficam para a Prata.
# MAGIC - Cada linha ganha `arquivo_origem` e `carregado_em`.
# MAGIC
# MAGIC **Como usar para gerar as contagens do relatório**
# MAGIC 1. Com o lote 1 no volume (01 a 06/11), deixe `ETAPA = "1_apos_lote1"` na última célula e execute o notebook.
# MAGIC 2. Execute de novo, sem arquivos novos, com `ETAPA = "2_reexecucao_sem_novos"`. As contagens **não mudam**.
# MAGIC 3. Suba o lote 2 (07 a 11/11), troque para `ETAPA = "3_apos_lote2"` e execute. Só os arquivos novos entram.
# MAGIC 4. A tabela `workspace.gs.controle_contagens` guarda as três fotografias: tire o print dela.

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
SCHEMA = "workspace.gs"
FONTES = ("clima", "carrinho")

# COMMAND ----------

def carregar_bronze(fonte: str) -> None:
    """Auto Loader incremental de chegada/<fonte> para workspace.gs.bronze_<fonte>."""
    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"{SCHEMA}.bronze_{fonte}")
    )
    consulta.awaitTermination()


def registrar_contagens(etapa: str) -> None:
    """Guarda uma 'fotografia' das contagens da Bronze em controle_contagens."""
    linhas = []
    for fonte in FONTES:
        df = spark.table(f"{SCHEMA}.bronze_{fonte}")
        linhas.append((etapa, fonte, df.count(), df.select("arquivo_origem").distinct().count()))
    (
        spark.createDataFrame(linhas, "etapa string, fonte string, linhas long, arquivos long")
        .withColumn("registrado_em", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(f"{SCHEMA}.controle_contagens")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

ETAPA = "1_apos_lote1"   # troque para "2_reexecucao_sem_novos" e depois "3_apos_lote2"

for fonte in FONTES:
    carregar_bronze(fonte)

registrar_contagens(ETAPA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências para o relatório

# COMMAND ----------

# Print 1: contagens de cada etapa (antes/depois do 2º lote e da 2ª execução)
display(spark.table(f"{SCHEMA}.controle_contagens").orderBy("registrado_em", "fonte"))

# COMMAND ----------

# Print 2: linhas por arquivo (cada arquivo aparece uma vez, sem duplicação de carga)
display(
    spark.table(f"{SCHEMA}.bronze_carrinho")
    .groupBy("arquivo_origem").count().orderBy("arquivo_origem")
)

# COMMAND ----------

# Print 3: amostra e esquema da Bronze (dado cru, tudo como texto)
display(spark.table(f"{SCHEMA}.bronze_carrinho").limit(5))
spark.table(f"{SCHEMA}.bronze_clima").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que esperar
# MAGIC
# MAGIC | Etapa | `bronze_clima` | `bronze_carrinho` |
# MAGIC |---|---|---|
# MAGIC | 1_apos_lote1 | 12 linhas, 12 arquivos (1 linha por arquivo, com listas horárias) | cerca de 870 linhas, 12 arquivos |
# MAGIC | 2_reexecucao_sem_novos | igual à etapa 1 | igual à etapa 1 |
# MAGIC | 3_apos_lote2 | 22 linhas, 22 arquivos | cerca de 1.600 linhas, 22 arquivos |
# MAGIC
# MAGIC O carrinho tem 72 leituras por arquivo (12 rondas × 6 zonas), mais cerca de 1% de duplicados criados de propósito pelo gerador, que a Prata remove.
