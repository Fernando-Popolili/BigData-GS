# Databricks notebook source
# MAGIC %md
# MAGIC # AviRonda · Prata (requisito 06)
# MAGIC
# MAGIC Da Bronze (dado cru, tudo texto) para a Prata (dado limpo e confiável).
# MAGIC
# MAGIC **O que este notebook faz**
# MAGIC 1. **Tipos corretos:** texto vira número, data e hora.
# MAGIC 2. **Clima:** o campo `hourly` (texto JSON com listas paralelas) vira uma linha por hora (`from_json` + `arrays_zip` + `explode`).
# MAGIC 3. **Duplicados** (mesma chave `galpao_id + zona + ts`) são removidos, mantendo a carga mais recente.
# MAGIC 4. **3 regras de qualidade** no carrinho. Cada linha barrada vai para a tabela de rejeitados com o motivo, e o total de cada regra é contado.
# MAGIC 5. **MERGE** pela chave: rodar de novo não duplica nada.
# MAGIC
# MAGIC | Regra | O que barra |
# MAGIC |---|---|
# MAGIC | R1_temp_nula | leitura sem temperatura (falha do sensor) |
# MAGIC | R2_temp_fora_da_faixa | temperatura fora de -10 °C a 50 °C (valor físico impossível para um galpão) |
# MAGIC | R3_co2_invalido | CO2 nulo ou fora de 300 a 10.000 ppm |
# MAGIC
# MAGIC Cada linha é classificada por **uma única regra** (a primeira que falha), então os totais somam sem contar duas vezes.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.gs"

# Faixas físicas plausíveis (usadas nas regras de qualidade)
TEMP_MIN, TEMP_MAX = -10.0, 50.0
CO2_MIN, CO2_MAX = 300, 10000

# Estrutura do texto JSON que a Bronze guarda em `hourly` e `origem`
SCHEMA_HOURLY = (
    "struct<time:array<string>,temperature_2m:array<double>,"
    "relative_humidity_2m:array<double>,precipitation:array<double>>"
)
SCHEMA_ORIGEM = "struct<local:string,municipio:string,data_ref:string,coletado_em_utc:string>"

CHAVE_CARRINHO = ["galpao_id", "zona", "ts"]

# COMMAND ----------

def preparar_clima(bronze_clima):
    """Bronze do clima -> uma linha por (região, hora), sem duplicados."""
    explodido = (
        bronze_clima
        .withColumn("h", F.from_json("hourly", SCHEMA_HOURLY))
        .withColumn("o", F.from_json("origem", SCHEMA_ORIGEM))
        .select(
            F.col("o.local").alias("regiao"),
            F.col("o.municipio").alias("municipio"),
            F.explode(
                F.arrays_zip(
                    F.col("h.time").alias("time"),
                    F.col("h.temperature_2m").alias("temperatura_c"),
                    F.col("h.relative_humidity_2m").alias("umidade_pct"),
                    F.col("h.precipitation").alias("precipitacao_mm"),
                )
            ).alias("z"),
            "arquivo_origem",
            "carregado_em",
        )
        .select(
            "regiao",
            "municipio",
            F.to_timestamp(F.col("z.time")).alias("data_hora"),
            F.col("z.temperatura_c").alias("temperatura_c"),
            F.col("z.umidade_pct").alias("umidade_pct"),
            F.col("z.precipitacao_mm").alias("precipitacao_mm"),
            "arquivo_origem",
            "carregado_em",
        )
        .filter(F.col("regiao").isNotNull() & F.col("data_hora").isNotNull())
    )
    janela = Window.partitionBy("regiao", "data_hora").orderBy(
        F.col("carregado_em").desc(), F.col("arquivo_origem").desc()
    )
    return (
        explodido.withColumn("_rn", F.row_number().over(janela))
        .filter("_rn = 1")
        .drop("_rn")
    )


def preparar_carrinho(bronze_carrinho):
    """Bronze do carrinho -> (válidas, rejeitadas com motivo, contagens)."""
    tipado = bronze_carrinho.select(
        "ronda_id",
        "galpao_id",
        "lote_id",
        F.col("idade_lote_dias").cast("int").alias("idade_lote_dias"),
        F.col("zona").cast("int").alias("zona"),
        F.to_timestamp("ts").alias("ts"),
        F.col("temp_c").cast("double").alias("temp_c"),
        F.col("umid_pct").cast("double").alias("umid_pct"),
        F.col("co2_ppm").cast("int").alias("co2_ppm"),
        F.col("nh3_ppm").cast("double").alias("nh3_ppm"),
        "sensor",
        "origem",
        "arquivo_origem",
        "carregado_em",
    )

    # Duplicados: mesma chave, fica a carga mais recente
    janela = Window.partitionBy(*CHAVE_CARRINHO).orderBy(
        F.col("carregado_em").desc(), F.col("arquivo_origem").desc()
    )
    unicos = (
        tipado.withColumn("_rn", F.row_number().over(janela))
        .filter("_rn = 1")
        .drop("_rn")
    )

    # 3 regras de qualidade, cada linha recebe só o primeiro motivo que falha
    motivo = (
        F.when(F.col("temp_c").isNull(), F.lit("R1_temp_nula"))
        .when(
            (F.col("temp_c") < TEMP_MIN) | (F.col("temp_c") > TEMP_MAX),
            F.lit("R2_temp_fora_da_faixa"),
        )
        .when(
            F.col("co2_ppm").isNull()
            | (F.col("co2_ppm") < CO2_MIN)
            | (F.col("co2_ppm") > CO2_MAX),
            F.lit("R3_co2_invalido"),
        )
    )
    classificado = unicos.withColumn("motivo_rejeicao", motivo)
    validas = classificado.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")
    rejeitadas = classificado.filter(F.col("motivo_rejeicao").isNotNull())

    total_bronze = bronze_carrinho.count()
    total_unicos = unicos.count()
    por_motivo = {
        r["motivo_rejeicao"]: r["count"]
        for r in rejeitadas.groupBy("motivo_rejeicao").count().collect()
    }
    contagens = {
        "bronze": total_bronze,
        "duplicados": total_bronze - total_unicos,
        "R1_temp_nula": por_motivo.get("R1_temp_nula", 0),
        "R2_temp_fora_da_faixa": por_motivo.get("R2_temp_fora_da_faixa", 0),
        "R3_co2_invalido": por_motivo.get("R3_co2_invalido", 0),
        "validas": validas.count(),
    }
    return validas, rejeitadas, contagens

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prata do clima

# COMMAND ----------

clima = preparar_clima(spark.table(f"{SCHEMA}.bronze_clima"))

# Cria a tabela vazia na primeira execução (não faz nada se ela já existe)
clima.limit(0).write.format("delta").mode("ignore").saveAsTable(f"{SCHEMA}.prata_clima")

clima.createOrReplaceTempView("lote_prata_clima")
spark.sql(f"""
    MERGE INTO {SCHEMA}.prata_clima AS alvo
    USING lote_prata_clima AS novo
    ON alvo.regiao = novo.regiao AND alvo.data_hora = novo.data_hora
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prata do carrinho

# COMMAND ----------

validas, rejeitadas, contagens = preparar_carrinho(spark.table(f"{SCHEMA}.bronze_carrinho"))

# Linhas barradas ficam guardadas com o motivo (quarentena), para auditoria
(
    rejeitadas.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.prata_carrinho_rejeitados")
)

# Cria a tabela vazia na primeira execução (não faz nada se ela já existe)
validas.limit(0).write.format("delta").mode("ignore").saveAsTable(f"{SCHEMA}.prata_carrinho")

validas.createOrReplaceTempView("lote_prata_carrinho")
spark.sql(f"""
    MERGE INTO {SCHEMA}.prata_carrinho AS alvo
    USING lote_prata_carrinho AS novo
    ON alvo.galpao_id = novo.galpao_id AND alvo.zona = novo.zona AND alvo.ts = novo.ts
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de qualidade: total barrado por cada uma

# COMMAND ----------

linhas = [
    ("0_total_na_bronze", "linhas lidas da bronze_carrinho", contagens["bronze"]),
    ("1_duplicados", "mesma chave galpao_id + zona + ts (removidos, fica a carga mais recente)", contagens["duplicados"]),
    ("R1_temp_nula", "temperatura ausente (falha do sensor)", contagens["R1_temp_nula"]),
    ("R2_temp_fora_da_faixa", f"temperatura fora de {TEMP_MIN} a {TEMP_MAX} °C", contagens["R2_temp_fora_da_faixa"]),
    ("R3_co2_invalido", f"CO2 nulo ou fora de {CO2_MIN} a {CO2_MAX} ppm", contagens["R3_co2_invalido"]),
    ("9_validas_na_prata", "linhas que seguem para a Prata", contagens["validas"]),
]
(
    spark.createDataFrame(linhas, "regra string, descricao string, linhas long")
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.prata_qualidade")
)

soma = (
    contagens["duplicados"] + contagens["R1_temp_nula"] + contagens["R2_temp_fora_da_faixa"]
    + contagens["R3_co2_invalido"] + contagens["validas"]
)
assert soma == contagens["bronze"], f"As contagens não fecham: {soma} != {contagens['bronze']}"
print("OK: bronze = duplicados + R1 + R2 + R3 + válidas =", soma)

# Print 1: total barrado por cada regra
display(spark.table(f"{SCHEMA}.prata_qualidade").orderBy("regra"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferências para o relatório

# COMMAND ----------

# Print 2: o MERGE mantém uma linha por chave (linhas = chaves distintas); rodar de novo não muda os números
for tabela, chave in [
    ("prata_carrinho", "galpao_id, zona, ts"),
    ("prata_clima", "regiao, data_hora"),
]:
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{tabela}").first()["n"]
    distintas = spark.sql(
        f"SELECT COUNT(*) AS n FROM (SELECT DISTINCT {chave} FROM {SCHEMA}.{tabela})"
    ).first()["n"]
    print(f"{tabela}: {total} linhas, {distintas} chaves distintas")

# COMMAND ----------

# Print 3: linhas barradas, com o motivo (quarentena)
display(
    spark.table(f"{SCHEMA}.prata_carrinho_rejeitados")
    .select("motivo_rejeicao", "galpao_id", "zona", "ts", "temp_c", "co2_ppm", "arquivo_origem")
    .orderBy("motivo_rejeicao", "ts")
    .limit(20)
)

# COMMAND ----------

# Print 4: tipos corretos na Prata
spark.table(f"{SCHEMA}.prata_carrinho").printSchema()
spark.table(f"{SCHEMA}.prata_clima").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que esperar (com os 22 arquivos de cada fonte na Bronze)
# MAGIC
# MAGIC - `prata_clima`: **528 linhas** (11 dias × 24 horas × 2 regiões), 528 chaves distintas.
# MAGIC - `prata_carrinho`: linhas = chaves distintas. A base tem 22 arquivos × 72 leituras = 1.584 chaves únicas; destas, uns 3% caem nas regras R1 a R3.
# MAGIC - `prata_qualidade`: a soma de duplicados + R1 + R2 + R3 + válidas é igual ao total da Bronze (o notebook confere com `assert`).
# MAGIC
# MAGIC **Para provar o MERGE:** anote as contagens do Print 2, execute o notebook inteiro de novo e confira que os números ficaram iguais.
