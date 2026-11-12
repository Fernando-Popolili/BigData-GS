# Databricks notebook source
# MAGIC %md
# MAGIC # AviRonda · Ouro (requisitos 07 e 08)
# MAGIC
# MAGIC Da Prata (dado limpo) para a Ouro (modelo dimensional pronto para o MVP e para consultas).
# MAGIC
# MAGIC **Modelo estrela**
# MAGIC
# MAGIC | Tabela | Papel | Granularidade |
# MAGIC |---|---|---|
# MAGIC | `fato_leitura` | **fato** | uma leitura do carrinho (galpão + zona + horário) |
# MAGIC | `dim_ponto` | dimensão | uma zona de um galpão |
# MAGIC | `dim_lote` | dimensão | um lote de aves |
# MAGIC | `dim_tempo` | dimensão | uma hora |
# MAGIC | `dim_galpao` | dimensão (**com LGPD**) | um galpão |
# MAGIC | `ouro_status_zona` | **tabela que o MVP consome** | a última leitura de cada zona, com status |
# MAGIC
# MAGIC **LGPD (requisito 08):** a dimensão do galpão traz o produtor responsável. O nome e o telefone originais **não são gravados** na Ouro: só ficam o **hash** (SHA-256 com sal), o nome e o telefone **mascarados**, e as coordenadas **generalizadas** (1 casa decimal, cerca de 11 km).
# MAGIC
# MAGIC > Os dados do produtor são **fictícios e simulados** (o carrinho ainda não opera em granjas reais). Em produção, o sal ficaria em um gerenciador de segredos e o contato viria do cadastro do cliente.

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "workspace.gs"

# Limites usados nas flags e no status (substituir pela tabela da Embrapa quando fechada)
TOL_CRITICO_C = 3.0     # crítico: temperatura acima do alvo da idade + 3 °C
TOL_ATENCAO_C = 1.5     # atenção: acima do alvo + 1,5 °C
CO2_LIMITE = 3000       # crítico: CO2 acima de 3.000 ppm
CO2_ATENCAO = 2500      # atenção: CO2 acima de 2.500 ppm
HORAS_POR_LEITURA = 2   # 12 rondas por dia: cada leitura representa 2 horas

# Sal de demonstração (NÃO é segredo real; em produção vem de um gerenciador de segredos)
SAL_DEMO = "aviaronda-demo-2026"

# COMMAND ----------

def criar_dim_galpao(spark):
    """Dimensão do galpão com proteção de dados pessoais: hash, máscara e coordenada generalizada."""
    origem = spark.createDataFrame(
        [
            ("G-SUL-01", "SUL", "Chapecó", "SC", -27.10, -52.61, "Produtor Fictício Sul", "(49) 99999-0001"),
            ("G-SDE-01", "SUDESTE", "Rio Claro", "SP", -22.41, -47.56, "Produtor Fictício Sudeste", "(19) 99999-0002"),
        ],
        "galpao_id string, regiao string, municipio string, uf string, latitude double, "
        "longitude double, produtor_nome string, produtor_telefone string",
    )
    return origem.select(
        "galpao_id",
        "regiao",
        "municipio",
        "uf",
        F.round("latitude", 1).alias("latitude_aprox"),
        F.round("longitude", 1).alias("longitude_aprox"),
        F.sha2(F.concat_ws("|", F.lit(SAL_DEMO), F.col("produtor_nome")), 256).alias("produtor_hash"),
        F.regexp_replace("produtor_nome", r"(?U)(\w)\w+", "$1***").alias("produtor_mascarado"),
        F.concat(
            F.substring("produtor_telefone", 1, 5), F.lit("*****"), F.substring("produtor_telefone", 11, 5)
        ).alias("telefone_mascarado"),
    )


def sql_ouro(s: str) -> dict:
    """SELECTs que montam cada tabela da Ouro (s = schema, por exemplo workspace.gs)."""
    temp_alvo = "GREATEST(23.0, 32.0 - 0.2 * (c.idade_lote_dias - 1))"
    return {
        "dim_ponto": f"""
            SELECT DISTINCT
                concat(galpao_id, '-Z', zona) AS ponto_id,
                galpao_id,
                zona,
                CASE zona WHEN 1 THEN 'entrada de ar' WHEN 6 THEN 'exaustor' ELSE 'meio' END AS posicao
            FROM {s}.prata_carrinho
        """,
        "dim_lote": f"""
            SELECT
                lote_id,
                galpao_id,
                MIN(date_sub(CAST(ts AS DATE), idade_lote_dias - 1)) AS data_inicio_lote,
                MIN(CAST(ts AS DATE)) AS primeira_leitura,
                MAX(CAST(ts AS DATE)) AS ultima_leitura,
                MIN(idade_lote_dias) AS idade_minima,
                MAX(idade_lote_dias) AS idade_maxima
            FROM {s}.prata_carrinho
            GROUP BY lote_id, galpao_id
        """,
        "dim_tempo": f"""
            SELECT
                CAST(date_format(data_hora, 'yyyyMMddHH') AS INT) AS tempo_id,
                data_hora,
                CAST(data_hora AS DATE) AS data,
                year(data_hora) AS ano,
                month(data_hora) AS mes,
                day(data_hora) AS dia,
                hour(data_hora) AS hora,
                dayofweek(data_hora) AS dia_da_semana,
                CASE WHEN hour(data_hora) < 6 THEN 'madrugada'
                     WHEN hour(data_hora) < 12 THEN 'manha'
                     WHEN hour(data_hora) < 18 THEN 'tarde'
                     ELSE 'noite' END AS periodo
            FROM (SELECT DISTINCT date_trunc('hour', ts) AS data_hora FROM {s}.prata_carrinho)
        """,
        "fato_leitura": f"""
            WITH base AS (
                SELECT
                    c.galpao_id, c.zona, c.ts, c.lote_id, c.idade_lote_dias,
                    c.temp_c, c.umid_pct, c.co2_ppm, c.nh3_ppm,
                    w.temperatura_c AS temp_externa_c,
                    w.precipitacao_mm,
                    {temp_alvo} AS temp_alvo_c
                FROM {s}.prata_carrinho c
                JOIN {s}.dim_galpao g ON g.galpao_id = c.galpao_id
                LEFT JOIN {s}.prata_clima w
                       ON w.regiao = g.regiao AND w.data_hora = date_trunc('hour', c.ts)
            )
            SELECT
                sha2(concat_ws('|', galpao_id, CAST(zona AS STRING), CAST(ts AS STRING)), 256) AS leitura_id,
                concat(galpao_id, '-Z', zona) AS ponto_id,
                lote_id,
                CAST(date_format(date_trunc('hour', ts), 'yyyyMMddHH') AS INT) AS tempo_id,
                galpao_id,
                ts,
                idade_lote_dias,
                temp_c, umid_pct, co2_ppm, nh3_ppm,
                temp_externa_c, precipitacao_mm,
                temp_alvo_c,
                (temp_c > temp_alvo_c + {TOL_CRITICO_C}) AS acima_limite_temp,
                (co2_ppm > {CO2_LIMITE}) AS acima_limite_co2,
                ((temp_c > temp_alvo_c + {TOL_CRITICO_C}) OR (co2_ppm > {CO2_LIMITE})) AS fora_do_limite
            FROM base
        """,
        "ouro_status_zona": f"""
            WITH ultima AS (
                SELECT *, row_number() OVER (PARTITION BY ponto_id ORDER BY ts DESC) AS rn
                FROM {s}.fato_leitura
            )
            SELECT
                u.ponto_id, u.galpao_id, p.zona, p.posicao,
                g.municipio, g.uf, g.produtor_mascarado,
                u.ts, u.temp_c, u.umid_pct, u.co2_ppm, u.temp_alvo_c,
                CASE WHEN u.fora_do_limite THEN 'critico'
                     WHEN u.temp_c > u.temp_alvo_c + {TOL_ATENCAO_C} OR u.co2_ppm > {CO2_ATENCAO} THEN 'atencao'
                     ELSE 'ok' END AS status
            FROM ultima u
            JOIN {s}.dim_ponto p ON p.ponto_id = u.ponto_id
            JOIN {s}.dim_galpao g ON g.galpao_id = u.galpao_id
            WHERE u.rn = 1
        """,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção da Ouro

# COMMAND ----------

# 1) Dimensão do galpão (LGPD): o nome e o telefone originais nunca chegam à tabela
(
    criar_dim_galpao(spark)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.dim_galpao")
)

# 2) Demais dimensões, fato e tabela do MVP (a ordem importa: a fato depende de dim_galpao)
for tabela, select in sql_ouro(SCHEMA).items():
    spark.sql(f"CREATE OR REPLACE TABLE {SCHEMA}.{tabela} AS {select}")
    print(f"{tabela}: {spark.table(f'{SCHEMA}.{tabela}').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências: modelo e LGPD

# COMMAND ----------

# Print 1: modelo dimensional (contagem de cada tabela)
for t in ["fato_leitura", "dim_ponto", "dim_lote", "dim_tempo", "dim_galpao", "ouro_status_zona"]:
    print(f"{t}: {spark.table(f'{SCHEMA}.{t}').count()} linhas")

# COMMAND ----------

# Print 2 (LGPD): hash, máscara e coordenada generalizada; nome e telefone originais não existem na tabela
display(spark.table(f"{SCHEMA}.dim_galpao"))
print("Colunas:", spark.table(f"{SCHEMA}.dim_galpao").columns)

# COMMAND ----------

# Print 3 (MVP): a tabela que o painel consome (última leitura de cada zona, com status e produtor mascarado)
display(spark.table(f"{SCHEMA}.ouro_status_zona").orderBy("galpao_id", "zona"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## As 5 consultas SQL do MVP
# MAGIC
# MAGIC Cada consulta responde a uma pergunta do granjeiro. A **interpretação** de cada resultado vai no relatório.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 1: qual zona está pior agora? (alimenta o painel do MVP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT galpao_id, zona, posicao, status, temp_c, co2_ppm, ts
# MAGIC FROM workspace.gs.ouro_status_zona
# MAGIC ORDER BY CASE status WHEN 'critico' THEN 1 WHEN 'atencao' THEN 2 ELSE 3 END, galpao_id, zona

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 2 (impacto): quantas horas cada zona passou fora do limite, por lote?
# MAGIC Cada leitura representa 2 horas (12 rondas por dia), então horas estimadas = leituras fora do limite × 2.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.galpao_id, f.lote_id, p.zona, p.posicao,
# MAGIC        COUNT(*) AS leituras,
# MAGIC        SUM(CASE WHEN f.fora_do_limite THEN 1 ELSE 0 END) AS leituras_fora_do_limite,
# MAGIC        SUM(CASE WHEN f.fora_do_limite THEN 1 ELSE 0 END) * 2 AS horas_fora_estimadas,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN f.fora_do_limite THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_fora_do_limite
# MAGIC FROM workspace.gs.fato_leitura f
# MAGIC JOIN workspace.gs.dim_ponto p ON p.ponto_id = f.ponto_id
# MAGIC GROUP BY f.galpao_id, f.lote_id, p.zona, p.posicao
# MAGIC ORDER BY horas_fora_estimadas DESC, f.galpao_id, p.zona
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 3: o galpão amortece o calor? Temperatura interna x externa por região e período do dia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT g.regiao, t.periodo,
# MAGIC        ROUND(AVG(f.temp_externa_c), 1) AS temp_externa_media,
# MAGIC        ROUND(AVG(f.temp_c), 1) AS temp_interna_media,
# MAGIC        ROUND(AVG(f.temp_c - f.temp_externa_c), 1) AS diferenca_interna_menos_externa
# MAGIC FROM workspace.gs.fato_leitura f
# MAGIC JOIN workspace.gs.dim_galpao g ON g.galpao_id = f.galpao_id
# MAGIC JOIN workspace.gs.dim_tempo t ON t.tempo_id = f.tempo_id
# MAGIC WHERE f.temp_externa_c IS NOT NULL
# MAGIC GROUP BY g.regiao, t.periodo
# MAGIC ORDER BY g.regiao, CASE t.periodo WHEN 'madrugada' THEN 1 WHEN 'manha' THEN 2 WHEN 'tarde' THEN 3 ELSE 4 END

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 4: quais foram os dias de maior risco? (% de leituras fora do limite, com a chuva do dia)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.data, f.galpao_id,
# MAGIC        COUNT(*) AS leituras,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN f.fora_do_limite THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_fora_do_limite,
# MAGIC        ROUND(MAX(f.temp_externa_c), 1) AS temp_externa_maxima,
# MAGIC        ROUND(AVG(f.precipitacao_mm), 2) AS chuva_media_mm_por_hora
# MAGIC FROM workspace.gs.fato_leitura f
# MAGIC JOIN workspace.gs.dim_tempo t ON t.tempo_id = f.tempo_id
# MAGIC GROUP BY t.data, f.galpao_id
# MAGIC ORDER BY pct_fora_do_limite DESC, t.data
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consulta 5: a qualidade do ar piora com a idade do lote? (CO2 por faixa de idade)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT galpao_id,
# MAGIC        CASE WHEN idade_lote_dias <= 28 THEN 'ate 28 dias'
# MAGIC             WHEN idade_lote_dias <= 35 THEN '29 a 35 dias'
# MAGIC             ELSE '36 a 42 dias' END AS faixa_de_idade,
# MAGIC        COUNT(*) AS leituras,
# MAGIC        ROUND(AVG(co2_ppm)) AS co2_medio_ppm,
# MAGIC        MAX(co2_ppm) AS co2_maximo_ppm,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN acima_limite_co2 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_acima_do_limite_co2
# MAGIC FROM workspace.gs.fato_leitura
# MAGIC GROUP BY galpao_id,
# MAGIC          CASE WHEN idade_lote_dias <= 28 THEN 'ate 28 dias'
# MAGIC               WHEN idade_lote_dias <= 35 THEN '29 a 35 dias'
# MAGIC               ELSE '36 a 42 dias' END
# MAGIC ORDER BY galpao_id, MIN(idade_lote_dias)
