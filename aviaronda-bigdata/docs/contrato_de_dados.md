# Contrato de dados: AviRonda

Produto em uma frase: um carrinho autônomo que faz a ronda do galpão avícola, mede calor e qualidade do ar na altura das aves e avisa o granjeiro, com a ação recomendada, antes que o problema vire perda.

## Perguntas que o MVP responde ao cidadão (o granjeiro)

1. Qual zona do galpão está pior agora (calor e CO2)?
2. Quantas horas cada zona ficou fora do limite neste lote? (consulta de impacto)
3. Como a temperatura interna acompanha a externa? O galpão está amortecendo o calor?
4. O dia de hoje é de risco (calor ou umidade) em comparação com os dias anteriores do lote?
5. Como a qualidade do ar piora com a idade do lote?

## Fontes

| # | Fonte | Tipo | Origem | Formato | Frequência | Arquivo em `chegada/` |
|---|---|---|---|---|---|---|
| 1 | Open-Meteo (temperatura, umidade e precipitação horárias) | Externa real (API pública, sem chave) | api.open-meteo.com | JSON (1 objeto por arquivo) | Diária, por região | `clima/clima_<regiao>_<data>.json` |
| 2 | Leituras do carrinho AviRonda | Simulada, justificada abaixo | `dags/aviaronda/gerador.py` | JSON Lines (1 leitura por linha) | Diária, por galpão (12 rondas × 6 zonas) | `carrinho/carrinho_<galpao>_<data>.json` |

Regiões simuladas: **SUL** (Chapecó, SC: risco de umidade e amônia) e **SUDESTE** (Rio Claro, SP: risco de calor), os dois modos de risco do El Niño.

## Contrato do JSON do carrinho

Uma linha por leitura:

```json
{"ronda_id":"G-SUL-01-2026-11-05T1400","galpao_id":"G-SUL-01","lote_id":"G-SUL-01-L01",
 "idade_lote_dias":36,"zona":4,"ts":"2026-11-05T14:08:12",
 "temp_c":27.4,"umid_pct":66.2,"co2_ppm":2410,"nh3_ppm":null,
 "sensor":"SCD40","origem":"simulado"}
```

| Campo | Tipo | Observação |
|---|---|---|
| `ronda_id` | string | galpão + data + hora de início da ronda |
| `galpao_id`, `lote_id` | string | identificam o galpão e o ciclo do lote |
| `idade_lote_dias` | int | 1 a 42 |
| `zona` | int | 1 = entrada de ar, 6 = perto do exaustor |
| `ts` | string ISO 8601 | horário local da leitura |
| `temp_c`, `umid_pct`, `co2_ppm` | número | vindos do sensor SCD40 |
| `nh3_ppm` | número ou null | nulo enquanto não houver sensor de amônia (o SCD40 não mede amônia) |
| `sensor`, `origem` | string | rastreabilidade; `origem = simulado` |

**Chave natural (usada no MERGE da Prata):** `galpao_id + zona + ts`.

## Justificativa da simulação (exigência do briefing)

O carrinho ainda não tem histórico de rondas em galpões reais. Por isso as leituras são simuladas de forma determinística (mesma data e semente geram o mesmo arquivo), com estas premissas:

- **Clima externo real:** o gerador usa a resposta da Open-Meteo do dia. Se ela faltar, usa um perfil sintético (pico de calor às 15h) e registra um aviso no log.
- **Temperatura interna** amortecida em relação à externa (galpão semiclimatizado), dependente da idade do lote e crescente em direção ao exaustor.
- **CO2** cresce com a idade do lote e ao longo do galpão.
- **Pontos críticos:** cerca de 6% das rondas têm uma zona com calor e CO2 elevados, para exercitar os alertas.
- **Anomalias intencionais** (nulos, valores absurdos, duplicados) alimentam as regras de qualidade da Prata.

Limitações a declarar no relatório: os coeficientes são aproximações para demonstração, não calibrações científicas; a temperatura de conforto por idade (`temperatura_alvo`) deve ser substituída pela tabela da Embrapa; os padrões espaciais foram inspirados na heterogeneidade descrita pela Embrapa, mas não reproduzem as medições dela.

## Mapa das camadas (previsão para os próximos passos)

| Camada | Conteúdo |
|---|---|
| Bronze | `bronze_clima` e `bronze_carrinho`, dado bruto com `arquivo_origem` e `carregado_em` |
| Prata | `prata_clima` (listas horárias explodidas) e `prata_carrinho` (tipos corretos, nulos e absurdos tratados, MERGE por chave) |
| Ouro | `fato_leitura`, `dim_ponto`, `dim_lote`, `dim_tempo` e `dim_local`, mais 5 consultas SQL |
