# 📊 JIRA Data Engineering Challenge

Pipeline de dados seguindo a arquitetura Medallion para cálculo de SLA de chamados do JIRA.

## 🏗️ Arquitetura
- **Bronze**: Ingestão do JSON bruto (Local/Azure).
- **Silver**: Limpeza, normalização de JSON aninhado e conversão para Parquet.
- **Gold**: Aplicação de regras de negócio (SLA), exclusão de fins de semana e feriados (BrasilAPI).

## ⏱️ Lógica de SLA
- **High**: 24h | **Medium**: 72h | **Low**: 120h.
- O cálculo utiliza `pd.bdate_range` com feriados nacionais para garantir que apenas horas úteis sejam contabilizadas.

## 📖 Dicionário de Dados (Gold)
| Coluna | Descrição |
| :--- | :--- |
| `issue_id` | ID único do chamado. |
| `assignee_name` | Nome do analista responsável. |
| `resolution_hours` | Horas úteis totais para resolução. |
| `is_sla_met` | Booleano indicando se o SLA foi atendido. |

## 🚀 Como Executar
1. `pip install -r requirements.txt`
2. `python -m src.bronze.ingest_bronze`
3. `python -m src.silver.transform_silver`
4. `python -m src.gold.build_gold`