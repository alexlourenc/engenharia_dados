import pandas as pd
import os
from src.sla_calculation import calculate_resolution_hours, get_expected_sla_hours

def build_gold():
    print("--- 🥇 Iniciando Construção da Camada Gold ---")
    
    input_path = "data/silver/silver_issues.parquet"
    if not os.path.exists(input_path):
        print("❌ Erro: Arquivo Silver não encontrado.")
        return

    df = pd.read_parquet(input_path)

    # 1. Filtrar apenas chamados concluídos (Done ou Resolved)
    df_gold = df[df['status'].isin(['Done', 'Resolved'])].copy()

    # 2. Aplicar Cálculos de SLA
    print("⏳ Calculando métricas de tempo (isso pode levar alguns segundos)...")
    
    df_gold['resolution_hours'] = df_gold.apply(
        lambda x: calculate_resolution_hours(x['created_at'], x['resolved_at']), axis=1
    )
    
    df_gold['sla_expected_hours'] = df_gold['priority'].apply(get_expected_sla_hours)
    
    # 3. Indicador de SLA Atendido (is_sla_met)
    df_gold['is_sla_met'] = df_gold['resolution_hours'] <= df_gold['sla_expected_hours']

    # 4. Salvar Tabela Final
    os.makedirs("data/gold", exist_ok=True)
    df_gold.to_csv("data/gold/gold_sla_issues.csv", index=False)

    # 5. Gerar Relatórios Obrigatórios
    # Relatório por Analista
    report_analyst = df_gold.groupby('assignee_name').agg(
        total_issues=('id', 'count'),
        avg_resolution_hours=('resolution_hours', 'mean')
    ).reset_index()
    report_analyst.to_csv("data/gold/gold_sla_by_analyst.csv", index=False)

    # Relatório por Tipo de Chamado
    report_type = df_gold.groupby('issue_type').agg(
        total_issues=('id', 'count'),
        avg_resolution_hours=('resolution_hours', 'mean')
    ).reset_index()
    report_type.to_csv("data/gold/gold_sla_by_issue_type.csv", index=False)

    print("✅ Camada Gold e Relatórios gerados com sucesso!")

if __name__ == "__main__":
    build_gold()