import pandas as pd
from pathlib import Path
import sys
import os
from tqdm import tqdm

# Add the src directory to the path to import the SLA calculation module
# Adiciona o diretório src ao path para importar o módulo de cálculo de SLA
sys.path.append(str(Path(__file__).resolve().parents[1]))
from sla_calculation import calculate_business_hours, get_expected_sla

def build_gold():
    """
    Constructs the Gold layer by applying business rules, SLA calculations, and generating reports.
    Constrói a camada Gold aplicando regras de negócio, cálculos de SLA e gerando relatórios.
    """
    # 1. Path and folder configuration
    # Configuração de caminhos e pastas
    base_dir = Path(__file__).resolve().parents[2]
    input_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"
    output_dir = base_dir / "data" / "gold"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "final_sla_report.parquet"

    print("--- 🥇 Starting Gold Construction / Iniciando Construção da Camada Gold ---")

    # 2. Read Silver data and Filter (Premise: Only 'Done' or 'Resolved' for Gold)
    # Leitura e Filtro (Premissa: Apenas chamados finalizados compõem a Gold)
    if not input_path.exists():
        print(f"❌ Error: Silver file not found at / Erro: Arquivo Silver não encontrado em {input_path}")
        return

    df = pd.read_parquet(input_path)
    # Filter based on business status
    # Filtra conforme o status de negócio
    df_gold = df[df['status'].isin(['Done', 'Resolved'])].copy()

    if df_gold.empty:
        print("⚠️ Warning: No completed tickets found. / Aviso: Nenhum chamado finalizado encontrado.")
        return

    # 3. Apply SLA Calculation with Progress Bar (tqdm)
    # Aplicação do Cálculo de SLA com Barra de Progresso (tqdm)
    print(f"⏳ Processing {len(df_gold)} completed tickets... / Processando chamados...")
    
    # Enable tqdm support for pandas
    # Habilita o suporte do tqdm para o pandas
    tqdm.pandas(desc="Calculating Business Hours / Calculando Horas Úteis")
    
    # Calculate resolution time considering business days and holidays
    # Calcula o tempo de resolução considerando dias úteis e feriados
    df_gold['hours_resolution'] = df_gold.progress_apply(
        lambda x: calculate_business_hours(x['created_at'], x['resolved_at']), axis=1
    )
    
    # Define expected SLA based on priority (High=24h, Medium=72h, Low=120h)
    # Define o SLA esperado com base na prioridade
    df_gold['sla_expected'] = df_gold['priority'].apply(get_expected_sla)
    
    # 4. Fulfillment Indicator (Boolean: True if met, False if violated)
    # Indicador de Atendimento (Boolean: True se atendeu, False se violou)
    df_gold['is_sla_met'] = df_gold['hours_resolution'] <= df_gold['sla_expected']

    # 5. Final Table Persistence (Parquet format to preserve data types)
    # Persistência da Tabela Final (Formato Parquet para preservar tipos)
    df_gold.to_parquet(output_path, index=False)
    
    # 6. Generation of Aggregated Reports (Business Requirements)
    # Geração de Relatórios Agregados (Requisitos de Negócio)
    
    # Report 1: Average SLA per Analyst
    # Relatório 1: SLA Médio por Analista
    analista_report = df_gold.groupby('analista').agg(
        qtd_chamados=('id', 'count'),
        sla_medio_horas=('hours_resolution', 'mean')
    ).reset_index()
    analista_report.to_csv(output_dir / "report_analista.csv", index=False)
    
    # Report 2: Average SLA per Issue Type
    # Relatório 2: SLA Médio por Tipo de Chamado
    tipo_report = df_gold.groupby('issue_type').agg(
        qtd_chamados=('id', 'count'),
        sla_medio_horas=('hours_resolution', 'mean')
    ).reset_index()
    tipo_report.to_csv(output_dir / "report_tipo_chamado.csv", index=False)
    
    print("-" * 30)
    print(f"✅ Gold layer completed successfully! / Camada Gold concluída!")
    print(f"📂 Master Table / Tabela Mestre: {output_path}")
    print(f"📊 Analyst Report / Relatório Analista: report_analista.csv")
    print(f"📊 Type Report / Relatório Tipo: report_tipo_chamado.csv")

if __name__ == "__main__":
    build_gold()