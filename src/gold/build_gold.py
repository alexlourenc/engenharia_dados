import pandas as pd
from pathlib import Path
import sys
import os
from tqdm import tqdm

# Adiciona o diretório src ao path para importar o sla_calculation
sys.path.append(str(Path(__file__).resolve().parents[1]))
from sla_calculation import calculate_business_hours, get_expected_sla

def build_gold():
    # 1. Configuração de caminhos e pastas
    base_dir = Path(__file__).resolve().parents[2]
    input_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"
    output_dir = base_dir / "data" / "gold"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "final_sla_report.parquet"

    print("🚀 Iniciando Construção da Camada Gold...")

    # 2. Leitura e Filtro (Premissa: Apenas Done ou Resolved para a Gold)
    if not input_path.exists():
        print(f"❌ Erro: Arquivo Silver não encontrado em {input_path}")
        return

    df = pd.read_parquet(input_path)
    # Filtra conforme regra de negócio: apenas chamados finalizados compõem a tabela de SLA
    df_gold = df[df['status'].isin(['Done', 'Resolved'])].copy()

    if df_gold.empty:
        print("⚠️ Aviso: Nenhum chamado com status 'Done' ou 'Resolved' encontrado.")
        return

    # 3. Aplicação do Cálculo de SLA com Barra de Progresso (tqdm)
    print(f"⏳ Processando {len(df_gold)} chamados finalizados...")
    
    # Habilita o suporte do tqdm para o pandas
    tqdm.pandas(desc="Calculando Horas Úteis (com feriados)")
    
    # Calcula o tempo de resolução considerando apenas dias úteis e feriados nacionais
    df_gold['hours_resolution'] = df_gold.progress_apply(
        lambda x: calculate_business_hours(x['created_at'], x['resolved_at']), axis=1
    )
    
    # Define o SLA esperado com base na prioridade (High=24h, Medium=72h, Low=120h)
    df_gold['sla_expected'] = df_gold['priority'].apply(get_expected_sla)
    
    # 4. Indicador de Atendimento (Boolean: True se atendeu, False se violou)
    df_gold['is_sla_met'] = df_gold['hours_resolution'] <= df_gold['sla_expected']

    # 5. Persistência da Tabela Final (Formato Parquet para preservar tipos)
    df_gold.to_parquet(output_path, index=False)
    
    # 6. Geração de Relatórios Agregados (Obrigatórios nas premissas)
    
    # Relatório 1: SLA Médio por Analista
    analista_report = df_gold.groupby('analista').agg(
        qtd_chamados=('id', 'count'),
        sla_medio_horas=('hours_resolution', 'mean')
    ).reset_index()
    analista_report.to_csv(output_dir / "report_analista.csv", index=False)
    
    # Relatório 2: SLA Médio por Tipo de Chamado
    tipo_report = df_gold.groupby('issue_type').agg(
        qtd_chamados=('id', 'count'),
        sla_medio_horas=('hours_resolution', 'mean')
    ).reset_index()
    tipo_report.to_csv(output_dir / "report_tipo_chamado.csv", index=False)
    
    print("-" * 30)
    print(f"✅ Camada Gold concluída com sucesso!")
    print(f"📂 Tabela Mestre: {output_path}")
    print(f"📊 Relatório por Analista: report_analista.csv")
    print(f"📊 Relatório por Tipo: report_tipo_chamado.csv")

if __name__ == "__main__":
    build_gold()