import pandas as pd
from pathlib import Path
import sys
import json
import logging
from tqdm import tqdm

# Adding the src directory to the path to import the SLA calculation module
# Adiciona o diretório src ao path para importar o módulo de cálculo de SLA
sys.path.append(str(Path(__file__).resolve().parents[1]))

try:
    from sla_calculation import calculate_business_hours, get_expected_sla
except ImportError as e:
    logging.error(f"Critical Error: Could not import SLA module / Erro crítico: Não foi possível importar o módulo SLA: {e}")
    sys.exit(1)

# Logging configuration
# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_brazil_holidays(base_dir):
    """
    Loads national holidays from a local JSON file.
    Carrega os feriados nacionais de um arquivo JSON local.
    """
    holiday_path = base_dir / "data" / "holidays_2026.json"
    if holiday_path.exists():
        with open(holiday_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return [h['date'] for h in data]
    
    logger.warning("Holiday file not found. Calculation will proceed without holidays. / Arquivo de feriados não encontrado. O cálculo prosseguirá sem feriados.")
    return []

def build_gold():
    """
    Constructs the Gold layer by applying business rules, SLA calculations, and generating reports.
    Constrói a camada Gold aplicando regras de negócio, cálculos de SLA e gerando relatórios.
    """
    logger.info("Starting Gold Construction / Iniciando Construção da Camada Gold")

    # Path configuration
    # Configuração de caminhos
    base_dir = Path(__file__).resolve().parents[2]
    input_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"
    output_dir = base_dir / "data" / "gold"
    output_path = output_dir / "final_sla_report.parquet"
    
    output_dir.mkdir(parents=True, exist_ok=True)

    # Data Loading and Filtering
    # Carregamento e Filtro de Dados
    if not input_path.exists():
        logger.error(f"Silver file not found / Arquivo Silver não encontrado: {input_path}")
        return

    df = pd.read_parquet(input_path)
    
    df_gold = df[df['status'].isin(['Done', 'Resolved', 'Concluído'])].copy()

    if df_gold.empty:
        logger.warning("No completed tickets found for processing / Nenhum chamado finalizado encontrado.")
        return

    # Holiday Integration
    # Integração de Feriados
    holidays_list = load_brazil_holidays(base_dir)

    # SLA Calculation Logic
    # Lógica de Cálculo de SLA
    logger.info(f"Processing {len(df_gold)} tickets... / Processando chamados...")
    
    tqdm.pandas(desc="Calculating Business Hours / Calculando Horas Úteis")
    
    df_gold['hours_resolution'] = df_gold.progress_apply(
        lambda x: calculate_business_hours(
            x['created_at'], 
            x['resolved_at'], 
            holidays=holidays_list
        ), axis=1
    )
    
    df_gold['sla_expected'] = df_gold['priority'].apply(get_expected_sla)
    df_gold['is_sla_met'] = df_gold['hours_resolution'] <= df_gold['sla_expected']

    # Persistence and Reporting
    # Persistência e Relatórios
    try:
        df_gold.to_parquet(output_path, index=False)
        
        analista_report = df_gold.groupby('analista').agg(
            qtd_chamados=('id', 'count'),
            sla_medio_horas=('hours_resolution', 'mean'),
            taxa_atendimento_sla=('is_sla_met', 'mean')
        ).reset_index()
        analista_report.to_csv(output_dir / "report_analista.csv", index=False)
        
        tipo_report = df_gold.groupby('issue_type').agg(
            qtd_chamados=('id', 'count'),
            sla_medio_horas=('hours_resolution', 'mean')
        ).reset_index()
        tipo_report.to_csv(output_dir / "report_tipo_chamado.csv", index=False)

        logger.info(f"Gold layer completed / Camada Gold concluída! Outputs: {output_dir}")
        
    except Exception as e:
        logger.error(f"Error saving Gold outputs / Erro ao salvar saídas da Gold: {e}")

if __name__ == "__main__":
    build_gold()