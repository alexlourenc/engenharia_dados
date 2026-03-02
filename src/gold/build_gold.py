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
    logging.error(f"❌ Critical Error: Could not import SLA module / Erro crítico: Não foi possível importar o módulo SLA: {e}")
    sys.exit(1)

# Logging configuration / Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_brazil_holidays(base_dir):
    """
    Loads national holidays from a local JSON file (previously fetched from BrasilAPI).
    Carrega os feriados nacionais de um arquivo JSON local (previamente coletado da BrasilAPI).
    """
    holiday_path = base_dir / "data" / "holidays_2026.json"
    if holiday_path.exists():
        with open(holiday_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Returns a list of strings in YYYY-MM-DD format
            # Retorna uma lista de strings no formato AAAA-MM-DD
            return [h['date'] for h in data]
    logger.warning("⚠️ Holiday file not found. Calculation will proceed without holidays.")
    return []

def build_gold():
    """
    Constructs the Gold layer by applying business rules, SLA calculations with holidays, and generating reports.
    Constrói a camada Gold aplicando regras de negócio, cálculos de SLA com feriados e gerando relatórios.
    """
    logger.info("--- 🥇 Starting Gold Construction / Iniciando Construção da Camada Gold ---")

    # 1. Path configuration / Configuração de caminhos
    # Adjusting base_dir to point to project root
    # Ajustando base_dir para apontar para a raiz do projeto
    base_dir = Path(__file__).resolve().parents[2]
    input_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"
    output_dir = base_dir / "data" / "gold"
    output_path = output_dir / "final_sla_report.parquet"
    
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Data Loading and Filtering / Carregamento e Filtro de Dados
    if not input_path.exists():
        logger.error(f"❌ Silver file not found / Arquivo Silver não encontrado: {input_path}")
        return

    df = pd.read_parquet(input_path)
    
    # Filter: Only tickets in final status compose the Gold layer
    # Filtro: Apenas chamados em status final compõem a camada Gold
    df_gold = df[df['status'].isin(['Done', 'Resolved', 'Concluído'])].copy()

    if df_gold.empty:
        logger.warning("⚠️ No completed tickets found for processing / Nenhum chamado finalizado encontrado.")
        return

    # 3. Holiday Integration / Integração de Feriados
    # Load holidays list to pass to the calculation engine
    # Carrega a lista de feriados para passar ao motor de cálculo
    holidays_list = load_brazil_holidays(base_dir)

    # 4. SLA Calculation Logic / Lógica de Cálculo de SLA
    logger.info(f"⏳ Processing {len(df_gold)} tickets... / Processando chamados...")
    
    tqdm.pandas(desc="Calculating Business Hours / Calculando Horas Úteis")
    
    # Passing the holiday list as an argument for accurate business hour calculation
    # Passando a lista de feriados como argumento para o cálculo preciso de horas úteis
    df_gold['hours_resolution'] = df_gold.progress_apply(
        lambda x: calculate_business_hours(
            x['created_at'], 
            x['resolved_at'], 
            holidays=holidays_list
        ), axis=1
    )
    
    df_gold['sla_expected'] = df_gold['priority'].apply(get_expected_sla)
    df_gold['is_sla_met'] = df_gold['hours_resolution'] <= df_gold['sla_expected']

    # 5. Persistence and Reporting / Persistência e Relatórios
    try:
        # Saving master table / Salvando tabela mestre
        df_gold.to_parquet(output_path, index=False)
        
        # Aggregated Report 1: Analyst Performance / Performance por Analista
        analista_report = df_gold.groupby('analista').agg(
            qtd_chamados=('id', 'count'),
            sla_medio_horas=('hours_resolution', 'mean'),
            taxa_atendimento_sla=('is_sla_met', 'mean')
        ).reset_index()
        analista_report.to_csv(output_dir / "report_analista.csv", index=False)
        
        # Aggregated Report 2: Performance by Issue Type / Performance por Tipo de Chamado
        tipo_report = df_gold.groupby('issue_type').agg(
            qtd_chamados=('id', 'count'),
            sla_medio_horas=('hours_resolution', 'mean')
        ).reset_index()
        tipo_report.to_csv(output_dir / "report_tipo_chamado.csv", index=False)

        logger.info(f"✅ Gold layer completed / Camada Gold concluída! Outputs: {output_dir}")
        
    except Exception as e:
        logger.error(f"❌ Error saving Gold outputs / Erro ao salvar saídas da Gold: {e}")

if __name__ == "__main__":
    build_gold()