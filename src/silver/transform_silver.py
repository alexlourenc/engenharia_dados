import os
import json
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Logging configuration for transformation monitoring
# Configuração de Logging para monitorar o processo de transformação
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_silver_transformation():
    """
    Performs data cleaning, normalization, and conversion from Bronze (JSON) to Silver (Parquet).
    Realiza a limpeza, normalização e conversão dos dados da Bronze (JSON) para Silver (Parquet).
    """
    logger.info("Starting Silver Transformation / Iniciando Transformação Silver")
    
    # Path Setup
    # Configuração de Caminhos
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=base_dir / ".env")
    
    input_filename = os.getenv("BLOB_NAME", "jira_issues_raw.json")
    input_path = base_dir / "data" / "bronze" / input_filename
    output_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"

    # Data Loading
    # Carregamento de Dados
    try:
        if not input_path.exists():
            logger.error(f"Source file not found / Arquivo não encontrado: {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Normalizing the 'issues' nested structure
        # Normalizando a estrutura aninhada 'issues'
        df = pd.json_normalize(raw_data, record_path=['issues'])
        logger.info(f"Data loaded: {len(df)} records found. / Dados carregados: {len(df)} registros.")
    
    except Exception as e:
        logger.error(f"Error loading JSON / Erro ao carregar JSON: {e}")
        return

    def extract_assignee(val):
        """
        Extracts analyst name from list or dict.
        Extrai nome do analista de lista ou dicionário.
        """
        if isinstance(val, list) and len(val) > 0:
            return val[0].get('name', 'Unassigned')
        return 'Unassigned'

    def extract_ts(val, field):
        """
        Extracts specific timestamp from list.
        Extrai timestamp específico de uma lista.
        """
        if isinstance(val, list) and len(val) > 0:
            return val[0].get(field)
        return None

    # Transformation Logic
    # Lógica de Transformação
    df['analista'] = df['assignee'].apply(extract_assignee)
    df['created_at'] = df['timestamps'].apply(lambda x: extract_ts(x, 'created_at'))
    df['resolved_at'] = df['timestamps'].apply(lambda x: extract_ts(x, 'resolved_at'))

    # Type Conversion
    # Conversão de Tipos
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # Data Quality: Removing ONLY records without creation date
    # Qualidade de Dados: Removendo APENAS registros sem data de criação
    df = df.dropna(subset=['created_at']).copy()

    # Column Selection and Final Typing
    # Seleção de Colunas e Tipagem Final
    cols_to_keep = ['id', 'issue_type', 'status', 'priority', 'analista', 'created_at', 'resolved_at']
    df_silver = df[cols_to_keep].copy()

    # Ensuring string types for categorical columns
    # Garantindo tipos string para colunas categóricas
    text_cols = ['id', 'issue_type', 'status', 'priority', 'analista']
    for col in text_cols:
        df_silver[col] = df_silver[col].astype(str)

    # Saving to Parquet (Silver Layer)
    # Salvando em Parquet (Camada Silver)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_silver.to_parquet(output_path, index=False, engine='pyarrow')
        logger.info(f"Silver layer saved successfully / Camada Silver salva com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"Error saving Parquet / Erro ao salvar Parquet: {e}")

if __name__ == "__main__":
    run_silver_transformation()