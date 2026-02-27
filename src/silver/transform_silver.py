import pandas as pd
import json
import os
from pathlib import Path
from dotenv import load_dotenv

def run_silver_transformation():
    """
    Performs data cleaning, normalization, and conversion from Bronze (JSON) to Silver (Parquet).
    Realiza a limpeza, normalização e conversão dos dados da Bronze (JSON) para Silver (Parquet).
    """
    print("--- 🥈 Starting Silver Transformation / Iniciando Transformação Silver ---")
    
    # 1. Dynamic Path Configuration
    # Setting up paths to ensure the script runs correctly regardless of the execution context
    # Configuração de Caminhos Dinâmicos para garantir que o script funcione independente do contexto
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=base_dir / ".env")
    
    # Input and Output path definitions
    # Definições de caminhos de entrada e saída
    input_filename = os.getenv("BLOB_NAME", "jira_issues_raw.json")
    input_path = base_dir / "data" / "bronze" / input_filename
    
    output_dir = base_dir / "data" / "silver"
    output_path = output_dir / "jira_issues_clean.parquet"

    # 2. Load Raw JSON Data
    # Carregar o JSON bruto
    try:
        if not input_path.exists():
            print(f"❌ Error: Source file not found at / Erro: Arquivo não encontrado em {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Normalize the 'issues' key into a flat DataFrame
        # Normaliza a chave 'issues' para um DataFrame estruturado
        df = pd.json_normalize(raw_data, record_path=['issues'])
        print(f"✅ Data loaded: {len(df)} records found. / Dados carregados: {len(df)} registros.")
    except Exception as e:
        print(f"❌ Error processing JSON structure / Erro ao processar JSON: {e}")
        return

    # 3. Extraction and Cleaning of Nested Fields
    # Extração e Limpeza de Campos Aninhados
    
    # Helper to extract the Analyst (Assignee) name
    # Função auxiliar para extrair o nome do Analista (Assignee)
    def extract_assignee_name(assignee_list):
        if isinstance(assignee_list, list) and len(assignee_list) > 0:
            return assignee_list[0].get('name')
        return "Unassigned"

    # Helper to extract specific dates from the 'timestamps' list
    # Função auxiliar para extrair datas específicas da lista 'timestamps'
    def extract_timestamp(ts_list, field):
        if isinstance(ts_list, list) and len(ts_list) > 0:
            return ts_list[0].get(field)
        return None

    # Applying extractions to create new columns
    # Aplicando as extrações para criar novas colunas
    df['analista'] = df['assignee'].apply(extract_assignee_name)
    df['created_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'created_at'))
    df['resolved_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'resolved_at'))

    # 4. Data Type Handling and Conversion
    # Tratamento de Tipos e Conversão de Datas
    # errors='coerce' ensures invalid dates become NaT (Not a Time)
    # errors='coerce' garante que datas inválidas virem NaT
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # 5. Data Quality: Remove records without a valid creation date
    # Qualidade de Dados: Remove registros sem data de criação válida
    before_count = len(df)
    df = df.dropna(subset=['created_at']).copy()
    print(f"🧹 Cleaning: {before_count - len(df)} invalid records removed. / Limpeza: registros removidos.")

    # 6. Preparation for Persistence
    # Preparação para Salvamento
    os.makedirs(output_dir, exist_ok=True)
    
    # Selecting only required columns for the Silver layer
    # Selecionando apenas as colunas necessárias para a camada Silver
    cols_to_keep = ['id', 'issue_type', 'status', 'priority', 'analista', 'created_at', 'resolved_at']
    df_silver = df[cols_to_keep].copy()

    # Enforce string type for text columns to prevent encoding issues
    # Força o tipo string para colunas de texto para evitar problemas de codificação
    text_cols = ['id', 'issue_type', 'status', 'priority', 'analista']
    for col in text_cols:
        df_silver[col] = df_silver[col].astype(str)

    # 7. Parquet Persistence using PyArrow
    # Persistência em Parquet usando PyArrow
    try:
        df_silver.to_parquet(output_path, index=False, engine='pyarrow')
        print(f"✅ Silver layer completed! File saved at / Arquivo salvo em: {output_path}")
    except Exception as e:
        print(f"❌ Error saving Parquet file / Erro ao salvar Parquet: {e}")

if __name__ == "__main__":
    run_silver_transformation()