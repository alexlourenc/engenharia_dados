import pandas as pd
import json
import os
from pathlib import Path
from dotenv import load_dotenv

def run_silver_transformation():
    print("--- 🥈 Iniciando Transformação Silver ---")
    
    # 1. Configuração de Caminhos Dinâmicos
    # Usamos Path para garantir que o script funcione independente de onde é chamado
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=base_dir / ".env")
    
    # Nome do arquivo definido no seu .env ou padrão
    input_filename = os.getenv("BLOB_NAME", "jira_issues_raw.json")
    input_path = base_dir / "data" / "bronze" / input_filename
    
    output_dir = base_dir / "data" / "silver"
    output_path = output_dir / "jira_issues_clean.parquet"

    # 2. Carregar o JSON bruto
    try:
        if not input_path.exists():
            print(f"❌ Erro: Arquivo de origem não encontrado em {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Normaliza a chave 'issues' para um DataFrame inicial
        df = pd.json_normalize(raw_data, record_path=['issues'])
        print(f"✅ Dados carregados: {len(df)} registros encontrados.")
    except Exception as e:
        print(f"❌ Erro ao processar estrutura do JSON: {e}")
        return

    # 3. Extração e Limpeza de Campos Aninhados
    
    # Função para extrair o analista (Assignee)
    def extract_assignee_name(assignee_list):
        if isinstance(assignee_list, list) and len(assignee_list) > 0:
            return assignee_list[0].get('name')
        return "Sem Analista"

    # Função para extrair datas da lista 'timestamps'
    def extract_timestamp(ts_list, field):
        if isinstance(ts_list, list) and len(ts_list) > 0:
            return ts_list[0].get(field)
        return None

    # Aplicando as extrações
    df['analista'] = df['assignee'].apply(extract_assignee_name)
    df['created_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'created_at'))
    df['resolved_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'resolved_at'))

    # 4. Tratamento de Tipos e Datas
    # errors='coerce' garante que datas inválidas virem NaT (Not a Time)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # 5. Limpeza de registros sem data de criação válida (Premissa do Projeto)
    before_count = len(df)
    df = df.dropna(subset=['created_at']).copy()
    print(f"🧹 Limpeza: {before_count - len(df)} registros com datas inválidas removidos.")

    # 6. Preparação para Salvamento (Evitando erros de conversão UTF-8)
    os.makedirs(output_dir, exist_ok=True)
    
    # Selecionamos apenas as colunas necessárias para a Silver
    cols_to_keep = ['id', 'issue_type', 'status', 'priority', 'analista', 'created_at', 'resolved_at']
    df_silver = df[cols_to_keep].copy()

    # Forçamos colunas de texto a serem strings puras para evitar conflitos no buffer
    text_cols = ['id', 'issue_type', 'status', 'priority', 'analista']
    for col in text_cols:
        df_silver[col] = df_silver[col].astype(str)

    # 7. Persistência em Parquet usando PyArrow (Mais estável que fastparquet)
    try:
        df_silver.to_parquet(output_path, index=False, engine='pyarrow')
        print(f"✅ Camada Silver concluída! Arquivo salvo em: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo Parquet: {e}")

if __name__ == "__main__":
    run_silver_transformation()