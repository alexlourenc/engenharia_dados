import pandas as pd
import json
import os

def run_silver_transformation():
    print("--- 🥈 Iniciando Transformação Silver ---")
    
    input_path = "data/bronze/bronze_issues.json"
    output_dir = "data/silver"
    output_path = os.path.join(output_dir, "silver_issues.parquet")

    # 1. Carregar o JSON bruto
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Normalizar a chave 'issues'
        df = pd.json_normalize(raw_data, record_path=['issues'])
        print(f"✅ Dados carregados: {len(df)} registros encontrados.")
    except Exception as e:
        print(f"❌ Erro ao processar estrutura do JSON: {e}")
        return

    # 2. Extrair o nome do analista (Assignee)
    def extract_assignee_name(assignee_list):
        if isinstance(assignee_list, list) and len(assignee_list) > 0:
            return assignee_list[0].get('name')
        return None

    df['assignee_name'] = df['assignee'].apply(extract_assignee_name)

    # 3. EXTRAÇÃO DAS DATAS (Ajuste para a chave 'timestamps')
    def extract_timestamp(ts_list, field):
        if isinstance(ts_list, list) and len(ts_list) > 0:
            return ts_list[0].get(field)
        return None

    # Criamos as colunas extraindo da lista 'timestamps'
    df['created_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'created_at'))
    df['resolved_at'] = df['timestamps'].apply(lambda x: extract_timestamp(x, 'resolved_at'))

    # 4. Tratamento de Datas
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # 5. Limpeza de registros sem data de criação válida
    before_count = len(df)
    df = df.dropna(subset=['created_at'])
    print(f"🧹 Limpeza: {before_count - len(df)} registros com datas inválidas removidos.")

    # 6. Salvar em Parquet (Removendo colunas de listas para evitar erro de formato)
    os.makedirs(output_dir, exist_ok=True)
    
    # Selecionamos apenas as colunas que não são listas/dicionários
    cols_to_keep = ['id', 'issue_type', 'status', 'priority', 'assignee_name', 'created_at', 'resolved_at']
    
    # Garantimos que apenas colunas existentes sejam salvas
    final_cols = [c for c in cols_to_keep if c in df.columns]
    
    df[final_cols].to_parquet(output_path, index=False, engine='fastparquet')
    
    print(f"✅ Camada Silver concluída! Arquivo salvo com colunas: {df[final_cols].columns.tolist()}")

if __name__ == "__main__":
    run_silver_transformation()