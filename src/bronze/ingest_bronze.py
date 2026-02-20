import os
import json

def run_ingestion():
    print("--- 🔍 Iniciando Ingestão Bronze (Local) ---")
    
    # Caminho definido na convenção
    source_path = "data/bronze/bronze_issues.json"
    
    # 1. Verifica se o arquivo existe fisicamente
    if not os.path.exists(source_path):
        print(f"❌ ERRO: Arquivo não encontrado em: {os.path.abspath(source_path)}")
        return

    # 2. Tenta ler o conteúdo
    try:
        with open(source_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 3. Validação de leitura (O "Saber se está lendo corretamente")
        print(f"✅ Sucesso! O arquivo foi lido corretamente.")
        
        # Se os dados forem uma lista, mostra quantos chamados existem
        if isinstance(data, list):
            print(f"📊 Total de registros encontrados: {len(data)}")
            if len(data) > 0:
                print(f"📋 Exemplo do primeiro registro: {data[0]}")
        
        # Se o JSON tiver uma chave pai (ex: 'issues')
        elif isinstance(data, dict):
            print(f"🔑 Chaves encontradas no JSON: {list(data.keys())}")
            
    except json.JSONDecodeError:
        print("❌ ERRO: O arquivo existe, mas NÃO é um JSON válido (erro de formatação).")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    run_ingestion()