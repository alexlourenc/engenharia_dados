import sys
import os
import time
from pathlib import Path

# Adiciona a pasta 'src' ao sistema para permitir as importações dos seus módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importação dos processos que você construiu
from bronze.ingest_bronze import ingest_bronze
from silver.transform_silver import run_silver_transformation
from gold.build_gold import build_gold

def run_pipeline():
    start_time = time.time()
    print("="*50)
    print("🚀 INICIANDO PIPELINE DE ENGENHARIA DE DADOS - JIRA")
    print("="*50)

    # --- FASE 1: BRONZE ---
    print("\n[ETAPA 1/3] Acessando Azure Blob Storage...")
    if ingest_bronze():
        print("✔️ Fase Bronze Concluída.")
    else:
        print("❌ Falha na Fase Bronze. Interrompendo pipeline.")
        return

    # --- FASE 2: SILVER ---
    print("\n[ETAPA 2/3] Iniciando Transformação e Limpeza (Silver)...")
    try:
        run_silver_transformation()
        print("✔️ Fase Silver Concluída.")
    except Exception as e:
        print(f"❌ Erro na Fase Silver: {e}")
        return

    # --- FASE 3: GOLD ---
    print("\n[ETAPA 3/3] Calculando SLA e Gerando Relatórios (Gold)...")
    try:
        build_gold()
        print("✔️ Fase Gold Concluída.")
    except Exception as e:
        print(f"❌ Erro na Fase Gold: {e}")
        return

    end_time = time.time()
    total_time = round(end_time - start_time, 2)
    
    print("\n" + "="*50)
    print(f"✅ PIPELINE FINALIZADO COM SUCESSO EM {total_time}s!")
    print("="*50)
    print("📂 Verifique os resultados em: data/gold/")

if __name__ == "__main__":
    run_pipeline()