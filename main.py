import sys
import os
import time
from pathlib import Path

def ensure_structure():
    """Cria a estrutura de pastas da Arquitetura Medallion se não existir."""
    folders = ['data/bronze', 'data/silver', 'data/gold']
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
            # Cria um .gitkeep para garantir que a pasta seja rastreada pelo Git
            with open(os.path.join(folder, '.gitkeep'), 'w') as f:
                pass
    print("✅ Estrutura de pastas verificada/criada.")

# Garante a estrutura antes de mais nada
ensure_structure()

# Adiciona a pasta 'src' ao sistema para permitir as importações dos módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importação dos módulos das camadas
try:
    from bronze.ingest_bronze import ingest_bronze
    from silver.transform_silver import run_silver_transformation
    from gold.build_gold import build_gold
    from validate_pipeline import validate_data_quality
except ImportError as e:
    print(f"❌ Error importing modules / Erro ao importar módulos: {e}")
    print("Ensure the 'src/' folder structure is correct. / Verifique a estrutura de pastas.")
    sys.exit(1)

def run_pipeline():
    """
    Orquestra o pipeline completo de engenharia de dados, da Bronze à Gold.
    """
    start_time = time.time()
    
    print("="*60)
    print("🚀 JIRA DATA ENGINEERING PIPELINE / PIPELINE DE ENGENHARIA DE DADOS")
    print("="*60)

    # --- FASE 1: BRONZE (Ingestão) ---
    print("\n[STEP 1/4] BRONZE PHASE: Azure Blob Storage Ingestion...")
    if ingest_bronze():
        print("✔️ Raw data persisted successfully. / Dados brutos persistidos.")
    else:
        print("❌ Critical failure during ingestion. Aborting. / Falha crítica. Interrompendo.")
        return

    # --- FASE 2: SILVER (Transformação) ---
    print("\n[STEP 2/4] SILVER PHASE: Cleaning, Normalization, and Typing...")
    try:
        run_silver_transformation()
        print("✔️ Normalized data saved in Parquet (Silver). / Dados salvos em Parquet.")
    except Exception as e:
        print(f"❌ Error in Silver Phase / Erro na Fase Silver: {e}")
        return

    # --- FASE 3: GOLD (Regras de Negócio/SLA) ---
    print("\n[STEP 3/4] GOLD PHASE: SLA Calculation (Business Days & Holidays)...")
    try:
        build_gold()
        print("✔️ Metrics and reports generated (Gold). / Métricas e relatórios gerados.")
    except Exception as e:
        print(f"❌ Error in Gold Phase / Erro na Fase Gold: {e}")
        return

    # --- FASE 4: VALIDAÇÃO (Data Quality) ---
    print("\n[STEP 4/4] QUALITY: Integrity and Rules Audit...")
    try:
        validate_data_quality()
        print("✔️ Quality audit finished. / Auditoria de qualidade finalizada.")
    except Exception as e:
        print(f"⚠️ Audit Alert / Alerta na Auditoria: {e}")
    
    end_time = time.time()
    total_time = round(end_time - start_time, 2)
    
    # --- FINALIZAÇÃO E CHAMADA PARA O STREAMLIT ---
    print("\n" + "="*60)
    print(f"✅ EXECUTION COMPLETED SUCCESSFULLY IN {total_time}s!")
    print("="*60)
    print(f"📍 Final Reports: data/gold/")
    print(f"📍 Pipeline Status: Healthy / Saudável")
    print("="*60)
    
    # Instrução para o Dashboard
    print("\n📊 VISUALIZAÇÃO DOS DADOS:")
    print("O pipeline foi concluído. Para visualizar o Dashboard interativo, execute:")
    print("-" * 40)
    print("streamlit run app.py")
    print("-" * 40 + "\n")

    # No final da função run_pipeline() do seu main.py
    print("\n📊 VISUALIZAÇÃO DOS DADOS:")
    print("O pipeline foi concluído. Para abrir o Dashboard, execute:")
    print("-" * 50)
    print("streamlit run app.py")
    print("-" * 50)
    print("💡 DICA: Para encerrar o Dashboard e liberar o terminal,")
    print("   pressione as teclas [Ctrl] + [C] simultaneamente.")
    print("-" * 50 + "\n")




if __name__ == "__main__":
    run_pipeline()