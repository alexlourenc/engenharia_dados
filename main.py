import sys
import os
import time
from pathlib import Path

# Add the 'src' folder to the system path to enable module imports
# Adiciona a pasta 'src' ao sistema para permitir as importações dos seus módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Import layer modules and validation functions
# Importação dos módulos das camadas e funções de validação
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
    Orchestrates the complete data engineering pipeline from Bronze to Data Quality.
    Orquestra o pipeline completo de engenharia de dados, da Bronze à Qualidade de Dados.
    """
    start_time = time.time()
    
    print("="*60)
    print("🚀 JIRA DATA ENGINEERING PIPELINE / PIPELINE DE ENGENHARIA DE DADOS")
    print("="*60)

    # --- PHASE 1: BRONZE (Ingestion) ---
    # --- FASE 1: BRONZE (Ingestão) ---
    print("\n[STEP 1/4] BRONZE PHASE: Azure Blob Storage Ingestion...")
    print("[ETAPA 1/4] FASE BRONZE: Ingestão do Azure Blob Storage...")
    if ingest_bronze():
        print("✔️ Raw data persisted successfully. / Dados brutos persistidos.")
    else:
        print("❌ Critical failure during ingestion. Aborting. / Falha crítica. Interrompendo.")
        return

    # --- PHASE 2: SILVER (Transformation) ---
    # --- FASE 2: SILVER (Transformação) ---
    print("\n[STEP 2/4] SILVER PHASE: Cleaning, Normalization, and Typing...")
    print("[ETAPA 2/4] FASE SILVER: Limpeza, Normalização e Tipagem...")
    try:
        run_silver_transformation()
        print("✔️ Normalized data saved in Parquet (Silver). / Dados salvos em Parquet.")
    except Exception as e:
        print(f"❌ Error in Silver Phase / Erro na Fase Silver: {e}")
        return

    # --- PHASE 3: GOLD (Business Rules/SLA) ---
    # --- FASE 3: GOLD (Regras de Negócio/SLA) ---
    print("\n[STEP 3/4] GOLD PHASE: SLA Calculation (Business Days & Holidays)...")
    print("[ETAPA 3/4] FASE GOLD: Cálculo de SLA (Dias Úteis e Feriados)...")
    try:
        build_gold()
        print("✔️ Metrics and reports generated (Gold). / Métricas e relatórios gerados.")
    except Exception as e:
        print(f"❌ Error in Gold Phase / Erro na Fase Gold: {e}")
        return

    # --- PHASE 4: VALIDATION (Data Quality) ---
    # --- FASE 4: VALIDAÇÃO (Data Quality) ---
    print("\n[STEP 4/4] QUALITY: Integrity and Rules Audit...")
    print("[ETAPA 4/4] QUALIDADE: Auditoria de Integridade e Regras...")
    try:
        validate_data_quality()
    except Exception as e:
        print(f"⚠️ Audit Alert / Alerta na Auditoria: {e}")
    
    end_time = time.time()
    total_time = round(end_time - start_time, 2)
    
    print("\n" + "="*60)
    print(f"✅ EXECUTION COMPLETED SUCCESSFULLY IN / FINALIZADA COM SUCESSO EM {total_time}s!")
    print("="*60)
    print(f"📍 Final Reports / Relatórios Finais: data/gold/")
    print(f"📍 Pipeline Status: Healthy / Saudável")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()