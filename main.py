"""
PROJECT: JIRA SLA Management Console
AUTHOR: Alex Lourenço (https://github.com/alexlourenc/)
DESCRIPTION: Streamlit interface for executive dashboard, pipeline audit logs, and data governance.
"""
import sys
import os
import time
import json
import logging
import pandas as pd
from pathlib import Path

# Setup logging for background monitoring
# Configura o logging para monitoramento em segundo plano
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Constants for project paths using absolute resolution
# Constantes para os caminhos do projeto usando resolução absoluta
PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_PATHS = {
    "bronze": PROJECT_ROOT / "data" / "bronze" / "jira_issues_raw.json",
    "silver": PROJECT_ROOT / "data" / "silver" / "jira_issues_clean.parquet",
    "gold": PROJECT_ROOT / "data" / "gold" / "final_sla_report.parquet",
    "stats": PROJECT_ROOT / "data" / "pipeline_stats.json"
}

def clear_terminal():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def ensure_structure():
    """Ensures that the required Medallion folder structure exists."""
    # Garante que a estrutura de pastas Medalhão necessária exista.
    for folder in ['data/bronze', 'data/silver', 'data/gold']:
        path = PROJECT_ROOT / folder
        path.mkdir(parents=True, exist_ok=True)
        (path / '.gitkeep').touch()

# Initialize environment / Inicializa o ambiente
ensure_structure()
sys.path.append(str(PROJECT_ROOT / "src"))

try:
    from bronze.ingest_bronze import ingest_bronze
    from silver.transform_silver import run_silver_transformation
    from gold.build_gold import build_gold
    from validate_pipeline import validate_data_quality
except ImportError as e:
    logger.error(f"❌ Import Error / Erro de Importação: {e}")
    sys.exit(1)

def get_actual_counts():
    """Calculates record counts for the audit report."""
    # Calcula a contagem de registros para o relatório de auditoria.
    counts = {"bronze": 0, "silver": 0}
    if PROJECT_PATHS["bronze"].exists():
        try:
            with open(PROJECT_PATHS["bronze"], 'r', encoding='utf-8') as f:
                data = json.load(f)
                issues = data.get('issues') or data.get('values') or data
                counts["bronze"] = len(issues) if isinstance(issues, list) else 0
        except: pass
    if PROJECT_PATHS["silver"].exists():
        try:
            counts["silver"] = len(pd.read_parquet(PROJECT_PATHS["silver"]))
        except: pass
    return counts

def save_pipeline_stats(results, total_time):
    """Saves all process steps and metrics to pipeline_stats.json."""
    # Salva todos os passos do processo e métricas no pipeline_stats.json.
    counts = get_actual_counts() 
    
    def get_status_text(index):
        if index < len(results):
            return "completed" if results[index][2] == "✔️" else "failed"
        return "failed"

    invalid_qty = max(0, counts["bronze"] - counts["silver"])

    stats = {
        "execution_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_time": total_time,
        "workflow": {
            "bronze": {"step": 1, "phase": "Bronze", "tasks": [{"name": "Ingestion", "status": get_status_text(0), "records": counts["bronze"], "file": str(PROJECT_PATHS["bronze"].absolute())}]},
            "silver": {"step": 2, "phase": "Silver", "tasks": [{"name": "Cleaning", "status": get_status_text(1), "invalid_records_removed": invalid_qty}, {"name": "file", "status": get_status_text(1), "file": str(PROJECT_PATHS["silver"].absolute())}]},
            "gold": {"step": 3, "phase": "Gold", "tasks": [{"name": "SLA Calculation", "status": get_status_text(2), "outputs": [str(PROJECT_PATHS["gold"].absolute())]}]},
            "quality": {"step": 4, "phase": "Quality", "tasks": [{"name": "Audit", "status": get_status_text(3), "result": "Integrity Checked"}]}
        },
        # Structured log from all steps / Log estruturado de todos os passos
        "steps": [{"STEP": r[0], "DESCRIPTION": r[1], "STATUS": r[2]} for r in results]
    }
    
    with open(PROJECT_PATHS["stats"], 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)

def run_pipeline():
    """End-to-end orchestration."""
    # Orquestração de ponta a ponta.
    results = []
    start_time = time.time()

    if ingest_bronze():
        results.append(["BRONZE", "Ingestion completed.", "✔️"])
        try:
            run_silver_transformation()
            results.append(["SILVER", "Cleaning & Normalization.", "✔️"])
            try:
                build_gold()
                results.append(["GOLD", "SLA Rules Applied.", "✔️"])
            except Exception as e: results.append(["GOLD", f"Gold Error: {e}", "❌"])
        except Exception as e: results.append(["SILVER", f"Silver Error: {e}", "❌"])
    else: results.append(["BRONZE", "Ingestion failed.", "❌"])

    try:
        validate_data_quality()
        results.append(["QUALITY", "Integrity audit finished.", "✔️"])
    except Exception as e: results.append(["QUALITY", f"Audit Alert: {e}", "⚠️"])
    
    total_time = round(time.time() - start_time, 2)
    save_pipeline_stats(results, total_time)
    
    clear_terminal()
    print("="*60)
    print("🚀 PIPELINE FINISHED & LOGS SAVED")
    logger.info("Then run / Depois execute: streamlit run app.py")
    

if __name__ == "__main__":
    run_pipeline()