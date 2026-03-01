import sys
import os
import time
import json
import pandas as pd
from pathlib import Path

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def ensure_structure():
    folders = ['data/bronze', 'data/silver', 'data/gold']
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        Path(os.path.join(folder, '.gitkeep')).touch()

ensure_structure()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

try:
    from bronze.ingest_bronze import ingest_bronze
    from silver.transform_silver import run_silver_transformation
    from gold.build_gold import build_gold
    from validate_pipeline import validate_data_quality
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    sys.exit(1)

def get_actual_counts():
    counts = {"bronze": 0, "silver": 0, "gold": 0}
    p_bronze = Path('data/bronze/jira_issues_raw.json')
    p_silver = Path('data/silver/jira_issues_clean.parquet')
    
    if p_bronze.exists():
        try:
            with open(p_bronze, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    counts["bronze"] = len(data)
                elif isinstance(data, dict):
                    issues = data.get('issues') or data.get('values') or data.get('data')
                    counts["bronze"] = len(issues) if isinstance(issues, list) else 1
        except: pass

    if p_silver.exists():
        try:
            counts["silver"] = len(pd.read_parquet(p_silver))
        except: pass
            
    return counts

def save_pipeline_stats(results, total_time):
    """Gera o JSON de auditoria no formato exato solicitado."""
    counts = get_actual_counts() 
    
    path_b = str(Path("data/bronze/jira_issues_raw.json").absolute())
    path_s = str(Path("data/silver/jira_issues_clean.parquet").absolute())
    path_g = str(Path("data/gold/final_sla_report.parquet").absolute())

    status_bronze = "completed" if results[0][2] == "✔️" else "failed"
    status_silver = "completed" if results[1][2] == "✔️" else "failed"
    status_gold   = "completed" if results[2][2] == "✔️" else "failed"
    status_quality = "completed" if len(results) > 3 and results[3][2] == "✔️" else "failed"

    # Cálculo conforme sua lógica de records vs invalid
    invalid_qty = counts["bronze"] - counts["silver"]

    stats = {
        "execution_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_time": total_time,
        "workflow": {
            "bronze": {
                "step": 1,
                "phase": "Bronze",
                "tasks": [
                    {
                        "name": "Ingestion",
                        "status": status_bronze,
                        "records": counts["bronze"],
                        "file": path_b
                    }
                ]
            },
            "silver": {
                "step": 2,
                "phase": "Silver",
                "tasks": [
                    {"name": "Data load", "status": status_silver, "records": counts["bronze"]},
                    {"name": "Cleaning", "status": status_silver, "invalid_records_removed": invalid_qty},
                    {"name": "file", "status": status_silver, "file": path_s}
                ]
            },
            "gold": {
                "step": 3,
                "phase": "Gold",
                "tasks": [
                    {
                        "name": "SLA Calculation",
                        "status": status_gold,
                        "outputs": [
                            path_g,
                            "report_analista.csv",
                            "report_tipo_chamado.csv"
                        ]
                    }
                ]
            },
            "quality": {
                "step": 4,
                "phase": "Quality",
                "tasks": [
                    {
                        "name": "Null Check",
                        "status": status_quality,
                        "result": "No nulls found" if status_quality == "completed" else "Failed"
                    },
                    {
                        "name": "Chronological Validation",
                        "status": status_quality,
                        "result": "All records validated" if status_quality == "completed" else "Validation Error"
                    }
                ]
            }
        },
        "steps": [{"STEP": r[0], "DESCRIPTION": r[1], "STATUS": r[2]} for r in results]
    }
    
    with open('data/pipeline_stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)

def print_summary(results, total_time):
    clear_terminal()
    print("="*60)
    print("🚀 JIRA DATA ENGINEERING PIPELINE")
    print("="*60)
    header = f"{'STEP':<10} | {'DESCRIPTION':<35} | {'STATUS'}"
    print(header)
    print("-" * len(header))
    for step, desc, status in results:
        print(f"{step:<10} | {desc[:35]:<35} | {status}")
    print("-" * len(header))
    print(f"✅ COMPLETED IN {total_time}s | Dashboard: streamlit run app.py\n")

def run_pipeline():
    results = []
    clear_terminal()
    print("="*60)
    print("🔍 RUNNING AUTOMATED PIPELINE")
    print("="*60)

    start_time = time.time()

    # Step 1: BRONZE
    if ingest_bronze():
        results.append(["BRONZE", "Ingestion completed.", "✔️"])
    else:
        results.append(["BRONZE", "Ingestion failed.", "❌"])
        return

    # Step 2: SILVER
    try:
        run_silver_transformation()
        results.append(["SILVER", "Cleaning & Normalization.", "✔️"])
    except Exception as e:
        results.append(["SILVER", f"Error: {e}", "❌"])

    # Step 3: GOLD
    try:
        build_gold()
        results.append(["GOLD", "SLA Rules Applied.", "✔️"])
    except Exception as e:
        results.append(["GOLD", f"Error: {e}", "❌"])

    # Step 4: QUALITY
    try:
        validate_data_quality()
        results.append(["QUALITY", "Integrity audit finished.", "✔️"])
    except Exception as e:
         results.append(["QUALITY", f"Audit Alert: {e}", "⚠️"])
    
    total_time = round(time.time() - start_time, 2)
    save_pipeline_stats(results, total_time)
    print_summary(results, total_time)

if __name__ == "__main__":
    run_pipeline()