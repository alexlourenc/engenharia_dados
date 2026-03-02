import pandas as pd
import json
import logging
from pathlib import Path

# Logging setup
# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def validate_data_quality():
    """
    Performs data quality audit and returns results for the dashboard.
    Realiza auditoria de qualidade e retorna resultados para o dashboard.
    """
    base_dir = Path(__file__).resolve().parents[1]
    gold_path = base_dir / "data" / "gold" / "final_sla_report.parquet"
    
    results = {
        "null_check": "Failed",
        "chronology_check": "Failed",
        "status": "❌"
    }

    if not gold_path.exists():
        return results

    try:
        df = pd.read_parquet(gold_path)
        
        # Null Check
        # Verificação de Nulos
        null_count = df.isna().sum().sum()
        results["null_check"] = "No nulls found" if null_count == 0 else f"{null_count} nulls detected"
        
        # Chronology Check (Resolution must be >= Creation)
        # Verificação Cronológica (Resolução deve ser >= Criação)
        invalid_dates = len(df[df['resolved_at'] < df['created_at']])
        results["chronology_check"] = "All records validated" if invalid_dates == 0 else f"{invalid_dates} invalid dates"
        
        if null_count == 0 and invalid_dates == 0:
            results["status"] = "✔️"
            
        return results
    
    except Exception as e:
        logger.error(f"Error during quality audit: {e}")
        return results