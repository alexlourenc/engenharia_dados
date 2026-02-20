import pandas as pd
import requests
from datetime import datetime

# Memória temporária para não chamar a API 1000 vezes
HOLIDAY_CACHE = {}

def get_brazilian_holidays(year: int):
    """Obtém feriados e salva na memória para reuso."""
    if year in HOLIDAY_CACHE:
        return HOLIDAY_CACHE[year]
    
    print(f"🌐 Buscando feriados de {year} na API...")
    try:
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}", timeout=10)
        if response.status_code == 200:
            holidays = [h['date'] for h in response.json()]
            HOLIDAY_CACHE[year] = holidays
            return holidays
        return []
    except:
        return []

def calculate_resolution_hours(start_at, end_at):
    """Calcula horas úteis entre duas datas de forma eficiente."""
    if pd.isna(start_at) or pd.isna(end_at):
        return 0.0
    
    # Garantir formato datetime
    start_dt = pd.to_datetime(start_at)
    end_dt = pd.to_datetime(end_at)

    if end_dt < start_dt:
        return 0.0
    
    holidays = get_brazilian_holidays(start_dt.year)
    
    try:
        # Freq='C' para considerar os feriados passados
        business_days = pd.bdate_range(
            start=start_dt, 
            end=end_dt, 
            holidays=holidays, 
            freq='C'
        )
        
        # Se for no mesmo dia útil, calcula a diferença real
        if len(business_days) <= 1:
            diff = (end_dt - start_dt).total_seconds() / 3600
            return round(max(0, diff), 2)
        
        # Multiplica dias úteis por 24h
        return float(len(business_days) * 24)
    except:
        # Fallback simples
        return round((end_dt - start_dt).total_seconds() / 3600, 2)

def get_expected_sla_hours(priority):
    mapping = {"High": 24, "Medium": 72, "Low": 120}
    return mapping.get(priority, 120)