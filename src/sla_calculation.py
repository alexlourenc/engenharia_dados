import pandas as pd
import numpy as np
import requests
import logging
from functools import lru_cache

# Logging configuration
# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@lru_cache(maxsize=12)
def get_brazilian_holidays(year):
    """
    Fetches Brazilian national holidays for a given year from BrasilAPI.
    Busca feriados nacionais brasileiros para um determinado ano na BrasilAPI.
    """
    try:
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}", timeout=10)
        if response.status_code == 200:
            return [h['date'] for h in response.json()]
    except Exception as e:
        logger.error(f"Error fetching holidays for {year}: {e}")
    return []

def calculate_business_hours(start_date, end_date, holidays=None):
    """
    Calculates business hours between two dates, excluding weekends and holidays.
    Calcula as horas úteis entre duas datas, excluindo fins de semana e feriados.
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return 0.0
    
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)

    if start_ts > end_ts:
        return 0.0

    if holidays is None:
        years = range(start_ts.year, end_ts.year + 1)
        holidays = []
        for y in years:
            holidays.extend(get_brazilian_holidays(y))
    
    holidays_np = np.array(holidays, dtype='datetime64[D]')
    
    try:
        business_days = np.busday_count(
            start_ts.date(), 
            end_ts.date(), 
            holidays=holidays_np
        )
        
        business_hours = business_days * 24
        
        time_adjustment = (end_ts.hour + end_ts.minute / 60) - (start_ts.hour + start_ts.minute / 60)
        final_hours = business_hours + time_adjustment
        
        return round(max(0.0, final_hours), 2)
        
    except Exception as e:
        logger.error(f"Calculation error: {e}")
        return 0.0

def get_expected_sla(priority):
    """
    Returns the SLA threshold in hours based on priority.
    Retorna o limite de SLA em horas com base na prioridade.
    """
    sla_map = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return sla_map.get(priority, 0)