import pandas as pd
import numpy as np
import requests
import logging
from datetime import datetime
from functools import lru_cache

# Logging configuration / Configuração de Logging
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
            # Returns dates in YYYY-MM-DD format for numpy compatibility
            # Retorna as datas no formato AAAA-MM-DD para compatibilidade com numpy
            return [h['date'] for h in response.json()]
    except Exception as e:
        logger.error(f"⚠️ Error fetching holidays for {year}: {e}")
    return []

def calculate_business_hours(start_date, end_date, holidays=None):
    """
    Calculates business hours between two dates, excluding weekends and holidays.
    Calcula as horas úteis entre duas datas, excluindo fins de semana e feriados.
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return 0.0
    
    # Ensure we are working with timestamps / Garante que estamos trabalhando com timestamps
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)

    if start_ts > end_ts:
        return 0.0

    # Logic for holiday retrieval if not provided / Lógica para buscar feriados se não fornecidos
    if holidays is None:
        years = range(start_ts.year, end_ts.year + 1)
        holidays = []
        for y in years:
            holidays.extend(get_brazilian_holidays(y))
    
    # Convert holiday list to numpy format / Converte lista de feriados para formato numpy
    holidays_np = np.array(holidays, dtype='datetime64[D]')
    
    try:
        # np.busday_count calculates full business days between dates
        # np.busday_count calcula dias úteis inteiros entre as datas
        business_days = np.busday_count(
            start_ts.date(), 
            end_ts.date(), 
            holidays=holidays_np
        )
        
        # Standard conversion: 1 business day = 24h for SLA context
        # Conversão padrão: 1 dia útil = 24h para contexto de SLA
        # This can be adjusted to 8h if considering only business shifts
        # Isso pode ser ajustado para 8h se considerar apenas turnos comerciais
        business_hours = business_days * 24
        
        # Calculate fractional hours for the start and end day for precision
        # Calcula horas fracionadas para o dia de início e fim para precisão
        # (End time hours - Start time hours)
        time_adjustment = (end_ts.hour + end_ts.minute/60) - (start_ts.hour + start_ts.minute/60)
        
        final_hours = business_hours + time_adjustment
        
        return round(max(0.0, final_hours), 2)
        
    except Exception as e:
        logger.error(f"❌ Calculation error: {e}")
        return 0.0

def get_expected_sla(priority):
    """
    Returns the SLA threshold in hours based on priority.
    Retorna o limite de SLA em horas com base na prioridade.
    """
    sla_map = {
        'High': 24,    # 1 Business Day
        'Medium': 72,  # 3 Business Days
        'Low': 120     # 5 Business Days
    }
    return sla_map.get(priority, 0)