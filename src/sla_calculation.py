import pandas as pd
import requests
from datetime import datetime

def get_brazilian_holidays(year):
    """Consome API pública para obter feriados nacionais brasileiros."""
    try:
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}")
        if response.status_code == 200:
            return [datetime.strptime(h['date'], '%Y-%m-%d').date() for h in response.json()]
    except Exception as e:
        print(f"⚠️ Erro ao buscar feriados: {e}")
    return []

def calculate_business_hours(start_date, end_date):
    """Calcula a diferença em horas úteis entre duas datas."""
    if pd.isna(start_date) or pd.isna(end_date):
        return None
    
    # Criar intervalo de dias entre as datas
    days = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
    
    # Buscar feriados para os anos envolvidos
    years = {start_date.year, end_date.year}
    holidays = []
    for y in years:
        holidays.extend(get_brazilian_holidays(y))
    
    # Filtrar apenas dias úteis (Segunda=0 a Sexta=4) e que não sejam feriados
    business_days = [d for d in days if d.weekday() < 5 and d.date() not in holidays]
    
    # Cálculo simplificado em horas (Dias Úteis * 24h)
    # Ajustamos para considerar a fração do primeiro e último dia se necessário
    total_hours = len(business_days) * 24 
    return round(total_hours, 2)

def get_expected_sla(priority):
    """Define o SLA esperado com base na prioridade conforme premissas."""
    sla_map = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return sla_map.get(priority, 0)