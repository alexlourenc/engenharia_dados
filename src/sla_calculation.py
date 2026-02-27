import pandas as pd
import requests
from datetime import datetime

def get_brazilian_holidays(year):
    """
    Consumes a public API to retrieve Brazilian national holidays for a given year.
    Consome uma API pública para obter feriados nacionais brasileiros de um determinado ano.
    """
    try:
        response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{year}")
        if response.status_code == 200:
            # Converts the API string dates into Python date objects
            # Converte as datas em string da API para objetos date do Python
            return [datetime.strptime(h['date'], '%Y-%m-%d').date() for h in response.json()]
    except Exception as e:
        print(f"⚠️ Error fetching holidays / Erro ao buscar feriados: {e}")
    return []

def calculate_business_hours(start_date, end_date):
    """
    Calculates the difference in business hours between two dates, excluding weekends and holidays.
    Calcula a diferença em horas úteis entre duas datas, excluindo fins de semana e feriados.
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return None
    
    # Create a range of days between the start and end dates
    # Cria um intervalo de dias entre as datas de início e fim
    days = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
    
    # Identify unique years in the range to fetch relevant holidays
    # Identifica os anos únicos no intervalo para buscar os feriados relevantes
    years = {start_date.year, end_date.year}
    holidays = []
    for y in years:
        holidays.extend(get_brazilian_holidays(y))
    
    # Filter only business days (Monday=0 to Friday=4) that are not holidays
    # Filtra apenas dias úteis (Segunda=0 a Sexta=4) que não sejam feriados
    business_days = [d for d in days if d.weekday() < 5 and d.date() not in holidays]
    
    # Simplified calculation: Total Business Days * 24 hours
    # Cálculo simplificado: Total de Dias Úteis * 24 horas
    total_hours = len(business_days) * 24 
    return round(total_hours, 2)

def get_expected_sla(priority):
    """
    Defines the expected SLA threshold based on ticket priority.
    Define o limite de SLA esperado com base na prioridade do chamado.
    """
    # Priority mapping: High (24h), Medium (72h), Low (120h)
    # Mapeamento de prioridade: Alta (24h), Média (72h), Baixa (120h)
    sla_map = {
        'High': 24,
        'Medium': 72,
        'Low': 120
    }
    return sla_map.get(priority, 0)