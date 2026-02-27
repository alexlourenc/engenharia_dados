import pandas as pd
import json
from pathlib import Path

def validate_data_quality():
    """
    Performs an automated data quality audit across Bronze, Silver, and Gold layers.
    Realiza uma auditoria automatizada de qualidade de dados nas camadas Bronze, Silver e Gold.
    """
    base_dir = Path(__file__).resolve().parents[1]
    bronze_path = base_dir / "data" / "bronze" / "jira_issues_raw.json"
    silver_path = base_dir / "data" / "silver" / "jira_issues_clean.parquet"
    gold_path = base_dir / "data" / "gold" / "final_sla_report.parquet"

    print("="*50)
    print("🔍 STARTING QUALITY AUDIT / INICIANDO AUDITORIA DE QUALIDADE")
    print("="*50)

    # --- 1. Volumetric Validation / Validação de Volumetria ---
    try:
        # Check raw records in Bronze
        # Verifica registros brutos na Bronze
        with open(bronze_path, 'r') as f:
            bronze_data = json.load(f)
        total_bronze = len(bronze_data['issues'])
        
        # Check records in Silver (after cleaning)
        # Verifica registros na Silver (após limpeza)
        df_silver = pd.read_parquet(silver_path)
        total_silver = len(df_silver)
        
        # Check records in Gold (final business logic)
        # Verifica registros na Gold (lógica de negócio final)
        df_gold = pd.read_parquet(gold_path)
        total_gold = len(df_gold)

        print(f"📊 Volumetrics / Volumetria:")
        print(f"   - Bronze: {total_bronze} records / registros")
        print(f"   - Silver: {total_silver} records (after date cleaning) / registros (após limpeza)")
        print(f"   - Gold:   {total_gold} records (after status filter) / registros (após filtro)")
    except Exception as e:
        print(f"❌ Error reading files / Erro na leitura dos arquivos: {e}")
        return

    # --- 2. Business Logic Test (SLA) / Teste de Lógica de Negócio (SLA) ---
    print("\n🧪 Testing SLA Logic / Testando Lógica de SLA:")
    
    # Verify if the expected SLA matches the priority thresholds
    # Verifica se o SLA esperado condiz com os limites de prioridade
    sla_check = df_gold.groupby('priority')['sla_expected'].unique().to_dict()
    expected_map = {'High': [24], 'Medium': [72], 'Low': [120]}
    
    for prio, val in expected_map.items():
        if prio in sla_check and list(sla_check[prio]) == val:
            print(f"   ✅ Priority {prio}: {val[0]}h SLA validated. / Prioridade {prio} validada.")
        else:
            print(f"   ❌ SLA rule error for / Erro na regra de SLA para: {prio}.")

    # --- 3. Integrity Test (Null Values) / Teste de Integridade (Valores Nulos) ---
    print("\n🛡️ Null Check in Gold Layer / Verificação de Nulos na Gold:")
    null_report = df_gold.isnull().sum()
    if null_report.sum() == 0:
        print("   ✅ No null values found in Gold Layer. / Nenhum valor nulo encontrado.")
    else:
        print(f"   ⚠️ Alert: Null values found / Valores nulos encontrados:\n{null_report[null_report > 0]}")

    # --- 4. Sanity Check: Resolution Dates / Validação Cronológica ---
    print("\n📅 Chronological Validation / Validação Cronológica:")
    # Resolution date must never be earlier than creation date
    # A resolução nunca pode ser anterior à data de criação
    time_travelers = df_gold[df_gold['resolved_at'] < df_gold['created_at']]
    if time_travelers.empty:
        print("   ✅ All resolution dates are post-creation. / Datas de resolução válidas.")
    else:
        print(f"   ❌ ERROR: {len(time_travelers)} tickets with backdated resolution! / chamados com erro!")

    print("\n" + "="*50)
    print("🏁 Audit Completed! / Auditoria Concluída!")
    print("="*50)

if __name__ == "__main__":
    validate_data_quality()