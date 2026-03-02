"""
PROJECT: JIRA SLA Management Console
AUTHOR: Alex Lourenço (https://github.com/alexlourenc/)
DESCRIPTION: Streamlit interface for executive dashboard, pipeline audit logs, and data governance.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import json
import logging
from pathlib import Path

# Configuring logging for production-grade monitoring
# Configurando o logging para monitoramento de nível de produção
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONSTANTS AND CONFIGURATION / CONSTANTES E CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent
PATHS = {
    "gold": PROJECT_ROOT / "data" / "gold" / "final_sla_report.parquet",
    "stats": PROJECT_ROOT / "data" / "pipeline_stats.json",
    "dictionary": PROJECT_ROOT / "data" / "data_dictionary.json"
}

st.set_page_config(
    page_title="JIRA SLA Management Console",
    layout="wide",
    page_icon="🚀",
    initial_sidebar_state="expanded"
)

# --- THEME AND CUSTOM CSS / TEMA E CSS PERSONALIZADO ---
st.markdown("""
    <style>
    .main { background-color: transparent; }
    .stMetric { padding: 20px; border-radius: 12px; border: 1px solid #444; }
    </style>
    """, unsafe_allow_html=True)

# --- HELPER FUNCTIONS / FUNÇÕES AUXILIARES ---

def apply_filters(df, dates, issue_type, analyst):
    """Applies global sidebar filters / Aplica filtros globais da barra lateral."""
    temp_df = df.copy()
    if 'created_at' in temp_df.columns and len(dates) == 2:
        temp_df['created_at'] = pd.to_datetime(temp_df['created_at'])
        temp_df = temp_df[(temp_df['created_at'].dt.date >= dates[0]) & 
                          (temp_df['created_at'].dt.date <= dates[1])]
    if issue_type != "All / Todos" and 'issue_type' in temp_df.columns:
        temp_df = temp_df[temp_df['issue_type'] == issue_type]
    if analyst != "All / Todos" and 'analista' in temp_df.columns:
        temp_df = temp_df[temp_df['analista'] == analyst]
    return temp_df

def file_preview_popover(file_path_str, filters, label="Preview Data"):
    """Interactive data preview and export / Prévia interativa e exportação de dados."""
    path = Path(file_path_str)
    if not path.exists():
        st.caption(f"🚫 Path not found: {file_path_str}")
        return

    with st.popover(f"🔍 {label}"):
        try:
            if path.suffix == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                issues = data.get('issues', data) if isinstance(data, dict) else data
                df_raw = pd.json_normalize(issues)
            elif path.suffix == '.parquet':
                df_raw = pd.read_parquet(path)
            else: return

            df_filtered = apply_filters(df_raw, filters['dates'], filters['type'], filters['analyst'])
            st.write(f"📊 **Records:** `{len(df_filtered)}` | **File:** `{path.name}`")
            st.dataframe(df_filtered, use_container_width=True)
            st.download_button("💾 Export Filtered CSV", df_filtered.to_csv(index=False), f"audit_{path.stem}.csv", key=f"dl_{path.stem}")
        except Exception as e:
            st.error(f"Error: {e}")

@st.cache_data(ttl=600)
def load_analytical_data():
    """Loads Gold analytical layer / Carrega a camada analítica Gold."""
    if PATHS["gold"].exists():
        try:
            df = pd.read_parquet(PATHS["gold"])
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['month_year'] = df['created_at'].dt.to_period('M').astype(str)
            return df
        except: return None
    return None

def load_json_meta(path_key):
    """Safely loads JSON metadata / Carrega metadados JSON com segurança."""
    path = PATHS.get(path_key)
    if path and path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return None
    return None

# --- CONTEXT INITIALIZATION / INICIALIZAÇÃO DE CONTEXTO ---
df_master = load_analytical_data()
stats = load_json_meta("stats")
dict_data = load_json_meta("dictionary")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🛰️ Data Ops")
    page = st.radio("Navigation:", ["📈 Executive Dashboard", "🔍 Pipeline Audit Logs", "📖 Data Dictionary"])
    
    current_filters = {'dates': [], 'type': "All / Todos", 'analyst': "All / Todos"}
    if df_master is not None:
        st.divider()
        st.subheader("🔍 Filters")
        min_d, max_d = df_master['created_at'].min().date(), df_master['created_at'].max().date()
        current_filters['dates'] = st.date_input("Period", [min_d, max_d])
        current_filters['type'] = st.selectbox("Issue Type", ["All / Todos"] + sorted(df_master['issue_type'].unique().tolist()))
        current_filters['analyst'] = st.selectbox("Analyst", ["All / Todos"] + sorted(df_master['analista'].unique().tolist()))
        df_view = apply_filters(df_master, current_filters['dates'], current_filters['type'], current_filters['analyst'])
    
    st.divider()
    if stats: st.sidebar.caption(f"Sync: {stats.get('execution_date', 'N/A')}")

# --- PAGE 1: EXECUTIVE DASHBOARD ---
if page == "📈 Executive Dashboard":
    st.title("🏆 JIRA SLA Performance")
    if df_master is not None:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Tickets", len(df_view))
        m2.metric("SLA Compliance", f"{(df_view['is_sla_met'].mean()*100):.1f}%" if len(df_view)>0 else "0%")
        m3.metric("Avg. Resolution", f"{df_view['hours_resolution'].mean():.1f}h" if len(df_view)>0 else "0h")
        m4.metric("Active Analysts", df_view['analista'].nunique())

        # SLA Alert Logic
        rank = df_view.groupby('analista').agg(
            tickets=('id', 'count'), 
            compliance=('is_sla_met', lambda x: round(x.mean()*100, 1))
        ).reset_index()
        
        underperformers = rank[rank['compliance'] < 80].sort_values(by='compliance', ascending=True)
        
        if not underperformers.empty:
            st.error(f"🚨 **ALERTA DE SLA:** {len(underperformers)} analista(s) abaixo da meta de 80%.")
            with st.expander("Ver analistas em alerta", expanded=True):
                for _, row in underperformers.iterrows():
                    st.warning(f"🚩 **{row['analista']}**: {row['compliance']}% (Total: {row['tickets']} chamados)")

        # --- REVISED TABS WITH MULTI-LEVEL DRILL DOWN ---
        # --- ABAS REVISADAS COM DETALHAMENTO MULTINÍVEL ---
        tab_rank, tab_trend, tab_analyst, tab_type, tab_detail = st.tabs([
            "🥇 Ranking", "📉 Trends", "👤 Analyst Detail", "🏷️ Issue Type", "📋 Detailed Audit"
        ])
        
        with tab_rank:
            rank['Status'] = rank['compliance'].apply(lambda x: "✅ OK" if x >= 80 else "⚠️ ALERT")
            st.dataframe(rank.sort_values(by='compliance', ascending=False), use_container_width=True, hide_index=True)
            
        with tab_trend:
            trend_df = df_view.groupby('month_year').agg(score=('is_sla_met', 'mean')).reset_index()
            trend_df['score'] *= 100
            st.plotly_chart(px.line(trend_df, x='month_year', y='score', title="SLA % Progression", markers=True), use_container_width=True)

        # NEW: ANALYST DRILL-DOWN / NOVO: DETALHAMENTO POR ANALISTA
        with tab_analyst:
            st.subheader("👤 Individual Analyst Profile")
            sel_analyst = st.selectbox("Select Analyst to Drill Down:", sorted(df_view['analista'].unique()))
            df_analyst = df_view[df_view['analista'] == sel_analyst]
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Analyst Tickets", len(df_analyst))
            c2.metric("Personal Compliance", f"{(df_analyst['is_sla_met'].mean()*100):.1f}%")
            c3.metric("Avg Resolution (h)", f"{df_analyst['hours_resolution'].mean():.1f}h")
            
            st.dataframe(df_analyst[['id', 'issue_type', 'priority', 'hours_resolution', 'is_sla_met', 'created_at']], use_container_width=True, hide_index=True)

        # NEW: ISSUE TYPE DRILL-DOWN / NOVO: DETALHAMENTO POR TIPO DE CHAMADO
        with tab_type:
            st.subheader("🏷️ Issue Type Analysis")
            sel_type = st.selectbox("Select Issue Type to Drill Down:", sorted(df_view['issue_type'].unique()))
            df_type = df_view[df_view['issue_type'] == sel_type]
            
            t1, t2, t3 = st.columns(3)
            t1.metric("Type Volume", len(df_type))
            t2.metric("Type Compliance", f"{(df_type['is_sla_met'].mean()*100):.1f}%")
            t3.metric("Type Avg Hours", f"{df_type['hours_resolution'].mean():.1f}h")
            
            st.plotly_chart(px.bar(df_type.groupby('analista').size().reset_index(name='count'), x='analista', y='count', title=f"Analyst Distribution for {sel_type}"), use_container_width=True)

        with tab_detail:
            st.markdown("### 📋 Full Audit Table")
            st.dataframe(df_view.sort_values(by='created_at', ascending=False), use_container_width=True, hide_index=True)
            st.download_button("💾 Download Detailed CSV", df_view.to_csv(index=False), "detailed_report.csv", "text/csv")
            
    else: 
        st.error("❌ Gold Layer not found.")

# --- PAGE 2: PIPELINE AUDIT LOGS ---
elif page == "🔍 Pipeline Audit Logs":
    st.title("🛡️ Engineering Audit Console")
    if stats and "workflow" in stats:
        wf = stats["workflow"]
        st.subheader("📋 Execution Summary (Terminal Logs)")
        if "steps" in stats:
            st.table(pd.DataFrame(stats["steps"]))
        st.divider()
        col_l, col_r = st.columns(2)
        with col_l:
            if "bronze" in wf:
                with st.container(border=True):
                    st.markdown("### 📦 STEP 1: Bronze")
                    for task in wf["bronze"].get('tasks', []):
                        st.write(f"✅ **{task['name']}**")
                        if "file" in task: file_preview_popover(task['file'], current_filters, "Inspect Bronze")
            if "gold" in wf:
                with st.container(border=True):
                    st.markdown("### 🏆 STEP 3: Gold")
                    for task in wf["gold"].get('tasks', []):
                        st.write(f"✅ **{task['name']}**")
                        if "outputs" in task:
                            for out in task['outputs']: 
                                if out.endswith('.parquet'): file_preview_popover(out, current_filters, f"Inspect Gold")
        with col_r:
            if "silver" in wf:
                with st.container(border=True):
                    st.markdown("### 🧹 STEP 2: Silver")
                    for task in wf["silver"].get('tasks', []):
                        st.write(f"✅ **{task['name']}**")
                        if "file" in task: file_preview_popover(task['file'], current_filters, "Inspect Silver")
            if "quality" in wf:
                with st.container(border=True):
                    st.markdown("### 🛡️ STEP 4: Quality")
                    st.write(f"✅ **Data Integrity Audit**")
                    with st.popover("🔍 Inspect All Process Steps & Export"):
                        st.markdown("#### Detailed Execution Table")
                        if "steps" in stats:
                            log_df = pd.DataFrame(stats["steps"])
                            st.dataframe(log_df, use_container_width=True, hide_index=True)
                            st.divider()
                            st.markdown("#### Quality Metrics & Logic")
                            q_tasks = wf["quality"].get('tasks', [])
                            for q in q_tasks:
                                logic_info = next((item for item in dict_data.get("process_quality_logs", []) 
                                                 if item["test_id"] == q["name"]), {}) if dict_data else {}
                                st.write(f"🔹 **{q['name']}**: {q.get('result', 'N/A')}")
                                if logic_info: st.caption(f"💡 Logic: {logic_info.get('logic_pt')}")
                            st.divider()
                            st.download_button("💾 Export Process Log (CSV)", log_df.to_csv(index=False), f"pipeline_log_{stats.get('execution_date','now')}.csv", "text/csv", use_container_width=True)

# --- PAGE 3: DOCUMENTATION ---
elif page == "📖 Data Dictionary":
    st.title("📖 Documentation")
    if dict_data and "fields_metadata" in dict_data:
        st.subheader("Field Definitions")
        st.dataframe(pd.DataFrame(dict_data["fields_metadata"]), use_container_width=True, hide_index=True)
        if "process_quality_logs" in dict_data:
            st.divider()
            st.subheader("Quality Validation Rules")
            st.dataframe(pd.DataFrame(dict_data["process_quality_logs"]), use_container_width=True, hide_index=True)