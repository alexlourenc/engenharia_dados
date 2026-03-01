import streamlit as st
import pandas as pd
import plotly.express as px
import json
from pathlib import Path

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="JIRA SLA Management Console", layout="wide", page_icon="🚀")

# --- FUNÇÕES DE CARREGAMENTO DE DADOS ---
@st.cache_data
def load_data():
    """Carrega os dados da camada Gold."""
    base_dir = Path(__file__).resolve().parent
    path = base_dir / "data" / "gold" / "final_sla_report.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['month_year'] = df['created_at'].dt.to_period('M').astype(str)
        return df
    return None

def load_audit_stats():
    """Carrega as estatísticas do arquivo pipeline_stats.json."""
    path = Path("data/pipeline_stats.json")
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def load_data_dictionary():
    """Carrega o dicionário de dados do arquivo JSON (Caminho Ajustado)."""
    #
    path = Path("data/data_dictionary.json")
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

# --- CARREGAMENTO GLOBAL ---
df_raw = load_data()
stats = load_audit_stats()
dict_data = load_data_dictionary()

# --- SIDEBAR: NAVEGAÇÃO E FILTROS ---
st.sidebar.title("🚀 Navigation")
page = st.sidebar.radio("Go to:", ["📊 Performance Dashboard", "🔍 Pipeline Audit Logs", "📖 Data Dictionary"])

st.sidebar.markdown("---")
st.sidebar.title("📅 Filters & Period")

if df_raw is not None:
    min_date = df_raw['created_at'].min().date()
    max_date = df_raw['created_at'].max().date()
    date_range = st.sidebar.date_input("Period", [min_date, max_date])

    issue_types = ["All / Todos"] + sorted(df_raw['issue_type'].unique().tolist())
    selected_type = st.sidebar.selectbox("Issue Type", issue_types)

    analysts = ["All / Todos"] + sorted(df_raw['analista'].unique().tolist())
    selected_analyst = st.sidebar.selectbox("Analyst", analysts)

    # Lógica de Filtragem Comum
    df_filtered = df_raw.copy()
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        df_filtered = df_filtered[(df_filtered['created_at'].dt.date >= date_range[0]) & 
                                  (df_filtered['created_at'].dt.date <= date_range[1])]
    
    if selected_type != "All / Todos":
        df_filtered = df_filtered[df_filtered['issue_type'] == selected_type]
    if selected_analyst != "All / Todos":
        df_filtered = df_filtered[df_filtered['analista'] == selected_analyst]

st.sidebar.markdown("---")
st.sidebar.subheader("🏁 Session Control")
with st.sidebar.expander("How to close?"):
    st.write("1. No terminal, Ctrl + C\n2. Digite 'deactivate'")

# --- SEÇÃO DO DESENVOLVEDOR (PROFISSIONAL) ---
st.sidebar.markdown("---")
st.sidebar.subheader("👨‍💻 Developer")
st.sidebar.markdown("**Alex Lourenço**")
st.sidebar.caption("Data Engineer | Analytics Solutions")

col_lnk, col_git = st.sidebar.columns(2)

with col_lnk:
    st.markdown(f"""
        <a href="https://www.linkedin.com/in/alexlourenc/" target="_blank">
            <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" width="100%">
        </a>
    """, unsafe_allow_html=True)

with col_git:
    st.markdown(f"""
        <a href="https://github.com/alexlourenc/engenharia_dados" target="_blank">
            <img src="https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white" width="100%">
        </a>
    """, unsafe_allow_html=True)

# --- PÁGINA 1: DASHBOARD DE PERFORMANCE ---
if page == "📊 Performance Dashboard":
    if df_raw is not None:
        st.title("🏆 SLA Performance & Management")
        st.markdown(f"**Analysis of:** {selected_type} | **Analyst:** {selected_analyst}")
        st.markdown("---")

        c1, c2, c3, c4 = st.columns(4)
        total_tkt = len(df_filtered)
        compliance = (df_filtered['is_sla_met'].sum() / total_tkt * 100) if total_tkt > 0 else 0
        
        c1.metric("Total Tickets", total_tkt)
        c2.metric("SLA Compliance", f"{compliance:.1f}%")
        c3.metric("Avg. Resolution Time", f"{df_filtered['hours_resolution'].mean():.1f}h" if total_tkt > 0 else "0h")
        c4.metric("Active Analysts", df_filtered['analista'].nunique())

        st.subheader("📈 Monthly SLA Trend")
        if total_tkt > 0:
            trend_data = df_filtered.groupby('month_year').agg(compliance=('is_sla_met', lambda x: (x.sum() / len(x) * 100))).reset_index()
            fig_trend = px.line(trend_data, x='month_year', y='compliance', markers=True, title="SLA % over Time")
            fig_trend.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="Target 80%")
            st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("---")
        st.subheader("🥇 Analyst Performance Ranking")
        if total_tkt > 0:
            rank_df = df_filtered.groupby('analista').agg(
                total_tickets=('id', 'count'), 
                met_sla=('is_sla_met', 'sum'), 
                avg_hours=('hours_resolution', 'mean')
            ).reset_index()
            rank_df['compliance_rate'] = (rank_df['met_sla'] / rank_df['total_tickets'] * 100).round(1)
            rank_df = rank_df.sort_values(by='compliance_rate', ascending=False)
            st.dataframe(rank_df[['analista', 'total_tickets', 'compliance_rate', 'avg_hours']], use_container_width=True)
            
            fig_rank = px.bar(rank_df, x='compliance_rate', y='analista', orientation='h', 
                             color='compliance_rate', color_continuous_scale='RdYlGn', title="Compliance by Analyst")
            st.plotly_chart(fig_rank, use_container_width=True)
    else:
        st.error("❌ Gold Layer not found.")

# --- PÁGINA 2: AUDITORIA DO PIPELINE ---
elif page == "🔍 Pipeline Audit Logs":
    st.title("🛡️ Data Engineering Pipeline Audit")
    
    if stats and "workflow" in stats:
        wf = stats["workflow"]
        st.info(f"Medallion Architecture | Filtros Ativos: {selected_type} / {selected_analyst}")
        
        st.subheader("📋 Execution Summary")
        if "steps" in stats:
            summary_df = pd.DataFrame(stats["steps"])
            def color_status(val):
                color = '#27ae60' if val == '✔️' else '#f39c12' if val == '➖' else '#e74c3c'
                return f'color: {color}; font-weight: bold'
            st.table(summary_df.style.applymap(color_status, subset=['STATUS']))
        
        st.divider()
        col_left, col_right = st.columns(2)
        
        with col_left:
            if "bronze" in wf:
                with st.container(border=True):
                    b = wf["bronze"]
                    st.subheader(f"📦 STEP {b['step']}: {b['phase']}")
                    for task in b['tasks']:
                        st.write(f"✅ **{task['name']}**")
                        if "records" in task:
                            st.metric("Total In Pipeline", f"{task['records']}")
                        st.code(task['file'], language="bash")

            with st.container(border=True):
                g = wf["gold"]
                st.subheader(f"🏆 STEP {g['step']}: {g['phase']}")
                st.write(f"✅ **{g['tasks'][0]['name']}**")
                st.write(f"📂 `final_sla_report.parquet`")

        with col_right:
            if "silver" in wf:
                with st.container(border=True):
                    s = wf["silver"]
                    st.subheader(f"🧹 STEP {s['step']}: {s['phase']}")
                    for task in s['tasks']:
                        if "records" in task:
                            st.info(f"Records: {task['records']}")
                        if "invalid_records_removed" in task:
                            st.warning(f"Cleaned: {task['invalid_records_removed']} rows")

            with st.container(border=True):
                st.subheader(f"🛡️ STEP 4: Quality & Volumetrics")
                st.markdown("📊 **Filtered Retention Summary:**")
                try:
                    v_brz = wf['bronze']['tasks'][0].get('records', 0)
                    v_gld_filt = len(df_filtered)
                    st.write(f"📦 **Total Ingested (Bronze):** `{v_brz}`")
                    st.write(f"🏆 **Selected/Filtered (Gold):** `{v_gld_filt}`")
                    percent = (v_gld_filt / v_brz) if v_brz > 0 else 0
                    st.write(f"📍 **Filter Coverage:** `{percent:.1%}` of total database")
                    st.progress(percent)
                except:
                    st.caption("Error calculating filtered volumetrics.")

        st.divider()
        st.subheader("🏆 Gold Layer Explorer (Filtered)")
        if df_filtered is not None:
            c_exp1, c_exp2 = st.columns([3, 1])
            with c_exp1:
                st.dataframe(df_filtered.head(100), use_container_width=True)
            with c_exp2:
                csv_data = df_filtered.to_csv(index=False).encode('utf-8')
                st.download_button("💾 Download Filtered CSV", csv_data, "filtered_sla.csv", "text/csv", use_container_width=True)
                st.metric("Rows Displayed", len(df_filtered))
    else:
        st.warning("⚠️ `pipeline_stats.json` not found.")

# --- PÁGINA 3: DICIONÁRIO DE DADOS ---
elif page == "📖 Data Dictionary":
    st.title("📖 Data Dictionary & Business Rules")
    st.markdown("Detailed technical documentation for the final analytical layer (Gold).")
    
    if dict_data and "data_dictionary" in dict_data:
        df_dict = pd.DataFrame(dict_data["data_dictionary"])
        display_df = df_dict.copy()
        display_df.columns = ["Field / Campo", "Type / Tipo", "Description (EN)", "Descrição (PT)", "Rules / Regras"]
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.divider()
        st.subheader("🔍 Detailed Field Analysis")
        
        for item in dict_data["data_dictionary"]:
            with st.expander(f"🔹 {item['field']} - {item['type']}"):
                col1, col2 = st.columns(2)
                col1.markdown(f"**English Description:**\n{item['description_en']}")
                col1.markdown(f"**Descrição em Português:**\n{item['description_pt']}")
                col2.info(f"**Data Validation / Rules:**\n{item['rules']}")
    else:
        st.error("❌ `data_dictionary.json` not found. Path checked: `data/data_dictionary.json`")

if stats:
    st.sidebar.caption(f"Last sync: {stats.get('execution_date', 'Unknown')}")