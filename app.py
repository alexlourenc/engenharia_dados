import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import time

# Page Configuration / Configuração da Página
st.set_page_config(page_title="JIRA SLA Management Console", layout="wide")

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

df_raw = load_data()

if df_raw is not None:
    # --- SIDEBAR FILTERS / FILTROS ---
    st.sidebar.title("📅 Filters & Period / Filtros")
    
    # 1. Date Range Filter
    min_date = df_raw['created_at'].min().date()
    max_date = df_raw['created_at'].max().date()
    date_range = st.sidebar.date_input("Period / Período", [min_date, max_date])

    # 2. Issue Type Filter
    issue_types = ["All / Todos"] + sorted(df_raw['issue_type'].unique().tolist())
    selected_type = st.sidebar.selectbox("Issue Type / Tipo de Chamado", issue_types)

    # 3. Analyst Filter
    analysts = ["All / Todos"] + sorted(df_raw['analista'].unique().tolist())
    selected_analyst = st.sidebar.selectbox("Analyst / Analista", analysts)

    # --- NOVO: SEÇÃO DE AJUDA E ENCERRAMENTO ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("🏁 Session Control / Sessão")
    
    with st.sidebar.expander("How to close? / Como encerrar?"):
        st.write("""
        1. Vá ao terminal (CMD).
        2. Pressione **Ctrl + C**.
        3. Digite **deactivate**.
        """)
    
    # Lógica de Filtragem
    df = df_raw.copy()
    if len(date_range) == 2:
        df = df[(df['created_at'].dt.date >= date_range[0]) & (df['created_at'].dt.date <= date_range[1])]
    
    if selected_type != "All / Todos":
        df = df[df['issue_type'] == selected_type]

    if selected_analyst != "All / Todos":
        df = df[df['analista'] == selected_analyst]

    # --- DASHBOARD LAYOUT ---
    st.title("🏆 SLA Performance & Management")
    st.markdown(f"**Análise de:** {selected_type} | **Analista:** {selected_analyst}")
    st.markdown("---")

    # --- TOP METRICS ---
    c1, c2, c3, c4 = st.columns(4)
    total_tkt = len(df)
    compliance = (df['is_sla_met'].sum() / total_tkt * 100) if total_tkt > 0 else 0
    
    c1.metric("Total Tickets", total_tkt)
    c2.metric("SLA Compliance", f"{compliance:.1f}%")
    c3.metric("Avg. Resolution Time", f"{df['hours_resolution'].mean():.1f}h" if total_tkt > 0 else "0h")
    c4.metric("Active Analysts", df['analista'].nunique())

    # --- MONTHLY TREND ---
    st.subheader("📈 Monthly SLA Trend / Tendência Mensal de SLA")
    if total_tkt > 0:
        trend_data = df.groupby('month_year').agg(
            compliance=('is_sla_met', lambda x: (x.sum() / len(x) * 100))
        ).reset_index()
        
        fig_trend = px.line(trend_data, x='month_year', y='compliance', 
                            title=f"Compliance % Over Months", markers=True)
        fig_trend.add_hline(y=80, line_dash="dash", line_color="red")
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.warning("No data for selection.")

    # --- PERFORMANCE RANKING ---
    st.markdown("---")
    st.subheader("🥇 Analyst Performance Ranking")
    
    if total_tkt > 0:
        rank_df = df.groupby('analista').agg(
            total_tickets=('id', 'count'),
            met_sla=('is_sla_met', 'sum'),
            avg_hours=('hours_resolution', 'mean')
        ).reset_index()
        
        rank_df['compliance_rate'] = (rank_df['met_sla'] / rank_df['total_tickets'] * 100).round(1)
        rank_df = rank_df.sort_values(by='compliance_rate', ascending=False)
        
        st.table(rank_df[['analista', 'total_tickets', 'compliance_rate', 'avg_hours']])

        fig_rank = px.bar(rank_df, x='compliance_rate', y='analista', orientation='h',
                          color='compliance_rate', color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_rank, use_container_width=True)

else:
    st.error("❌ Gold Layer not found. Please run the pipeline first.")