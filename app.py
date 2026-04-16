import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="Tech Salaries Dashboard", layout="wide")

st.sidebar.header("Filtros Dinâmicos")

filtro_exp = st.sidebar.selectbox(
    "Nível de Experiência",
    ["Todos", "Júnior", "Pleno", "Sênior", "Executivo"]
)

st.title("📊 Tech Salaries Analysis")

con = duckdb.connect()

col1, col2 = st.columns(2)

query_base = "SELECT job_title, AVG(salary_in_usd) as avg_salary FROM 'data/silver/salaries_cleaned.parquet'"

if filtro_exp != "Todos":
    query_base += f" WHERE experience_level = '{filtro_exp}'"

query_base += " GROUP BY job_title ORDER BY avg_salary DESC LIMIT 10"

df_jobs = con.execute(query_base).fetch_arrow_table()

fig_jobs = px.bar(
    df_jobs,
    x='avg_salary',
    y='job_title',
    orientation='h',
    title=f'Top 10 Salários - {filtro_exp}',
    color='avg_salary',
    color_continuous_scale='Viridis'
)
fig_jobs.update_yaxes(categoryorder='total ascending')

with col1:
    st.plotly_chart(fig_jobs, use_container_width=True)

df_trend = con.execute("SELECT * FROM 'data/gold/salary_by_year_exp.parquet'").fetch_arrow_table()

fig_trend = px.line(
    df_trend,
    x='work_year',
    y='avg_salary',
    color='experience_level',
    title='Evolução Salarial por Experiência',
    markers=True
)
fig_trend.update_xaxes(type='category', categoryorder='category ascending')

with col2:
    st.plotly_chart(fig_trend, use_container_width=True)