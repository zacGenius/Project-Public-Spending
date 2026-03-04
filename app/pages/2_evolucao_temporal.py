# app/pages/2_evolucao_temporal.py
import streamlit as st
import plotly.express as px
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from app import load_data

st.title("Evolução Temporal dos Gastos")

df = load_data()

# Filtro de órgão
orgaos = sorted(df['orgao'].unique())
orgaos_selecionados = st.sidebar.multiselect(
    "Selecione os Órgãos",
    orgaos,
    default=orgaos[:5]  # primeiros 5 por padrão
)

if not orgaos_selecionados:
    st.warning("Selecione ao menos um órgão no menu lateral.")
    st.stop()

# Filtra
df_filtrado = df[df['orgao'].isin(orgaos_selecionados)]

# Agrega por ano e órgão
df_evolucao = (
    df_filtrado
    .groupby(['ano', 'orgao'])['total_gastos']
    .sum()
    .reset_index()
)

# Gráfico de linha
fig = px.line(
    df_evolucao,
    x='ano',
    y='total_gastos',
    color='orgao',
    title="Evolução de Gastos por Órgão ao Longo dos Anos",
    labels={
        'ano': 'Ano',
        'total_gastos': 'Total Gasto (R$)',
        'orgao': 'Órgão'
    },
    markers=True  # pontos em cada ano
)

st.plotly_chart(fig, use_container_width=True)

# Variação percentual ano a ano
st.markdown("### Variação Ano a Ano")

df_variacao = df_evolucao.copy()
df_variacao['variacao_pct'] = (
    df_variacao
    .groupby('orgao')['total_gastos']
    .pct_change() * 100
).round(2)

st.dataframe(
    df_variacao.rename(columns={
        'ano': 'Ano',
        'orgao': 'Órgão',
        'total_gastos': 'Total Gasto (R$)',
        'variacao_pct': 'Variação % (vs ano anterior)'
    }),
    use_container_width=True
)