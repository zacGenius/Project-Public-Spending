# app/pages/1_gastos_orgao.py
import streamlit as st
import plotly.express as px
import sys
import os

# Importa a função de conexão do app.py principal
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from app import load_data

st.title("Gastos por Órgão")

df = load_data()

# Filtro de ano na barra lateral
anos = sorted(df['ano'].unique(), reverse=True)
ano_selecionado = st.sidebar.selectbox("Selecione o Ano", anos)

# Filtro de top N
top_n = st.sidebar.slider("Top N órgãos", min_value=5, max_value=30, value=10)

# Filtra e agrega
df_filtrado = (
    df[df['ano'] == ano_selecionado]
    .groupby('orgao')['total_gastos']
    .sum()
    .sort_values(ascending=False)
    .head(top_n)
    .reset_index()
)

# Gráfico
fig = px.bar(
    df_filtrado,
    x='total_gastos',
    y='orgao',
    orientation='h',
    title=f"Top {top_n} órgãos que mais gastaram em {ano_selecionado}",
    labels={'total_gastos': 'Total Gasto (R$)', 'orgao': 'Órgão'},
    color='total_gastos',
    color_continuous_scale='Blues'
)

fig.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig, use_container_width=True)

# Tabela abaixo do gráfico
st.dataframe(
    df_filtrado.rename(columns={
        'orgao': 'Órgão',
        'total_gastos': 'Total Gasto (R$)'
    }),
    use_container_width=True
)
