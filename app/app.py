import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Page config
st.set_page_config(
    page_title="PUBLIC_SPEND",
    page_icon="💸",
    layout="wide"
)

# -------------------------------------------------------
# CONEXÃO COM BANCO
# Usando st.cache_resource para não reconectar a cada
# interação do usuário, a conexão fica em cache enquanto
# o app estiver rodando
# -------------------------------------------------------
@st.cache_resource
def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "public_spend")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

# -------------------------------------------------------
# FUNÇÃO DE LEITURA
# Centralizada aqui para todas as páginas importarem.
# st.cache_data cacheia o resultado da query por 1 hora.
# Sem isso, cada clique do usuário refaz a query no banco.
# -------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    engine = get_engine()
    query = text("SELECT * FROM gold_gold.fato_gastos_por_orgao_ano")
    
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

 

# Página inicial
st.title("- Public Spend Dashboard -")
st.markdown("---")

# Métricas gerais no topo
df = load_data()
st.write(df.columns)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Total Gasto",
        value=f"R$ {df['total_gastos'].sum():,.2f}"
    )

with col2:
    st.metric(
        label="Órgãos",
        value=df['orgao'].nunique()
    )

with col3:
    st.metric(
        label="Anos com Dados",
        value=df['ano'].nunique()
    )

st.markdown("---")
st.markdown("Navegue pelas páginas no menu à esquerda.")


