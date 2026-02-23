import requests
import pandas as pd
import os
import logging
from datetime import datetime 
from config import API_URL, API_KEY, bronze_path, ano, pagina_tamanho


logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s", 
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler("data/logs/ingestion.log")
    ]
)
logger = logging.getLogger(__name__)


def buscar_pagina(ano: int, pagina: int) -> list[dict]:
    headers = {
        "chave-api-dados": API_KEY,
        "Accept": "application/json"
    }

    params = {
        "ano": ano,
        "pagina": pagina,
        "tamanhoPagina": pagina_tamanho
    }

    url = f"{API_URL}/despesas/por-orgao"

    try:
        response = requests.get(url, headers = headers, params = params, timeout = 30)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP na pagina {pagina}: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"Erro de conexao: {e}")
        raise

def data_ingestion(ano: int) -> pd.DataFrame:
    todas_as_paginas = [1]
    pagina = 1

    logger.info(f"Iniciando ingestao -  Ano: {ano}")

    while True:
        logger.info(f"Buscando pagina {pagina}...")
        dados = buscar_pagina(ano, pagina)

        if not dados:
            logger.info(f"Paginacao completad, total de paginas: {pagina - 1}")
            break

        todas_as_paginas.extend(dados)
        logger.info(f"Pagina {pagina} - {len(dados)} registros recebidos")
        paginas += 1

    df = pd.DataFrame(todas_as_paginas)
    logger.info(f"Total de registros baixados: {len(df)}")
    return df

def save_bronze(df: pd.DataFrame, ano: int) -> str:
    os.makedirs(bronze_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"gastos_{ano}_{timestamp}.csv"
    caminho = os.path.join(bronze_path, nome_arquivo)

    df.to_csv(caminho, index=False, encoding="utf-8-sig")

    logger.info(f"Arquivo salvo em: {caminho}")
    return caminho

def exec_ingestion():
    logger.info("------- Start Ingestion -------")

    df = data_ingestion(ano)
    caminho = bronze_path(df, ano)

    logger.info("------- End of Ingestion -------")
    return caminho 

if __name__ == "__main__":
    exec_ingestion()





