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

HEADERS = {
    "chave-api-dados": API_KEY,
    "Accept": "application/json"
}

def buscar_orgaos() -> list[str]:
    orgaos = []
    pagina = 1
    while True:
        url = f"{API_URL}/orgaos-siafi"
        params = {"pagina": pagina}
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        dados = response.json()
        if not dados:
            break
        # Filtra códigos inválidos
        validos = [o["codigo"] for o in dados if "INVALIDO" not in o["descricao"].upper()]
        orgaos.extend(validos)
        logger.info(f"Órgãos - página {pagina}: {len(validos)} válidos de {len(dados)}")
        pagina += 1
    logger.info(f"Total de órgãos válidos encontrados: {len(orgaos)}")
    return orgaos

def buscar_despesas_orgao(ano: int, orgao: str) -> list[dict]:
    todas = []
    pagina = 1
    url = f"{API_URL}/despesas/por-orgao"
    while True:
        params = {
            "ano": ano,
            "pagina": pagina,
            "tamanhoPagina": pagina_tamanho,
            "orgaoSuperior": orgao
        }
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()
            dados = response.json()
            if not dados:
                break
            todas.extend(dados)
            logger.info(f"  Órgão {orgao} - página {pagina}: {len(dados)} registros")
            pagina += 1
        except requests.exceptions.HTTPError as e:
            logger.warning(f"  Órgão {orgao} - erro na página {pagina}: {e}")
            break
    return todas

def data_ingestion(ano: int) -> pd.DataFrame:
    logger.info(f"Iniciando download - Ano: {ano}")
    orgaos = buscar_orgaos()
    todos_os_registros = []

    for i, orgao in enumerate(orgaos, 1):
        logger.info(f"[{i}/{len(orgaos)}] Baixando órgão: {orgao}")
        registros = buscar_despesas_orgao(ano, orgao)
        todos_os_registros.extend(registros)
        logger.info(f"  Total acumulado: {len(todos_os_registros)} registros")

    df = pd.DataFrame(todos_os_registros)
    logger.info(f"Download concluído. Total de registros: {len(df)}")
    return df

def save_bronze(df: pd.DataFrame, ano: int) -> str:
    os.makedirs(bronze_path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = os.path.join(bronze_path, f"gastos_{ano}_{timestamp}.csv")
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    logger.info(f"Arquivo salvo em: {caminho}")
    return caminho

def exec_ingestion():
    logger.info("====== INÍCIO DA INGESTÃO ======")
    df = data_ingestion(ano)
    caminho = save_bronze(df, ano)
    logger.info("====== FIM DA INGESTÃO ======")
    return caminho

if __name__ == "__main__":
    exec_ingestion()





