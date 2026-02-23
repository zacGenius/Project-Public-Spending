import os 
import dotenv from load_dotenv

load_dotenv()

API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY = os.getenv("TRANSPARENCIA_API_KEY")

bronze_path = "data/bronze"

ano = 2024
pagina_tamanho = 500