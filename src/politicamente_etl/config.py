# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 20:38:26

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURAÇÃO GERAL ---
SCRIPT_VERSION = "3.0.0"
load_dotenv()

# --- VARIÁVEIS DE AMBIENTE ---
DATABASE_URL = os.getenv("DATABASE_URL")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))

# --- CONSTANTES ---
TSE_CAND_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
TSE_VOTES_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona"
DATA_DIR = "data"
LOG_DIR = "logs"
BATCH_SIZE = 1000

# --- CONFIGURAÇÃO DE LOGGING ---
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = f"etl_run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)

    logger = logging.getLogger("politicamente_etl")
    logger.setLevel(LOG_LEVEL)

    if logger.hasHandlers():
        logger.handlers.clear()

    # Handler para o arquivo de log
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter("%(asctime)s - [%(levelname)s] - %(message)s"))
    logger.addHandler(file_handler)

    # Handler para o console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("[%(levelname)s] - %(message)s"))
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")