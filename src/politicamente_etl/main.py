# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 15:49:00

import os
import argparse
import io
import zipfile
import uuid
import logging
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests
import pandas as pd
from tqdm import tqdm

# --- CONFIGURAÇÃO ---
SCRIPT_VERSION = "1.5.0"
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))
TSE_CAND_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
TSE_VOTES_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona"
DATA_DIR = "data"
BATCH_SIZE = 1000

# --- CONFIGURAÇÃO DE LOGGING ---
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(__name__)

logger = setup_logging()

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def get_tse_data_generator(year, base_url, file_prefix, force_download=False):
    """
    Função geradora que baixa um ZIP e produz DataFrames de cada CSV interno,
    um de cada vez, para economizar memória.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    zip_filename = f"{file_prefix}_{year}.zip"
    zip_filepath = os.path.join(DATA_DIR, zip_filename)

    if not os.path.exists(zip_filepath) or force_download:
        zip_url = f"{base_url}/{zip_filename}"
        logger.info(f"Baixando dados de: {zip_url}")
        try:
            response = requests.get(zip_url, stream=True)
            response.raise_for_status()
            with open(zip_filepath, 'wb') as f:
                total_size = int(response.headers.get('content-length', 0))
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=zip_filename) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        pbar.update(len(chunk))
                        f.write(chunk)
            logger.info(f"Download concluído. Arquivo salvo em: {zip_filepath}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao baixar o arquivo ZIP: {e}")
            return
    else:
        logger.info(f"Usando arquivo ZIP local já baixado: {zip_filepath}")

    try:
        with zipfile.ZipFile(zip_filepath) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError("Nenhum arquivo CSV encontrado no ZIP.")

            # Prioriza o arquivo consolidado
            consolidated_file = f"{file_prefix}_{year}_BRASIL.csv"
            if consolidated_file in csv_files:
                logger.info(f"Encontrado arquivo consolidado, processando apenas: {consolidated_file}")
                csv_files = [consolidated_file]

            for csv_filename in csv_files:
                logger.info(f"Lendo arquivo: {csv_filename}")
                with z.open(csv_filename) as csv_file:
                    yield pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
    except Exception as e:
        logger.error(f"Erro ao processar o arquivo: {e}")
        return

def update_results(df_generator):
    """Atualiza a tabela de candidaturas com os resultados da votação."""
    logger.info("🚀 Iniciando a atualização dos resultados das candidaturas...")

    aggregated_results = {}

    for df in df_generator:
        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Agregando votos de {df['SG_UF'].iloc[0]}", leave=False):
            key = str(row['SQ_CANDIDATO'])
            if key not in aggregated_results:
                aggregated_results[key] = {"total_votes": 0, "status": row['DS_SIT_TOT_TURNO']}
            aggregated_results[key]["total_votes"] += int(row['QT_VOTOS'])

    logger.info(f"Agregação concluída. {len(aggregated_results)} resultados únicos de candidatos para atualizar.")

    db = get_db_session()
    try:
        updates = [{"sq_tse": key, "total_votes": value["total_votes"], "status": value["status"]} for key, value in aggregated_results.items()]

        updated_count = 0
        with tqdm(total=len(updates), desc="Atualizando Resultados no DB") as pbar:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                with db.begin():
                    for item in batch:
                        result = db.execute(
                            text("UPDATE candidacies SET total_votes_received = :total_votes, status_resultado = :status WHERE sq_candidate_tse = :sq_tse"),
                            item
                        )
                        if result.rowcount > 0:
                            updated_count += 1
                        else:
                            logger.warning(f"Nenhuma candidatura encontrada para o SQ_CANDIDATO: {item['sq_tse']}. Nenhum registro foi atualizado.")
                pbar.update(len(batch))

        logger.info(f"✅ Atualização de resultados concluída. {updated_count} registros de candidaturas foram atualizados.")
    except Exception as e:
        logger.error(f"Erro ao atualizar os resultados: {e}")
        db.rollback()
    finally:
        db.close()

# ... (outras funções de seeding permanecem as mesmas, mas usando logger em vez de print) ...
def seed_parties(df_generator):
    """Popula a tabela de partidos a partir de um DataFrame."""
    logger.info("🚀 Iniciando a população da tabela de partidos...")
    all_parties = pd.DataFrame()
    for df in tqdm(df_generator, desc="Lendo arquivos de dados"):
        all_parties = pd.concat([all_parties, df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']]])

    parties_df = all_parties.drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        parties_to_upsert = [{"num": int(row["NR_PARTIDO"]), "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]} for _, row in parties_df.iterrows()]
        with tqdm(total=len(parties_to_upsert), desc="Processando Partidos") as pbar:
            for i in range(0, len(parties_to_upsert), BATCH_SIZE):
                batch = parties_to_upsert[i:i + BATCH_SIZE]
                db.execute(text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"), batch)
                db.commit()
                pbar.update(len(batch))
        logger.info("✅ População de partidos concluída.")
    except Exception as e:
        logger.error(f"Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    logger.info(f"DomTech Forger - ETL Script v{SCRIPT_VERSION} iniciado.")
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    base_parser.add_argument("--force-download", action='store_true')

    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos.", parents=[base_parser])
    parser_parties.set_defaults(func=lambda args: seed_parties(get_tse_data_generator(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))

    # ... (outros parsers) ...

    parser_results = subparsers.add_parser("update_results", help="Atualiza os resultados de votação.", parents=[base_parser])
    parser_results.set_defaults(func=lambda args: update_results(get_tse_data_generator(args.year, TSE_VOTES_BASE_URL, "votacao_candidato_munzona", args.force_download)))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()