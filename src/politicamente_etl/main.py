# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 01:13:03

import os
import argparse
import io
import zipfile
import uuid
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAÇÃO ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))
TSE_CAND_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
TSE_VOTES_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona"
DATA_DIR = "data"
BATCH_SIZE = 1000

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def get_tse_data(year, base_url, file_prefix, force_download=False):
    """Função genérica para baixar e extrair dados do TSE."""
    os.makedirs(DATA_DIR, exist_ok=True)
    zip_filename = f"{file_prefix}_{year}.zip"
    zip_filepath = os.path.join(DATA_DIR, zip_filename)
    target_csv_filename = f"{file_prefix}_{year}_BRASIL.csv"

    if not os.path.exists(zip_filepath) or force_download:
        zip_url = f"{base_url}/{zip_filename}"
        print(f"Baixando dados de: {zip_url}")
        try:
            response = requests.get(zip_url, stream=True)
            response.raise_for_status()
            with open(zip_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download concluído. Arquivo salvo em: {zip_filepath}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao baixar o arquivo ZIP: {e}")
            return None
    else:
        print(f"Usando arquivo ZIP local já baixado: {zip_filepath}")

    try:
        with zipfile.ZipFile(zip_filepath) as z:
            if target_csv_filename not in z.namelist():
                raise FileNotFoundError(f"Arquivo '{target_csv_filename}' não encontrado no ZIP.")

            with z.open(target_csv_filename) as csv_file:
                df = pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
                print(f"Sucesso! {len(df)} registros lidos do arquivo CSV.")
                return df
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo: {e}")
        return None

def seed_parties(df):
    """Popula a tabela de partidos a partir de um DataFrame."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de partidos...")
    parties_df = df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        parties_to_upsert = [{"num": int(row["NR_PARTIDO"]), "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]} for _, row in parties_df.iterrows()]
        with tqdm(total=len(parties_to_upsert), desc="Processando Partidos") as pbar:
            for i in range(0, len(parties_to_upsert), BATCH_SIZE):
                batch = parties_to_upsert[i:i + BATCH_SIZE]
                db.execute(text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"), batch)
                db.commit()
                pbar.update(len(batch))
        print("✅ População de partidos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_politicians(df):
    """Popula a tabela de políticos a partir de um DataFrame."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de políticos...")
    politicians_df = df[['NM_CANDIDATO', 'NM_URNA_CANDIDATO']].drop_duplicates()
    db = get_db_session()
    try:
        politicians_to_insert = [{"id": uuid.uuid4(), "name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]} for _, row in politicians_df.iterrows()]
        with tqdm(total=len(politicians_to_insert), desc="Inserindo Políticos") as pbar:
            for i in range(0, len(politicians_to_insert), BATCH_SIZE):
                batch = politicians_to_insert[i:i + BATCH_SIZE]
                db.execute(text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) ON CONFLICT (full_name, nickname) DO NOTHING"), batch)
                db.commit()
                pbar.update(len(batch))
        print("✅ População de políticos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de políticos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_candidacies(df, year):
    """Popula as tabelas de eleições e candidaturas."""
    # ... (código mantido da versão anterior) ...

def update_results(df):
    """Atualiza a tabela de candidaturas com os resultados da votação."""
    if df is None: return
    print("🚀 Iniciando a atualização dos resultados das candidaturas...")

    # Agrupa os votos por candidato para obter o total
    results_df = df.groupby('SQ_CANDIDATO')['QT_VOTOS_NOMINAIS'].sum().reset_index()

    db = get_db_session()
    try:
        updates = []
        for _, row in tqdm(results_df.iterrows(), total=len(results_df), desc="Preparando Atualizações"):
            # O ID sequencial do candidato no arquivo de votação corresponde ao número eleitoral na candidatura
            updates.append({
                "electoral_number": int(row["SQ_CANDIDATO"]),
                "total_votes": int(row["QT_VOTOS_NOMINAIS"])
            })

        print(f"Atualizando {len(updates)} candidaturas...")
        with tqdm(total=len(updates), desc="Atualizando Resultados") as pbar:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                # Esta é uma forma simplificada. Uma abordagem mais robusta usaria o ID da candidatura.
                for item in batch:
                    db.execute(
                        text("UPDATE candidacies SET total_votos_recebidos = :total_votes WHERE electoral_number = :electoral_number"),
                        item
                    )
                db.commit()
                pbar.update(len(batch))

        print("✅ Atualização de resultados concluída.")
    except Exception as e:
        print(f"❌ Erro ao atualizar os resultados: {e}")
        db.rollback()
    finally:
        db.close()


def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    base_parser.add_argument("--force-download", action='store_true')

    parser_parties = subparsers.add_parser("seed_parties", help="Popula APENAS a tabela de partidos.", parents=[base_parser])
    parser_parties.set_defaults(func=lambda args: seed_parties(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula APENAS a tabela de políticos.", parents=[base_parser])
    parser_politicians.set_defaults(func=lambda args: seed_politicians(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula a tabela de candidaturas.", parents=[base_parser])
    parser_candidacies.set_defaults(func=lambda args: seed_candidacies(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download), args.year))

    parser_results = subparsers.add_parser("update_results", help="Atualiza os resultados (votos) das candidaturas.", parents=[base_parser])
    parser_results.set_defaults(func=lambda args: update_results(get_tse_data(args.year, TSE_VOTES_BASE_URL, "votacao_candidato_munzona", args.force_download)))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()