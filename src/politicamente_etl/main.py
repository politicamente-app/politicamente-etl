# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 17:57:22

import os
import argparse
import io
import zipfile
import uuid
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAÇÃO ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))
TSE_DATA_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
DATA_DIR = "data"

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def get_election_data_as_dataframe(year, force_download=False):
    """Baixa e/ou carrega os dados de uma eleição em um DataFrame do Pandas."""
    os.makedirs(DATA_DIR, exist_ok=True)
    zip_filename = f"consulta_cand_{year}.zip"
    zip_filepath = os.path.join(DATA_DIR, zip_filename)
    target_csv_filename = f"consulta_cand_{year}_BRASIL.csv"

    if not os.path.exists(zip_filepath) or force_download:
        zip_url = f"{TSE_DATA_BASE_URL}/{zip_filename}"
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
        for _, row in tqdm(parties_df.iterrows(), total=len(parties_df), desc="Populando Partidos"):
            db.execute(
                text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"),
                {"num": int(row["NR_PARTIDO"]), "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]}
            )
        db.commit()
        print(f"✅ População de partidos concluída.")
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
        politicians_to_insert = []
        for _, row in tqdm(politicians_df.iterrows(), total=len(politicians_df), desc="Preparando Políticos"):
            politicians_to_insert.append({
                "id": uuid.uuid4(),
                "name": row["NM_CANDIDATO"],
                "nick": row["NM_URNA_CANDIDATO"]
            })

        print(f"Inserindo {len(politicians_to_insert)} políticos no banco de dados (pode levar um momento)...")
        # Inserção em lote
        db.execute(
            text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) ON CONFLICT (full_name, nickname) DO NOTHING"),
            politicians_to_insert
        )
        db.commit()
        print("✅ População de políticos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de políticos: {e}")
        db.rollback()
    finally:
        db.close()

def process_candidacies_chunk(chunk, caches):
    """Processa um 'pedaço' do DataFrame para inserir candidaturas."""
    db = get_db_session()
    try:
        candidacies_to_insert = []
        for _, row in chunk.iterrows():
            election_key = f"{row['NR_ANO_ELEICAO']}-{row['NR_TURNO']}-{row['DS_ELEICAO']}"
            politician_key = f'{row["NM_CANDIDATO"]}-{row["NM_URNA_CANDIDATO"]}'

            election_id = caches['elections'].get(election_key)
            politician_id = caches['politicians'].get(politician_key)
            party_id = caches['parties'].get(int(row["NR_PARTIDO"]))

            if party_id and election_id and politician_id:
                candidacies_to_insert.append({
                    "p_id": politician_id, "party_id": party_id, "e_id": election_id,
                    "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"])
                })

        if candidacies_to_insert:
            db.execute(
                text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number) VALUES (:p_id, :party_id, :e_id, :office, :num) ON CONFLICT DO NOTHING"),
                candidacies_to_insert
            )
            db.commit()
        return len(chunk)
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def seed_candidacies(df, year):
    """Popula as tabelas de eleições e candidaturas."""
    if df is None: return
    print("🚀 Iniciando a população de eleições e candidaturas...")

    db = get_db_session()
    try:
        # Pré-carrega caches
        parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
        politicians_cache = {f'{p.full_name}-{p.nickname}': p.politician_id for p in db.execute(text("SELECT politician_id, full_name, nickname FROM politicians")).all()}

        # Cria as eleições primeiro
        elections_df = df[['NR_ANO_ELEICAO', 'NR_TURNO', 'DS_ELEICAO']].drop_duplicates()
        for _, row in elections_df.iterrows():
            turn = int(row['NR_TURNO'])
            day = 2 if turn == 1 else 30
            election_date = date(int(row['NR_ANO_ELEICAO']), 10, day)
            db.execute(text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING"),
                       {"date": election_date, "type": row["DS_ELEICAO"], "turn": turn})
        db.commit()
        elections_cache = {f"{e.date_part}-{e.turn}-{e.election_type}": e.election_id for e in db.execute(text("SELECT election_id, date_part('year', election_date) as date_part, turn, election_type FROM elections")).all()}

        chunks = [df.iloc[i:i + 10000] for i in range(0, len(df), 10000)]
        caches = {'parties': parties_cache, 'politicians': politicians_cache, 'elections': elections_cache}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            with tqdm(total=len(df), desc="Progresso Geral") as pbar:
                futures = [executor.submit(process_candidacies_chunk, chunk, caches) for chunk in chunks]
                for future in as_completed(futures):
                    processed_count = future.result()
                    pbar.update(processed_count)

        print(f"✅ Concluído! Processamento de {len(df)} candidaturas finalizado.")
    except Exception as e:
        print(f"❌ Erro durante o seeding de candidaturas: {e}")
    finally:
        db.close()


def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    parser_parties = subparsers.add_parser("seed_parties", help="Popula APENAS a tabela de partidos.")
    parser_parties.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    parser_parties.add_argument("--force-download", action='store_true')
    parser_parties.set_defaults(func=lambda args: seed_parties(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula APENAS a tabela de políticos.")
    parser_politicians.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    parser_politicians.add_argument("--force-download", action='store_true')
    parser_politicians.set_defaults(func=lambda args: seed_politicians(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula a tabela de candidaturas.")
    parser_candidacies.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    parser_candidacies.add_argument("--force-download", action='store_true')
    parser_candidacies.set_defaults(func=lambda args: seed_candidacies(get_election_data_as_dataframe(args.year, args.force_download), args.year))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()