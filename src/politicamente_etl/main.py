# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 17:28:13

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
    """
    Baixa e/ou carrega os dados de uma eleição em um DataFrame do Pandas.
    Implementa um sistema de cache local para evitar downloads repetidos.
    """
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
    """Popula a tabela de partidos a partir de um DataFrame, com lógica de UPSERT robusta."""
    if df is None:
        print("DataFrame não fornecido. Abortando o seeding de partidos.")
        return

    print("🚀 Iniciando a população da tabela de partidos...")
    parties_df = df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        existing_parties_raw = db.execute(text("SELECT party_number, initials FROM parties")).all()
        existing_numbers = {p.party_number for p in existing_parties_raw}
        existing_initials = {p.initials for p in existing_parties_raw}

        for _, row in tqdm(parties_df.iterrows(), total=len(parties_df), desc="Populando Partidos"):
            party_number = int(row["NR_PARTIDO"])
            initials = row["SG_PARTIDO"]
            party_name = row["NM_PARTIDO"]

            if party_number in existing_numbers:
                db.execute(
                    text("UPDATE parties SET initials = :init, party_name = :name WHERE party_number = :num"),
                    {"num": party_number, "init": initials, "name": party_name}
                )
            elif initials in existing_initials:
                continue
            else:
                db.execute(
                    text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name)"),
                    {"num": party_number, "init": initials, "name": party_name}
                )
                existing_numbers.add(party_number)
                existing_initials.add(initials)

        db.commit()
        print(f"✅ População de partidos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def process_chunk(chunk, year, parties_cache, pbar_position):
    """Processa um 'pedaço' do DataFrame para inserir dados, com sua própria barra de progresso."""
    db = get_db_session()
    try:
        elections_cache = {}
        politicians_cache = {}

        for _, row in tqdm(chunk.iterrows(), total=len(chunk), desc=f"Worker {pbar_position}", position=pbar_position, leave=False):
            turn = int(row['NR_TURNO'])
            election_key = f"{year}-{turn}-{row['DS_ELEICAO']}"
            if election_key not in elections_cache:
                day = 2 if turn == 1 else 30
                election_date = date(year, 10, day)
                db.execute(
                    text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING"),
                    {"date": election_date, "type": row["DS_ELEICAO"], "turn": turn}
                )
                db.commit()
                election_result = db.execute(text("SELECT election_id FROM elections WHERE turn = :turn AND date_part('year', election_date) = :year AND election_type = :type"),
                                             {"turn": turn, "year": year, "type": row["DS_ELEICAO"]}).first()
                if election_result:
                    elections_cache[election_key] = election_result[0]
            election_id = elections_cache.get(election_key)

            politician_key = f'{row["NM_CANDIDATO"]}-{row["NM_URNA_CANDIDATO"]}'
            if politician_key not in politicians_cache:
                existing_politician = db.execute(text("SELECT politician_id FROM politicians WHERE full_name = :name AND nickname = :nick"), {"name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}).first()
                if existing_politician:
                    politician_id = existing_politician[0]
                else:
                    politician_id = db.execute(
                        text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) RETURNING politician_id"),
                        {"id": uuid.uuid4(), "name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}
                    ).scalar_one()
                politicians_cache[politician_key] = politician_id
            politician_id = politicians_cache.get(politician_key)

            party_id = parties_cache.get(int(row["NR_PARTIDO"]))
            if party_id and election_id and politician_id:
                db.execute(
                    text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number) VALUES (:p_id, :party_id, :e_id, :office, :num) ON CONFLICT DO NOTHING"),
                    {"p_id": politician_id, "party_id": party_id, "e_id": election_id, "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"])}
                )
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def seed_politicians_and_candidacies(df, year):
    """Popula as tabelas de politicos e candidaturas usando processamento paralelo."""
    if df is None:
        return

    print(f"🚀 Iniciando a população de políticos e candidaturas com até {MAX_WORKERS} workers paralelos...")

    db = get_db_session()
    parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
    db.close()

    chunk_size = len(df) // MAX_WORKERS if len(df) > MAX_WORKERS else len(df)
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # A barra de progresso geral agora rastreia a conclusão dos lotes (chunks)
        with tqdm(total=len(chunks), desc="Progresso Geral dos Lotes", position=0) as pbar:
            futures = {executor.submit(process_chunk, chunk, year, parties_cache, i + 1): i for i, chunk in enumerate(chunks)}

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Um erro ocorreu em um dos workers: {e}")
                pbar.update(1) # Atualiza a barra de progresso geral quando um lote termina

    print(f"✅ Concluído! Processamento de {len(df)} candidaturas finalizado.")


def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    parser.add_argument("--year", type=int, default=2022, help="O ano da eleição a ser processada (ex: 2022).")
    parser.add_argument("--force-download", action='store_true', help="Força o download de um novo arquivo ZIP, mesmo que um já exista localmente.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    parser_parties = subparsers.add_parser("seed_parties", help="Popula APENAS a tabela de partidos políticos.")
    parser_parties.set_defaults(func=lambda args: seed_parties(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula as tabelas de políticos e candidaturas.")
    parser_candidacies.set_defaults(func=lambda args: seed_politicians_and_candidacies(get_election_data_as_dataframe(args.year, args.force_download), args.year))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()