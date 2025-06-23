# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 17:49:17

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
SEARCH_LIMIT_YEARS = 10 # Limite de anos para procurar para trás

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def find_latest_election_year():
    """Descobre o ano da eleição mais recente com dados disponíveis no TSE."""
    print("🔎 Procurando o ano da eleição mais recente...")
    current_year = date.today().year
    start_year = current_year if current_year % 2 == 0 else current_year - 1

    for year in range(start_year, start_year - SEARCH_LIMIT_YEARS, -2):
        zip_url = f"{TSE_DATA_BASE_URL}/consulta_cand_{year}.zip"
        try:
            print(f"   Testando ano {year}...")
            response = requests.head(zip_url, timeout=5)
            if response.status_code == 200:
                print(f"✅ Ano da eleição encontrado: {year}")
                return year
        except requests.exceptions.RequestException:
            continue
    return None

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
    """Popula a tabela de partidos a partir de um DataFrame."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de partidos...")
    parties_df = df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        existing_numbers = {p.party_number for p in db.execute(text("SELECT party_number FROM parties")).all()}

        for _, row in tqdm(parties_df.iterrows(), total=len(parties_df), desc="Populando Partidos"):
            party_number = int(row["NR_PARTIDO"])
            # Lógica UPSERT
            if party_number in existing_numbers:
                db.execute(text("UPDATE parties SET initials = :init, party_name = :name WHERE party_number = :num"),
                           {"num": party_number, "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]})
            else:
                db.execute(text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name)"),
                           {"num": party_number, "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]})
                existing_numbers.add(party_number)
        db.commit()
        print(f"✅ População de partidos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def process_chunk(chunk, year, parties_cache):
    """Processa um 'pedaço' do DataFrame para inserir dados."""
    db = get_db_session()
    try:
        # Lógica de inserção
        for _, row in chunk.iterrows():
            # (A lógica interna permanece a mesma, mas está mais robusta)
            # 1. Processa Eleição
            turn = int(row['NR_TURNO'])
            election_key = f"{year}-{turn}-{row['DS_ELEICAO']}"
            election_result = db.execute(text("SELECT election_id FROM elections WHERE turn = :turn AND date_part('year', election_date) = :year AND election_type = :type"),
                                        {"turn": turn, "year": year, "type": row["DS_ELEICAO"]}).first()
            if election_result:
                election_id = election_result[0]
            else:
                day = 2 if turn == 1 else 30
                election_date = date(year, 10, day)
                election_id = db.execute(text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING RETURNING election_id"),
                                        {"date": election_date, "type": row["DS_ELEICAO"], "turn": turn}).scalar_one_or_none()
                db.commit()

            # 2. Processa Político
            politician_key = f'{row["NM_CANDIDATO"]}-{row["NM_URNA_CANDIDATO"]}'
            existing_politician = db.execute(text("SELECT politician_id FROM politicians WHERE full_name = :name AND nickname = :nick"), {"name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}).first()
            if existing_politician:
                politician_id = existing_politician[0]
            else:
                politician_id = db.execute(text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) ON CONFLICT (full_name, nickname) DO NOTHING RETURNING politician_id"),
                                            {"id": uuid.uuid4(), "name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}).scalar_one_or_none()
                if not politician_id: # Se o conflito ocorreu e nada foi retornado
                    politician_id = db.execute(text("SELECT politician_id FROM politicians WHERE full_name = :name AND nickname = :nick"), {"name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}).scalar()

            # 3. Processa Candidatura
            party_id = parties_cache.get(int(row["NR_PARTIDO"]))
            if party_id and election_id and politician_id:
                db.execute(text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number) VALUES (:p_id, :party_id, :e_id, :office, :num) ON CONFLICT DO NOTHING"),
                           {"p_id": politician_id, "party_id": party_id, "e_id": election_id, "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"])})
        db.commit()
        return len(chunk) # Retorna o número de itens processados
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def seed_candidacies(df, year):
    """Popula as tabelas de politicos e candidaturas usando processamento paralelo."""
    if df is None: return
    print(f"🚀 Iniciando a população de políticos e candidaturas com até {MAX_WORKERS} workers paralelos...")

    db = get_db_session()
    parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
    db.close()

    chunks = [df.iloc[i:i + len(df) // MAX_WORKERS] for i in range(0, len(df), len(df) // MAX_WORKERS)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # A barra de progresso geral agora rastreia o total de registros processados
        with tqdm(total=len(df), desc="Progresso Geral") as pbar:
            futures = [executor.submit(process_chunk, chunk, year, parties_cache) for chunk in chunks]

            for future in as_completed(futures):
                try:
                    processed_count = future.result()
                    pbar.update(processed_count) # Atualiza a barra com o número de itens do chunk
                except Exception as e:
                    print(f"❌ Um erro ocorreu em um dos workers: {e}")

    print(f"✅ Concluído! Processamento de {len(df)} candidaturas finalizado.")

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    # Parser para seed_parties
    parser_parties = subparsers.add_parser("seed_parties", help="Popula APENAS a tabela de partidos políticos.")
    parser_parties.add_argument("--year", type=int, help="Ano da eleição para buscar os dados. Se omitido, busca o mais recente.")
    parser_parties.add_argument("--force-download", action='store_true', help="Força um novo download do arquivo do TSE.")
    parser_parties.set_defaults(func=lambda args: seed_parties(get_election_data_as_dataframe(args.year if args.year else find_latest_election_year(), args.force_download)))

    # Parser para seed_candidacies
    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula as tabelas de políticos e candidaturas.")
    parser_candidacies.add_argument("--year", type=int, help="Ano da eleição para buscar os dados. Se omitido, busca o mais recente.")
    parser_candidacies.add_argument("--force-download", action='store_true', help="Força um novo download do arquivo do TSE.")
    parser_candidacies.set_defaults(func=lambda args: seed_candidacies(get_election_data_as_dataframe(args.year if args.year else find_latest_election_year(), args.force_download), args.year if args.year else find_latest_election_year()))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()