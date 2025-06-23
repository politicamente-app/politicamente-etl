# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 16:11:00

import os
import argparse
import csv
import io
import zipfile
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

# --- CONFIGURAÇÃO ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TSE_DATA_BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
SEARCH_LIMIT_YEARS = 10 # Limite de anos para procurar para trás

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def find_latest_election_year():
    """
    Descobre o ano da eleição mais recente com dados disponíveis no TSE,
    com um limite de busca.
    """
    print("🔎 Procurando o ano da eleição mais recente...")
    current_year = date.today().year

    # Começa pelo ano par mais recente
    start_year = current_year if current_year % 2 == 0 else current_year - 1

    for year in range(start_year, start_year - SEARCH_LIMIT_YEARS, -2):
        zip_url = f"{TSE_DATA_BASE_URL}/consulta_cand_{year}.zip"
        try:
            print(f"   Testando ano {year}...")
            response = requests.head(zip_url, timeout=5)
            if response.status_code == 200:
                print(f"✅ Ano da eleição encontrado: {year}")
                if year != start_year:
                    print(f"⚠️  Atenção: Os dados mais recentes encontrados são de {year}, que não é o ano corrente da eleição ({start_year}). Os dados podem estar defasados.")
                return year
        except requests.exceptions.RequestException:
            continue

    print(f"❌ Não foi possível encontrar um ano de eleição válido nos últimos {SEARCH_LIMIT_YEARS} anos.")
    return None

def fetch_and_extract_csv(year):
    """
    Baixa o arquivo ZIP de um ano específico, descompacta em memória
    e retorna o conteúdo do arquivo CSV principal.
    """
    zip_url = f"{TSE_DATA_BASE_URL}/consulta_cand_{year}.zip"
    target_csv_filename = f"consulta_cand_{year}_BRASIL.csv"

    print(f"Baixando dados de: {zip_url}")
    try:
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            if target_csv_filename not in z.namelist():
                raise FileNotFoundError(f"Arquivo '{target_csv_filename}' não encontrado no ZIP. Verifique o ano ou a estrutura do arquivo do TSE.")

            with z.open(target_csv_filename) as csv_file:
                return io.StringIO(csv_file.read().decode('latin-1'))

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao baixar o arquivo ZIP: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo ZIP: {e}")
        return None

def seed_election_data(year):
    """
    Processa o arquivo CSV de um ano e popula as tabelas
    de eleições, partidos, políticos e candidaturas.
    """
    if year is None:
        year = find_latest_election_year()
        if not year:
            return

    csv_file_in_memory = fetch_and_extract_csv(year)
    if not csv_file_in_memory:
        return

    print(f"🚀 Iniciando a população do banco para a eleição de {year}...")
    db = get_db_session()

    try:
        elections_cache, parties_cache, politicians_cache = {}, {}, {}
        reader = csv.DictReader(csv_file_in_memory, delimiter=';')

        count = 0
        for row in reader:
            election_key = f"{year}-{row['NR_TURNO']}"
            if election_key not in elections_cache:
                election_date = date(year, 10, int(row["NR_TURNO"][0]))
                db.execute(
                    text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING"),
                    {"date": election_date, "type": row["DS_ELEICAO"], "turn": int(row["NR_TURNO"])}
                )
                db.commit()
                election_id = db.execute(text("SELECT election_id FROM elections WHERE turn = :turn AND date_part('year', election_date) = :year"), {"turn": int(row["NR_TURNO"]), "year": year}).scalar_one()
                elections_cache[election_key] = election_id
            election_id = elections_cache[election_key]

            party_number = int(row["NR_PARTIDO"])
            if party_number not in parties_cache:
                db.execute(
                    text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"),
                    {"num": party_number, "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]}
                )
                db.commit()
                party_id = db.execute(text("SELECT party_id FROM parties WHERE party_number = :num"), {"num": party_number}).scalar_one()
                parties_cache[party_number] = party_id
            party_id = parties_cache[party_number]

            politician_key = f'{row["NM_CANDIDATO"]}-{row["DT_NASCIMENTO"]}'
            if politician_key not in politicians_cache:
                politician_id = db.execute(
                    text("INSERT INTO politicians (full_name, nickname) VALUES (:name, :nick) RETURNING politician_id"),
                    {"name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}
                ).scalar_one()
                politicians_cache[politician_key] = politician_id
            politician_id = politicians_cache[politician_key]

            db.execute(
                text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number) VALUES (:p_id, :party_id, :e_id, :office, :num) ON CONFLICT DO NOTHING"),
                {"p_id": politician_id, "party_id": party_id, "e_id": election_id, "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"])}
            )

            count += 1
            if count % 1000 == 0:
                db.commit()
                print(f"   ... {count} candidaturas processadas.")

        db.commit()
        print(f"✅ Concluído! Total de {count} candidaturas processadas.")
        print(f"ℹ️  Fonte de dados utilizada: Eleição de {year}.")

    except Exception as e:
        print(f"❌ Erro durante o seeding: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    parser_seed = subparsers.add_parser("seed_election_data", help="Popula o banco com dados de uma eleição do TSE.")
    parser_seed.add_argument("--year", type=int, required=False, help="O ano da eleição a ser processada (ex: 2022). Se não for fornecido, busca o mais recente.")
    parser_seed.set_defaults(func=lambda args: seed_election_data(args.year))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()