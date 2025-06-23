# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 16:22:07

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

# --- CONFIGURAÇÃO ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
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
    """Popula a tabela de partidos a partir de um DataFrame."""
    print("🚀 Iniciando a população da tabela de partidos...")
    parties_df = df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        for _, row in parties_df.iterrows():
            db.execute(
                text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"),
                {"num": int(row["NR_PARTIDO"]), "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]}
            )
        db.commit()
        print(f"✅ Concluído! {len(parties_df)} partidos inseridos/atualizados.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_politicians_and_candidacies(df, year):
    """Popula as tabelas de politicos e candidaturas a partir de um DataFrame."""
    print("🚀 Iniciando a população de políticos e candidaturas...")
    db = get_db_session()
    try:
        # Cache para evitar buscas repetidas
        parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
        elections_cache = {}

        for _, row in df.iterrows():
            # Processa eleição
            election_key = f"{year}-{row['NR_TURNO']}"
            if election_key not in elections_cache:
                election_date = date(year, 10, int(row["NR_TURNO"][0]))
                db.execute(text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING"),
                           {"date": election_date, "type": row["DS_ELEICAO"], "turn": int(row["NR_TURNO"])})
                db.commit()
                election_id = db.execute(text("SELECT election_id FROM elections WHERE turn = :turn AND date_part('year', election_date) = :year"), {"turn": int(row["NR_TURNO"]), "year": year}).scalar_one()
                elections_cache[election_key] = election_id
            election_id = elections_cache[election_key]

            # Processa político (com geração de UUID no script)
            new_politician_id = uuid.uuid4()
            politician_id = db.execute(
                text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) ON CONFLICT (full_name, nickname) DO NOTHING RETURNING politician_id"),
                {"id": new_politician_id, "name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}
            ).scalar_one_or_none()

            # Se o politico ja existia, busca o ID
            if not politician_id:
                 politician_id = db.execute(text("SELECT politician_id FROM politicians WHERE full_name = :name AND nickname = :nick"), {"name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}).scalar_one()

            # Processa candidatura
            party_id = parties_cache.get(int(row["NR_PARTIDO"]))
            if party_id:
                db.execute(
                    text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number) VALUES (:p_id, :party_id, :e_id, :office, :num) ON CONFLICT DO NOTHING"),
                    {"p_id": politician_id, "party_id": party_id, "e_id": election_id, "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"])}
                )

        db.commit()
        print(f"✅ Concluído! {len(df)} candidaturas processadas.")
    except Exception as e:
        print(f"❌ Erro durante o seeding: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    # Argumentos globais
    parser.add_argument("--year", type=int, default=2022, help="O ano da eleição a ser processada (ex: 2022).")
    parser.add_argument("--force-download", action='store_true', help="Força o download de um novo arquivo ZIP, mesmo que um já exista localmente.")

    # Subcomandos para cada tarefa
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    parser_parties = subparsers.add_parser("seed_parties", help="Popula APENAS a tabela de partidos políticos.")
    parser_parties.set_defaults(func=lambda args: seed_parties(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula as tabelas de políticos e candidaturas.")
    parser_candidacies.set_defaults(func=lambda args: seed_politicians_and_candidacies(get_election_data_as_dataframe(args.year, args.force_download), args.year))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()