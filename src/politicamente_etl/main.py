# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-30 11:21:33

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
BATCH_SIZE = 1000 # Número de registros por lote de inserção

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
    """Popula a tabela de partidos a partir de um DataFrame em lotes."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de partidos...")
    parties_df = df[['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO']].drop_duplicates(subset=['NR_PARTIDO'])

    db = get_db_session()
    try:
        parties_to_upsert = [
            {"num": int(row["NR_PARTIDO"]), "init": row["SG_PARTIDO"], "name": row["NM_PARTIDO"]}
            for _, row in parties_df.iterrows()
        ]

        if not parties_to_upsert:
            print("Nenhum partido para inserir/atualizar.")
            return

        with tqdm(total=len(parties_to_upsert), desc="Processando Partidos") as pbar:
            for i in range(0, len(parties_to_upsert), BATCH_SIZE):
                batch = parties_to_upsert[i:i + BATCH_SIZE]
                db.execute(
                    text("INSERT INTO parties (party_number, initials, party_name) VALUES (:num, :init, :name) ON CONFLICT (party_number) DO UPDATE SET initials = :init, party_name = :name"),
                    batch
                )
                db.commit()
                pbar.update(len(batch))

        print(f"✅ População de partidos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_politicians(df):
    """Popula a tabela de políticos a partir de um DataFrame em lotes."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de políticos...")
    politicians_df = df[['NM_CANDIDATO', 'NM_URNA_CANDIDATO']].drop_duplicates()

    db = get_db_session()
    try:
        politicians_to_insert = [
            {"id": uuid.uuid4(), "name": row["NM_CANDIDATO"], "nick": row["NM_URNA_CANDIDATO"]}
            for _, row in politicians_df.iterrows()
        ]

        if not politicians_to_insert:
            print("Nenhum novo político para inserir.")
            return

        print(f"Inserindo {len(politicians_to_insert)} políticos únicos em lotes de {BATCH_SIZE}...")

        with tqdm(total=len(politicians_to_insert), desc="Inserindo Políticos") as pbar:
            for i in range(0, len(politicians_to_insert), BATCH_SIZE):
                batch = politicians_to_insert[i:i + BATCH_SIZE]
                db.execute(
                    text("INSERT INTO politicians (politician_id, full_name, nickname) VALUES (:id, :name, :nick) ON CONFLICT (full_name, nickname) DO NOTHING"),
                    batch
                )
                db.commit()
                pbar.update(len(batch))

        print("✅ População de políticos concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de políticos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_coalitions(df, year):
    """Popula as tabelas de coligações e suas associações com partidos."""
    if df is None: return
    print("🚀 Iniciando a população da tabela de coligações...")

    # Filtra apenas as linhas que são de coligação
    coalitions_df = df[df['TP_AGREMIACAO'] == 'COLIGAÇÃO'][['NM_COLIGACAO', 'DS_COMPOSICAO_COLIGACAO']].drop_duplicates()

    db = get_db_session()
    try:
        print("   Pré-carregando caches de Partidos e Eleições...")
        parties_cache = {row.initials: row.party_id for row in db.execute(text("SELECT party_id, initials FROM parties")).all()}
        elections_cache = {f"{int(e.date_part)}-{e.turn}-{e.election_type}": e.election_id for e in db.execute(text("SELECT election_id, date_part('year', election_date) as date_part, turn, election_type FROM elections")).all()}

        # Assume uma única eleição principal para simplificar
        # Em um sistema real, a coligação deveria estar ligada a uma eleição específica
        election_id = next(iter(elections_cache.values()), None)
        if not election_id:
            print("❌ Nenhuma eleição encontrada no banco. Cancele o seeding de coligações.")
            return

        for _, row in tqdm(coalitions_df.iterrows(), total=len(coalitions_df), desc="Processando Coligações"):
            coalition_name = row['NM_COLIGACAO']
            composition = row['DS_COMPOSICAO_COLIGACAO']

            # Insere a coligação e obtém o ID
            result = db.execute(
                text("INSERT INTO coligacoes (nome_coligacao, id_eleicao_fk) VALUES (:name, :e_id) ON CONFLICT DO NOTHING RETURNING coligacao_id"),
                {"name": coalition_name, "e_id": election_id}
            ).scalar_one_or_none()
            db.commit()

            # Se a coligação já existia, busca o ID dela
            if not result:
                result = db.execute(text("SELECT coligacao_id FROM coligacoes WHERE nome_coligacao = :name AND id_eleicao_fk = :e_id"),
                                    {"name": coalition_name, "e_id": election_id}).scalar_one()

            coalition_id = result

            # Processa os partidos da composição
            party_initials = [p.strip() for p in composition.split('/')]
            for initial in party_initials:
                party_id = parties_cache.get(initial)
                if party_id and coalition_id:
                    db.execute(
                        text("INSERT INTO coligacao_partidos (id_coligacao_fk, id_partido_fk) VALUES (:c_id, :p_id) ON CONFLICT DO NOTHING"),
                        {"c_id": coalition_id, "p_id": party_id}
                    )

        db.commit()
        print("✅ População de coligações concluída.")
    except Exception as e:
        print(f"❌ Erro ao popular a tabela de coligações: {e}")
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
    parser_parties.set_defaults(func=lambda args: seed_parties(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula APENAS a tabela de políticos.", parents=[base_parser])
    parser_politicians.set_defaults(func=lambda args: seed_politicians(get_election_data_as_dataframe(args.year, args.force_download)))

    parser_coalitions = subparsers.add_parser("seed_coalitions", help="Popula a tabela de coligações.", parents=[base_parser])
    parser_coalitions.set_defaults(func=lambda args: seed_coalitions(get_election_data_as_dataframe(args.year, args.force_download), args.year))

    # ... outros subparsers ...

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()