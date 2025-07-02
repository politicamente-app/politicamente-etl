# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 15:31:32

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
        print(f"Baixando dados de: {zip_url}")
        try:
            response = requests.get(zip_url, stream=True)
            response.raise_for_status()
            with open(zip_filepath, 'wb') as f:
                total_size = int(response.headers.get('content-length', 0))
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=zip_filename) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        pbar.update(len(chunk))
                        f.write(chunk)
            print(f"Download concluído. Arquivo salvo em: {zip_filepath}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao baixar o arquivo ZIP: {e}")
            return
    else:
        print(f"Usando arquivo ZIP local já baixado: {zip_filepath}")

    try:
        with zipfile.ZipFile(zip_filepath) as z:
            consolidated_file = f"{file_prefix}_{year}_BRASIL.csv"
            if consolidated_file in z.namelist():
                print(f"Encontrado arquivo consolidado: {consolidated_file}")
                with z.open(consolidated_file) as csv_file:
                    yield pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
                return

            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError("Nenhum arquivo CSV encontrado no ZIP.")

            for csv_filename in csv_files:
                with z.open(csv_filename) as csv_file:
                    yield pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo: {e}")
        return

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
    if df is None: return
    print("🚀 Iniciando a população de eleições e candidaturas...")

    db = get_db_session()
    try:
        print("   Pré-carregando caches de dados...")
        parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
        politicians_cache = {f'{p.full_name}-{p.nickname}': p.politician_id for p in db.execute(text("SELECT politician_id, full_name, nickname FROM politicians")).all()}

        elections_df = df[['ANO_ELEICAO', 'NR_TURNO', 'DS_ELEICAO']].drop_duplicates()
        for _, row in tqdm(elections_df.iterrows(), total=len(elections_df), desc="Criando Eleições"):
            turn = int(row['NR_TURNO'])
            day = 2 if turn == 1 else 30
            election_date = date(int(row['ANO_ELEICAO']), 10, day)
            db.execute(text("INSERT INTO elections (election_date, election_type, turn) VALUES (:date, :type, :turn) ON CONFLICT DO NOTHING"),
                       {"date": election_date, "type": row["DS_ELEICAO"], "turn": turn})
        db.commit()
        elections_cache = {f"{int(e.date_part)}-{e.turn}-{e.election_type}": e.election_id for e in db.execute(text("SELECT election_id, date_part('year', election_date) as date_part, turn, election_type FROM elections")).all()}

        candidacies_to_insert = []
        for _, row in df.iterrows():
            election_key = f"{row['ANO_ELEICAO']}-{row['NR_TURNO']}-{row['DS_ELEICAO']}"
            politician_key = f'{row["NM_CANDIDATO"]}-{row["NM_URNA_CANDIDATO"]}'

            election_id = elections_cache.get(election_key)
            politician_id = politicians_cache.get(politician_key)
            party_id = parties_cache.get(int(row["NR_PARTIDO"]))

            if party_id and election_id and politician_id:
                candidacies_to_insert.append({
                    "p_id": politician_id, "party_id": party_id, "e_id": election_id,
                    "office": row["DS_CARGO"], "num": int(row["NR_CANDIDATO"]),
                    "sq_tse": str(row["SQ_CANDIDATO"])
                })

        with tqdm(total=len(candidacies_to_insert), desc="Inserindo Candidaturas") as pbar:
            for i in range(0, len(candidacies_to_insert), BATCH_SIZE):
                batch = candidacies_to_insert[i:i + BATCH_SIZE]
                db.execute(
                    text("INSERT INTO candidacies (politician_id, party_id, election_id, office, electoral_number, sq_candidate_tse) VALUES (:p_id, :party_id, :e_id, :office, :num, :sq_tse) ON CONFLICT DO NOTHING"),
                    batch
                )
                db.commit()
                pbar.update(len(batch))

        print(f"✅ Concluído! Processamento de candidaturas finalizado.")
    except Exception as e:
        print(f"❌ Erro durante o seeding de candidaturas: {e}")
    finally:
        db.close()

def update_results(df_generator):
    """Atualiza a tabela de candidaturas com os resultados da votação."""
    if df_generator is None: return
    print("🚀 Iniciando a atualização dos resultados das candidaturas...")

    aggregated_results = {}

    for df in df_generator:
        # CORREÇÃO: Usar a coluna correta para o ID do candidato no arquivo de votação
        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Agregando votos de {df['SG_UF'].iloc[0]}", leave=False):
            key = str(row['SQ_CANDIDATO'])
            if key not in aggregated_results:
                aggregated_results[key] = {
                    "total_votes": 0,
                    "status": row['DS_SIT_TOT_TURNO']
                }
            aggregated_results[key]["total_votes"] += int(row['QT_VOTOS'])

    print(f"Agregação concluída. {len(aggregated_results)} resultados únicos de candidatos para atualizar.")

    db = get_db_session()
    try:
        updates = [{"sq_tse": key, "total_votes": value["total_votes"], "status": value["status"]} for key, value in aggregated_results.items()]

        with tqdm(total=len(updates), desc="Atualizando Resultados no DB") as pbar:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                with db.begin():
                    for item in batch:
                        # CORREÇÃO: A query de UPDATE agora usa a chave correta e única
                        db.execute(
                            text("UPDATE candidacies SET total_votes_received = :total_votes, status_resultado = :status WHERE sq_candidate_tse = :sq_tse"),
                            item
                        )
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

    parser_all = subparsers.add_parser("seed_all", help="Executa todos os seeds em ordem.", parents=[base_parser])
    parser_all.set_defaults(func=lambda args: seed_all(args.year, args.force_download))

    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos.", parents=[base_parser])
    parser_parties.set_defaults(func=lambda args: seed_parties(pd.concat(list(get_tse_data_generator(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula a tabela de políticos.", parents=[base_parser])
    parser_politicians.set_defaults(func=lambda args: seed_politicians(pd.concat(list(get_tse_data_generator(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))))

    parser_coalitions = subparsers.add_parser("seed_coalitions", help="Popula a tabela de coligações.", parents=[base_parser])
    parser_coalitions.set_defaults(func=lambda args: seed_coalitions(pd.concat(list(get_tse_data_generator(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download))), args.year))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula a tabela de candidaturas.", parents=[base_parser])
    parser_candidacies.set_defaults(func=lambda args: seed_candidacies(pd.concat(list(get_tse_data_generator(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download))), args.year))

    parser_results = subparsers.add_parser("update_results", help="Atualiza os resultados de votação.", parents=[base_parser])
    parser_results.set_defaults(func=lambda args: update_results(get_tse_data_generator(args.year, TSE_VOTES_BASE_URL, "votacao_candidato_munzona", args.force_download)))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()