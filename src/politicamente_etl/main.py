# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 03:02:15

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
            return None
    else:
        print(f"Usando arquivo ZIP local já baixado: {zip_filepath}")

    try:
        print("Processando arquivo(s) CSV...")
        all_dfs = []
        with zipfile.ZipFile(zip_filepath) as z:
            # Lista todos os arquivos CSV dentro do ZIP
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError("Nenhum arquivo CSV encontrado no ZIP.")

            for csv_filename in tqdm(csv_files, desc="Lendo arquivos CSV do ZIP"):
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
                    all_dfs.append(df)

        # Concatena todos os DataFrames em um só
        full_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Sucesso! {len(full_df)} registros lidos de {len(csv_files)} arquivo(s) CSV.")
        return full_df
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo: {e}")
        return None

# ... (as funções seed_parties, seed_politicians, seed_coalitions e seed_candidacies permanecem as mesmas) ...

def update_results(df):
    """Atualiza a tabela de candidaturas com os resultados da votação."""
    if df is None: return
    print("🚀 Iniciando a atualização dos resultados das candidaturas...")

    # Agrupa os votos por candidato para obter o total
    # Usamos SQ_CANDIDATO para ligar ao NR_CANDIDATO e ANO_ELEICAO
    results_df = df.groupby(['ANO_ELEICAO', 'NR_CANDIDATO', 'DS_CARGO'])['QT_VOTOS_NOMINAIS'].sum().reset_index()
    status_df = df[['ANO_ELEICAO', 'NR_CANDIDATO', 'DS_CARGO', 'DS_SIT_TOT_TURNO']].drop_duplicates()

    # Junta os totais com o status
    final_results = pd.merge(results_df, status_df, on=['ANO_ELEICAO', 'NR_CANDIDATO', 'DS_CARGO'])

    db = get_db_session()
    try:
        updates = []
        for _, row in tqdm(final_results.iterrows(), total=len(final_results), desc="Preparando Atualizações"):
            updates.append({
                "year": int(row["ANO_ELEICAO"]),
                "office": row["DS_CARGO"],
                "electoral_number": int(row["NR_CANDIDATO"]),
                "total_votes": int(row["QT_VOTOS_NOMINAIS"]),
                "status": row["DS_SIT_TOT_TURNO"]
            })

        print(f"Atualizando {len(updates)} candidaturas...")
        with tqdm(total=len(updates), desc="Atualizando Resultados") as pbar:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                for item in batch:
                    # Atualiza usando o número do candidato, cargo e ano da eleição como chave
                    db.execute(
                        text("""
                            UPDATE candidacies SET
                                total_votos_recebidos = :total_votes,
                                status_resultado = :status
                            WHERE electoral_number = :electoral_number
                            AND office = :office
                            AND election_id IN (SELECT election_id FROM elections WHERE date_part('year', election_date) = :year)
                        """),
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

    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos políticos.", parents=[base_parser])
    parser_parties.set_defaults(func=lambda args: seed_parties(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula a tabela de políticos.", parents=[base_parser])
    parser_politicians.set_defaults(func=lambda args: seed_politicians(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download)))

    parser_coalitions = subparsers.add_parser("seed_coalitions", help="Popula a tabela de coligações.", parents=[base_parser])
    parser_coalitions.set_defaults(func=lambda args: seed_coalitions(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download), args.year))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula a tabela de candidaturas.", parents=[base_parser])
    parser_candidacies.set_defaults(func=lambda args: seed_candidacies(get_tse_data(args.year, TSE_CAND_BASE_URL, "consulta_cand", args.force_download), args.year))

    parser_results = subparsers.add_parser("update_results", help="Atualiza os resultados de votação das candidaturas.", parents=[base_parser])
    parser_results.set_defaults(func=lambda args: update_results(get_tse_data(args.year, TSE_VOTES_BASE_URL, "votacao_candidato_munzona", args.force_download)))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()