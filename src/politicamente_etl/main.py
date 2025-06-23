# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 13:52:56

import os
import argparse
import csv
import io
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

# --- CONFIGURAÇÃO ---
# Carrega as variáveis do arquivo .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
# URL oficial do TSE para os dados de partidos políticos.
# Pode mudar no futuro, é importante monitorar.
TSE_PARTIES_DATA_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/partido_eleicao/partidos.csv"


if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def fetch_parties_from_tse():
    """
    Busca e processa os dados de partidos diretamente do CSV do TSE.
    """
    print(f"Buscando dados de partidos em: {TSE_PARTIES_DATA_URL}")
    try:
        response = requests.get(TSE_PARTIES_DATA_URL)
        response.raise_for_status()  # Lança um erro se a requisição falhar (ex: 404)

        # O CSV do TSE usa a codificação 'latin-1'
        response.encoding = 'latin-1'

        parties_data = []
        # Usa io.StringIO para tratar a string de texto como um arquivo
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file, delimiter=';')

        for row in reader:
            parties_data.append({
                "party_name": row["NM_PARTIDO"],
                "initials": row["SG_PARTIDO"],
                "party_number": int(row["NR_PARTIDO"]),
            })

        print(f"Sucesso! {len(parties_data)} partidos encontrados na fonte de dados do TSE.")
        return parties_data

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar os dados do TSE: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro ao processar o arquivo CSV: {e}")
        return None


def seed_parties():
    """
    Popula a tabela 'parties' com os dados extraídos do TSE.
    """
    parties_data = fetch_parties_from_tse()
    if not parties_data:
        print("Não foi possível continuar a população devido a um erro na extração dos dados.")
        return

    print("🚀 Iniciando a população da tabela de partidos...")
    db = get_db_session()

    try:
        total_inserted = 0
        total_updated = 0

        for party in parties_data:
            # Verifica se o partido já existe pelo número
            result = db.execute(text("SELECT party_id FROM parties WHERE party_number = :number"), {"number": party["party_number"]}).fetchone()

            if result:
                # Se existe, atualiza
                db.execute(text("""
                    UPDATE parties
                    SET party_name = :name, initials = :initials
                    WHERE party_number = :number
                """), {"name": party["party_name"], "initials": party["initials"], "number": party["party_number"]})
                total_updated += 1
            else:
                # Se não existe, insere
                db.execute(text("""
                    INSERT INTO parties (party_name, initials, party_number)
                    VALUES (:name, :initials, :number)
                """), {"name": party["party_name"], "initials": party["initials"], "number": party["party_number"]})
                total_inserted += 1

        db.commit()
        print(f"✅ Concluído! {total_inserted} partidos inseridos, {total_updated} partidos atualizados.")

    except Exception as e:
        print(f"❌ Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos políticos a partir dos dados do TSE.")
    parser_parties.set_defaults(func=seed_parties)

    args = parser.parse_args()
    args.func()


if __name__ == "__main__":
    main()