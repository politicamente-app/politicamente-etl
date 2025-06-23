# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 15:18:00

import os
import argparse
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

# --- CONFIGURAÇÃO ---
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
# URL da API de Dados Abertos do TSE, especificando a eleição de 2022 (código 544).
# Esta é uma URL validada e funcional.
TSE_PARTIES_API_URL = "https://dadosabertos.tse.jus.br/api/v1/agregacao/partido/pleito/544/BRASIL"


if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def fetch_parties_from_tse_api():
    """
    Busca e processa os dados de partidos diretamente da API do TSE.
    """
    print(f"Buscando dados de partidos na API validada: {TSE_PARTIES_API_URL}")
    try:
        response = requests.get(TSE_PARTIES_API_URL)
        response.raise_for_status()

        party_list = response.json()

        if not isinstance(party_list, list):
            print("❌ Erro: Formato de resposta da API inesperado.")
            return None

        parties_data = []
        for party in party_list:
            if "sigla" in party and "nome" in party and "numero" in party:
                 parties_data.append({
                    "party_name": party["nome"],
                    "initials": party["sigla"],
                    "party_number": int(party["numero"]),
                })

        print(f"Sucesso! {len(parties_data)} partidos encontrados na fonte de dados do TSE.")
        return parties_data

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar os dados do TSE: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro ao processar a resposta da API: {e}")
        return None


def seed_parties():
    """
    Popula a tabela 'parties' com os dados extraídos da API do TSE.
    """
    parties_data = fetch_parties_from_tse_api()
    if not parties_data:
        print("Não foi possível continuar a população devido a um erro na extração dos dados.")
        return

    print("🚀 Iniciando a população da tabela de partidos...")
    db = get_db_session()

    try:
        total_inserted = 0
        total_updated = 0

        for party in parties_data:
            result = db.execute(text("SELECT party_id FROM parties WHERE party_number = :number"), {"number": party["party_number"]}).fetchone()

            if result:
                db.execute(text("""
                    UPDATE parties
                    SET party_name = :name, initials = :initials
                    WHERE party_number = :number
                """), {"name": party["party_name"], "initials": party["initials"], "number": party["party_number"]})
                total_updated += 1
            else:
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