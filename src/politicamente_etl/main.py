# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 13:34:25

import os
import argparse
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# --- CONFIGURAÇÃO ---
# Carrega as variáveis do arquivo .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

# --- DADOS ESTÁTICOS ---
# Fonte: TSE em Junho de 2025.
# No futuro, podemos automatizar a extração destes dados.
PARTIES_DATA = [
    {"party_name": "MOVIMENTO DEMOCRÁTICO BRASILEIRO", "initials": "MDB", "party_number": 15},
    {"party_name": "PARTIDO DEMOCRÁTICO TRABALHISTA", "initials": "PDT", "party_number": 12},
    {"party_name": "PARTIDO DOS TRABALHADORES", "initials": "PT", "party_number": 13},
    {"party_name": "PARTIDO COMUNISTA DO BRASIL", "initials": "PCdoB", "party_number": 65},
    {"party_name": "PARTIDO SOCIALISTA BRASILEIRO", "initials": "PSB", "party_number": 40},
    {"party_name": "PARTIDO DA SOCIAL DEMOCRACIA BRASILEIRA", "initials": "PSDB", "party_number": 45},
    {"party_name": "AGIR", "initials": "AGIR", "party_number": 36},
    {"party_name": "MOBILIZAÇÃO NACIONAL", "initials": "MOBILIZA", "party_number": 33},
    {"party_name": "CIDADANIA", "initials": "CIDADANIA", "party_number": 23},
    {"party_name": "PARTIDO VERDE", "initials": "PV", "party_number": 43},
    {"party_name": "AVANTE", "initials": "AVANTE", "party_number": 70},
    {"party_name": "PROGRESSISTAS", "initials": "PP", "party_number": 11},
    {"party_name": "PARTIDO SOCIALISTA DOS TRABALHADORES UNIFICADO", "initials": "PSTU", "party_number": 16},
    {"party_name": "PARTIDO COMUNISTA BRASILEIRO", "initials": "PCB", "party_number": 21},
    {"party_name": "PARTIDO RENOVADOR TRABALHISTA BRASILEIRO", "initials": "PRTB", "party_number": 28},
    {"party_name": "DEMOCRACIA CRISTÃ", "initials": "DC", "party_number": 27},
    {"party_name": "PARTIDO DA CAUSA OPERÁRIA", "initials": "PCO", "party_number": 29},
    {"party_name": "PODEMOS", "initials": "PODE", "party_number": 20},
    {"party_name": "REPUBLICANOS", "initials": "REPUBLICANOS", "party_number": 10},
    {"party_name": "PARTIDO SOCIALISMO E LIBERDADE", "initials": "PSOL", "party_number": 50},
    {"party_name": "PARTIDO LIBERAL", "initials": "PL", "party_number": 22},
    {"party_name": "PARTIDO SOCIAL DEMOCRÁTICO", "initials": "PSD", "party_number": 55},
    {"party_name": "SOLIDARIEDADE", "initials": "SOLIDARIEDADE", "party_number": 77},
    {"party_name": "PARTIDO NOVO", "initials": "NOVO", "party_number": 30},
    {"party_name": "REDE SUSTENTABILIDADE", "initials": "REDE", "party_number": 18},
    {"party_name": "PARTIDO DA MULHER BRASILEIRA", "initials": "PMB", "party_number": 35},
    {"party_name": "UNIDADE POPULAR", "initials": "UP", "party_number": 80},
    {"party_name": "UNIÃO BRASIL", "initials": "UNIÃO", "party_number": 44},
    {"party_name": "PARTIDO RENOVAÇÃO DEMOCRÁTICA", "initials": "PRD", "party_number": 25},
]


def get_db_session():
    """Cria e retorna uma sessão de banco de dados."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()

def seed_parties():
    """
    Popula a tabela 'parties' com os dados dos partidos políticos.
    Usa uma abordagem "UPSERT" para inserir novos partidos ou atualizar os existentes.
    """
    print("🚀 Iniciando a população da tabela de partidos...")
    db = get_db_session()

    try:
        total_inserted = 0
        total_updated = 0

        for party in PARTIES_DATA:
            # Verifica se o partido já existe pelo número
            result = db.execute(text("SELECT party_id FROM parties WHERE party_number = :number"), {"number": party["party_number"]}).fetchone()

            if result:
                # Se existe, atualiza (opcional, mas bom para manter os dados consistentes)
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

    # Define os subcomandos para cada tarefa de seeding
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    # Comando para popular os partidos
    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos políticos.")
    parser_parties.set_defaults(func=seed_parties)

    args = parser.parse_args()
    args.func()


if __name__ == "__main__":
    main()