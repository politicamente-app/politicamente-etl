# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 20:38:26

import uuid
from datetime import date
from sqlalchemy import text
from tqdm import tqdm
from ..database import get_db_session
from ..config import logger, BATCH_SIZE

def seed_parties(df):
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
        logger.error(f"Erro ao popular a tabela de partidos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_politicians(df):
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
        logger.error(f"Erro ao popular a tabela de políticos: {e}")
        db.rollback()
    finally:
        db.close()

def seed_candidacies(df, year):
    if df is None: return
    print("🚀 Iniciando a população de eleições e candidaturas...")
    db = get_db_session()
    try:
        print("   Pré-carregando caches de dados...")
        parties_cache = {row.party_number: row.party_id for row in db.execute(text("SELECT party_id, party_number FROM parties")).all()}
        politicians_cache = {f'{p.full_name}-{p.nickname}': p.politician_id for p in db.execute(text("SELECT politician_id, full_name, nickname FROM politicians")).all()}

        elections_df = df[['ANO_ELEICAO', 'NR_TURNO', 'DS_ELEICAO']].drop_duplicates()
        for _, row in tqdm(elections_df.iterrows(), total=len(elections_df), desc="Criando Eleições"):
            turn, ano = int(row['NR_TURNO']), int(row['ANO_ELEICAO'])
            election_date = date(ano, 10, 2 if turn == 1 else 30)
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
        logger.error(f"Erro durante o seeding de candidaturas: {e}")
    finally:
        db.close()

def update_results(df):
    if df is None: return
    print("🚀 Iniciando a atualização dos resultados das candidaturas...")

    results_df = df.groupby('SQ_CANDIDATO').agg(
        QT_VOTOS=('QT_VOTOS', 'sum'),
        DS_SIT_TOT_TURNO=('DS_SIT_TOT_TURNO', 'first')
    ).reset_index()

    db = get_db_session()
    try:
        updates = [{"sq_tse": str(row["SQ_CANDIDATO"]), "total_votes": int(row["QT_VOTOS"]), "status": row["DS_SIT_TOT_TURNO"]} for _, row in results_df.iterrows()]

        updated_count = 0
        with tqdm(total=len(updates), desc="Atualizando Resultados no DB") as pbar:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                with db.begin():
                    for item in batch:
                        result = db.execute(
                            text("UPDATE candidacies SET total_votes_received = :total_votes, status_resultado = :status WHERE sq_candidate_tse = :sq_tse"),
                            item
                        )
                        if result.rowcount > 0: updated_count += 1
                        else: logger.warning(f"Nenhuma candidatura encontrada para o SQ_CANDIDATO: {item['sq_tse']}.")
                pbar.update(len(batch))

        print(f"✅ Atualização de resultados concluída. {updated_count} registros atualizados.")
    except Exception as e:
        logger.error(f"Erro ao atualizar os resultados: {e}")
        db.rollback()
    finally:
        db.close()