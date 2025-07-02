# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 20:38:26

import argparse
from datetime import date
from .config import logger, SCRIPT_VERSION, TSE_CAND_BASE_URL, TSE_VOTES_BASE_URL
from .etl.extract import get_tse_data_as_dataframe
from .etl.load import seed_parties, seed_politicians, seed_candidacies, update_results

def main():
    """Função principal para analisar os argumentos e chamar a tarefa correta."""
    logger.info(f"PoliticaMente ETL Script v{SCRIPT_VERSION} iniciado.")
    parser = argparse.ArgumentParser(description="Script de ETL para popular o banco de dados do PoliticaMente.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comando a ser executado")

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument("--year", type=int, default=date.today().year, help="Ano da eleição.")
    base_parser.add_argument("--force-download", action='store_true')

    def run_seed(func, base_url, prefix, args):
        df = get_tse_data_as_dataframe(args.year, base_url, prefix, args.force_download)
        if df is not None:
            func(df)

    def run_seed_with_year(func, base_url, prefix, args):
        df = get_tse_data_as_dataframe(args.year, base_url, prefix, args.force_download)
        if df is not None:
            func(df, args.year)

    parser_parties = subparsers.add_parser("seed_parties", help="Popula a tabela de partidos.", parents=[base_parser])
    parser_parties.set_defaults(func=lambda args: run_seed(seed_parties, TSE_CAND_BASE_URL, "consulta_cand", args))

    parser_politicians = subparsers.add_parser("seed_politicians", help="Popula a tabela de políticos.", parents=[base_parser])
    parser_politicians.set_defaults(func=lambda args: run_seed(seed_politicians, TSE_CAND_BASE_URL, "consulta_cand", args))

    parser_candidacies = subparsers.add_parser("seed_candidacies", help="Popula a tabela de candidaturas.", parents=[base_parser])
    parser_candidacies.set_defaults(func=lambda args: run_seed_with_year(seed_candidacies, TSE_CAND_BASE_URL, "consulta_cand", args))

    parser_results = subparsers.add_parser("update_results", help="Atualiza os resultados de votação.", parents=[base_parser])
    parser_results.set_defaults(func=lambda args: run_seed(update_results, TSE_VOTES_BASE_URL, "votacao_candidato_munzona", args))

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()