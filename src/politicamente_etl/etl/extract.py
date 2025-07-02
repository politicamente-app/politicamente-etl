# Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 20:38:26

import os
import io
import zipfile
import requests
import pandas as pd
from tqdm import tqdm
from ..config import logger, DATA_DIR

def get_tse_data_as_dataframe(year, base_url, file_prefix, force_download=False):
    """
    Função genérica para baixar e extrair dados do TSE, retornando um DataFrame consolidado.
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
            logger.error(f"Erro ao baixar o arquivo ZIP: {e}")
            return None
    else:
        print(f"Usando arquivo ZIP local já baixado: {zip_filepath}")

    try:
        with zipfile.ZipFile(zip_filepath) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files: raise FileNotFoundError("Nenhum arquivo CSV encontrado no ZIP.")

            consolidated_file = f"{file_prefix}_{year}_BRASIL.csv"
            csv_to_read = [consolidated_file] if consolidated_file in csv_files else csv_files

            df_list = [pd.read_csv(z.open(f), sep=';', encoding='latin-1', low_memory=False) for f in tqdm(csv_to_read, desc="Lendo arquivos CSV")]

            full_df = pd.concat(df_list, ignore_index=True)
            print(f"Sucesso! {len(full_df)} registros lidos de {len(csv_to_read)} arquivo(s) CSV.")
            return full_df
    except Exception as e:
        logger.error(f"Erro ao processar o arquivo: {e}")
        return None