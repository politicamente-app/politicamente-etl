<!-- Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-07-02 15:57:26 -->

# PoliticaMente ETL

Este repositório contém os scripts de **ETL (Extract, Transform, Load)** para a plataforma PoliticaMente. A sua principal responsabilidade é extrair dados de fontes públicas (como o TSE), transformá-los para o nosso modelo de dados e carregá-los no banco de dados da aplicação.

Este é um projeto da organização **politicamente-app**.

## Funcionalidades

* **População de Partidos, Políticos, Coligações e Candidaturas:** Extrai e carrega os dados de arquivos CSV oficiais de candidaturas do TSE de forma paralela e otimizada.
* **Atualização de Resultados:** Processa os arquivos de votação para atualizar as candidaturas com o total de votos e o status final (eleito, não eleito, etc.).

## Instalação e Uso

### Pré-requisitos

* Python 3.9+
* Um ambiente virtual Python (`venv`).

### 1. Instalação do Script

1.  **Crie e ative um ambiente virtual:**
    ```sh
    python -m venv venv
    source venv/bin/activate
    ```

2.  **Instale as dependências:**
    ```sh
    pip install sqlalchemy psycopg2-binary python-dotenv requests pandas tqdm
    ```
3.  **Crie e configure o arquivo `.env`:**
    * Crie um arquivo chamado `.env` na pasta raiz do projeto.
    * Dentro dele, adicione a string de conexão do seu banco de dados e a configuração de paralelismo:
        ```
        DATABASE_URL="sua_string_de_conexao_aqui"
        MAX_WORKERS=4
        ```

### 2. Como Executar o Script

O script agora tem um comando principal que executa todas as etapas de seeding de uma vez, além dos comandos individuais.

* **Para popular o banco com todos os dados de uma eleição (Recomendado):**
    ```sh
    python src/politicamente_etl/main.py seed_all --year 2022
    ```

* **Para executar etapas individuais (para depuração):**
    ```sh
    python src/politicamente_etl/main.py seed_parties --year 2022
    python src/politicamente_etl/main.py seed_politicians --year 2022
    python src/politicamente_etl/main.py seed_coalitions --year 2022
    python src/politicamente_etl/main.py seed_candidacies --year 2022
    python src/politicamente_etl/main.py update_results --year 2022
    ```