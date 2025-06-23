<!-- Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 16:10:59 -->

# PoliticaMente ETL

Este repositório contém os scripts de **ETL (Extract, Transform, Load)** para a plataforma PoliticaMente. A sua principal responsabilidade é extrair dados de fontes públicas (como o TSE), transformá-los para o nosso modelo de dados e carregá-los no banco de dados da aplicação.

Este é um projeto da organização **politicamente-app**.

## Funcionalidades

* **População de Partidos, Políticos e Candidaturas:** Extrai e carrega os dados de um arquivo CSV oficial de candidaturas do TSE de forma automatizada.

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
    pip install sqlalchemy psycopg2-binary python-dotenv requests
    ```
3.  **Crie e configure o arquivo `.env`:**
    * Crie um arquivo chamado `.env` na pasta raiz do projeto.
    * Dentro dele, adicione a string de conexão do seu banco de dados:
        ```
        DATABASE_URL="sua_string_de_conexao_aqui"
        ```

### 2. Como Executar o Script

O script agora pode inferir o ano da eleição mais recente ou aceitar um ano específico.

* **Para popular o banco com os dados da eleição mais recente (Recomendado):**
    ```sh
    python src/politicamente_etl/main.py seed_election_data
    ```

* **Para popular o banco com os dados de um ano específico:**
    ```sh
    python src/politicamente_etl/main.py seed_election_data --year 2022
    ```