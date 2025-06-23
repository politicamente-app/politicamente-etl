<!-- Este arquivo foi gerado/atualizado pelo DomTech Forger em 2025-06-23 14:09:29 -->

# PoliticaMente ETL

Este repositório contém os scripts de **ETL (Extract, Transform, Load)** para a plataforma PoliticaMente. A sua principal responsabilidade é extrair dados de fontes públicas (como o TSE), transformá-los para o nosso modelo de dados e carregá-los no banco de dados da aplicação.

Este é um projeto da organização **politicamente-app**.

## Funcionalidades

* **População de Partidos:** Extrai e carrega a lista oficial de partidos políticos registrados no Brasil diretamente do Portal de Dados Abertos do TSE.
* **População de Eleições:** (Futuro) Carrega os dados históricos das eleições.
* **População de Candidaturas:** (Futuro) Carrega a lista de todos os candidatos de cada eleição.

## Instalação e Uso

### Pré-requisitos

* Python 3.9+
* Um ambiente virtual Python (`venv`).

### Instalação

1.  **Clone o repositório:**
    ```sh
    git clone [https://github.com/politicamente-app/politicamente-etl.git](https://github.com/politicamente-app/politicamente-etl.git)
    cd politicamente-etl
    ```

2.  **Crie e ative um ambiente virtual:**
    ```sh
    python -m venv venv
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```sh
    pip install sqlalchemy psycopg2-binary python-dotenv requests
    ```
4.  **Crie e configure o arquivo `.env`:**
    * Crie um arquivo chamado `.env` na pasta raiz do projeto.
    * Dentro dele, adicione a string de conexão do seu banco de dados (a mesma usada pela API):
        ```
        DATABASE_URL="sua_string_de_conexao_aqui"
        ```

### Como Executar o Script

O script é modular. Você especifica qual tarefa (ou "seeder") deseja executar através de um argumento na linha de comando.

* **Para popular a tabela de partidos:**
    ```sh
    python src/politicamente_etl/main.py seed_parties
    ```
* **Para ver todas as opções disponíveis:**
    ```sh
    python src/politicamente_etl/main.py --help
    ```