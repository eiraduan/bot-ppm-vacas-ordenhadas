import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

def main(): 
    load_dotenv()

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    TABELA_DESTINO = "dados_ppm_vacas_ordenhadas"

    try:
        print("Tentando estabelecer a conexão com o banco de dados...")
        conexao = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Conexão estabelecida com sucesso!")
        
        cursor = conexao.cursor()

        print(f"Verificando e criando a tabela '{TABELA_DESTINO}' se ela não existir...")
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABELA_DESTINO}(
            id SERIAL PRIMARY KEY,
            nivel_territorial_codigo INTEGER,
            nivel_territorial VARCHAR(255),
            unidade_de_medida_codigo INTEGER,
            unidade_de_medida VARCHAR(255),
            valor INTEGER,
            municipio_codigo INTEGER, 
            municipio VARCHAR(255),
            ano_codigo INTEGER,
            ano INTEGER,
            variavel_codigo INTEGER,
            variavel VARCHAR(255)
        );
        """
        cursor.execute(create_table_query)
        
        conexao.commit()
        print(f"Tabela '{TABELA_DESTINO}' verificada/criada com sucesso.")

    except Exception as e:
        print(f"Erro: {e}")

    finally:
        if 'conexao' in locals() and conexao:
            cursor.close()
            conexao.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()