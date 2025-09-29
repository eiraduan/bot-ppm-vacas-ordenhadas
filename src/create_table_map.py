import os
import psycopg2
from dotenv import load_dotenv
import logging 

def main():
    load_dotenv()
    logger = logging.getLogger(__name__)
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    TABELA_MAPA = "mapa_ppm_vacas_ordenhadas"
    TABELA_DADOS = "dados_ppm_vacas_ordenhadas"
    TABELA_MUNICIPIOS = "ro_municipios_2022"

    try:
        logger.info("Tentando estabelecer a conexão com o banco de dados...")
        conexao = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logger.info("Conexão estabelecida com sucesso!")
        
        cursor = conexao.cursor()

        logger.info(f"Verificando e criando a tabela '{TABELA_MAPA}' se ela não existir...")
        
        create_table_query = f"""
        CREATE TABLE gisadmin.{TABELA_MAPA} AS
        SELECT
            dp.nivel_territorial_codigo,
            dp.nivel_territorial,
            dp.unidade_de_medida_codigo,
            dp.unidade_de_medida,
            dp.valor,
            dp.municipio_codigo,
            dp.municipio,
            dp.ano_codigo,
            dp.ano,
            dp.variavel_codigo,
            dp.variavel,
            rm.nm_mun,
            rm.shape AS geom
        FROM
            gisadmin.{TABELA_DADOS} AS dp
        INNER JOIN
            gisadmin.{TABELA_MUNICIPIOS} AS rm
        ON
            CAST(dp.municipio_codigo AS VARCHAR) = rm.cd_mun;

        ALTER TABLE gisadmin.{TABELA_MAPA} ADD COLUMN id SERIAL PRIMARY KEY;
        """

        cursor.execute(create_table_query)
        
        conexao.commit()
        logger.info(f"Tabela '{TABELA_MAPA}' verificada/criada com sucesso.")

    except Exception as e:
        logger.error(f"Erro: {e}")

    finally:
        if 'conexao' in locals() and conexao:
            cursor.close()
            conexao.close()
            logger.info("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()