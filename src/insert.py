import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import logging 

def main():
    load_dotenv()
    logger = logging.getLogger(__name__)
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    DIRETORIO_SCRIPT = Path(__file__).parent
    PASTA_ARQUIVOS = DIRETORIO_SCRIPT.parent / "files"
    TABELA_DESTINO = "dados_ppm_vacas_ordenhadas"

    logger.info("Iniciando o processo de ETL (Extrair, Transformar, Carregar)...")

    try:
        url_object = URL.create(
            "postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME,
        )
        engine = create_engine(url_object)
        logger.info("Conexão com o banco de dados estabelecida com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        exit()

    arquivo_consolidado = PASTA_ARQUIVOS / "PPM_RO_VACAS_ORDENHADAS_FINAL.xlsx"

    if not arquivo_consolidado.exists():
        logger.info(f"Erro: O arquivo '{arquivo_consolidado.name}' não foi encontrado na pasta '{PASTA_ARQUIVOS}'.")
    else:
        try:
            logger.info(f"Processando o arquivo: {arquivo_consolidado.name}")
            
            df = pd.read_excel(arquivo_consolidado)
     
            df.rename(columns={
                "Nível Territorial (Código)": "nivel_territorial_codigo",
                "Nível Territorial": "nivel_territorial",
                "Unidade de Medida (Código)": "unidade_de_medida_codigo",
                "Unidade de Medida": "unidade_de_medida",
                "Valor": "valor",
                "Município (Código)": "municipio_codigo",
                "Município": "municipio",
                "Ano (Código)": "ano_codigo",
                "Ano": "ano",
                "Variável (Código)": "variavel_codigo",
                "Variável": "variavel"
            }, inplace=True)

            if not df.empty:
                logger.info(f"{len(df)} linhas prontas para serem carregadas.")
                
                df.to_sql(
                    name=TABELA_DESTINO,
                    con=engine,
                    if_exists='append', 
                    index=False
                )
                logger.info(f"Dados do arquivo '{arquivo_consolidado.name}' salvos na tabela '{TABELA_DESTINO}' com sucesso.")
            else:
                logger.info(f"Nenhuma linha encontrada no arquivo {arquivo_consolidado.name}.")
                
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo {arquivo_consolidado.name}: {e}")

    logger.info("Processamento finalizado.")

if __name__ == "__main__":
    main()