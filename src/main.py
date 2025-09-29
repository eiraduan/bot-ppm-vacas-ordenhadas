import logging
import download

from pathlib import Path

def setup_master_logging(log_file='processo_completo.log'):
    project_dir = Path(__file__).resolve().parent.parent
    log_dir = project_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / log_file

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def main():
    setup_master_logging()
    logger = logging.getLogger(__name__)
    logger.info("Iniciado o processo de ETL completo.")

    try:
        logger.info("--- Etapa 1: Download dos arquivos ---")
        download.main()

    except Exception as e:
        logger.error(f"O processo falhou em alguma etapa: {e}")

if __name__ == "__main__":
    main()