"""
Pipeline principal: Extract → Transform → Load

Uso:
    python -m src.pipeline
    python -m src.pipeline --periodo 2010-2022
    python -m src.pipeline --dry-run   (sem carga no BQ)
"""

import argparse
from src.extract.ibge_api import fetch_mortalidade_infantil
from src.transform.clean import transform
from src.load.bigquery_loader import load_to_bigquery
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run(periodo: str = "2000-2022", dry_run: bool = False) -> None:
    logger.info("=" * 50)
    logger.info(f"Pipeline IBGE Saúde | período: {periodo} | dry_run: {dry_run}")
    logger.info("=" * 50)

    # Extract
    df_raw = fetch_mortalidade_infantil(periodo=periodo)

    # Transform
    df_clean = transform(df_raw)

    # Load
    if dry_run:
        logger.info("[DRY RUN] Pulando carga no BigQuery")
        print(df_clean.head(10).to_string())
        print(f"\nShape: {df_clean.shape}")
    else:
        load_to_bigquery(df_clean)

    logger.info("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline IBGE Saúde → BigQuery")
    parser.add_argument("--periodo", default="2000-2022",
                        help="Período no formato AAAA-AAAA (default: 2000-2022)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Executa sem carregar no BigQuery")
    args = parser.parse_args()
    run(periodo=args.periodo, dry_run=args.dry_run)
