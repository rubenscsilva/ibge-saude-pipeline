"""
Pipeline principal: Extract → Transform → Load

Uso:
    python -m src.pipeline
    python -m src.pipeline --periodo 2010-2022
    python -m src.pipeline --dry-run
"""

import argparse
from src.extract.ibge_api import fetch_obitos_por_uf
from src.transform.clean import transform
from src.load.bigquery_loader import load_to_bigquery
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run(periodo: str = "2010-2022", dry_run: bool = False) -> None:
    logger.info("=" * 50)
    logger.info(f"Pipeline IBGE Saúde | período: {periodo} | dry_run: {dry_run}")
    logger.info("=" * 50)

    df_raw   = fetch_obitos_por_uf(periodo=periodo)
    df_clean = transform(df_raw)

    if dry_run:
        logger.info("[DRY RUN] Pulando carga no BigQuery")
        print(df_clean.head(15).to_string())
        print(f"\nShape: {df_clean.shape}")
        print(f"Anos:  {sorted(df_clean['ano'].unique())}")
        print(f"UFs:   {df_clean['uf_codigo'].nunique()}")
    else:
        load_to_bigquery(df_clean)

    logger.info("Pipeline finalizado com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline IBGE Saúde → BigQuery")
    parser.add_argument("--periodo", default="2010-2022")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(periodo=args.periodo, dry_run=args.dry_run)
