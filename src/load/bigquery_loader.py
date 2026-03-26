"""
Carrega DataFrame transformado no BigQuery.
Usa write_disposition=WRITE_TRUNCATE para idempotência (re-run seguro).
"""

import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

SCHEMA_MORTALIDADE = [
    bigquery.SchemaField("uf_codigo",                 "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("uf_nome",                   "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("ano",                       "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("taxa_mortalidade_infantil", "FLOAT",   mode="REQUIRED"),
    bigquery.SchemaField("regiao",                    "STRING",  mode="REQUIRED"),
]


def get_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    return bigquery.Client(project=project_id)


def load_to_bigquery(df: pd.DataFrame, table_name: str | None = None) -> None:
    """
    Carrega o DataFrame no BigQuery com WRITE_TRUNCATE.

    Args:
        df: DataFrame transformado
        table_name: Nome da tabela (default: BQ_TABLE_MORTALIDADE do .env)
    """
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset    = os.environ.get("BQ_DATASET", "ibge_saude")
    table      = table_name or os.environ.get("BQ_TABLE_MORTALIDADE", "mortalidade_infantil")

    table_ref = f"{project_id}.{dataset}.{table}"
    logger.info(f"Carregando {len(df)} linhas em {table_ref}...")

    client = get_client()

    # Garante que o dataset existe
    client.create_dataset(dataset, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA_MORTALIDADE,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # aguarda conclusão

    table_obj = client.get_table(table_ref)
    logger.info(f"Carga concluída: {table_obj.num_rows} linhas em {table_ref}")
