"""
Extrai dados da API SIDRA/IBGE.

Tabela utilizada:
  - Tabela 265: Mortalidade infantil por UF
    https://sidra.ibge.gov.br/tabela/265

Documentação da API SIDRA:
  https://apisidra.ibge.gov.br/
"""

import requests
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

SIDRA_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

# Tabela 265 — Taxa de mortalidade infantil por UF
# Variável 793 = taxa de mortalidade infantil (por 1.000 nascidos vivos)
TABELA_MORTALIDADE = "265"
VARIAVEL_MORTALIDADE = "793"


def fetch_mortalidade_infantil(periodo: str = "2000-2022") -> pd.DataFrame:
    """
    Busca a taxa de mortalidade infantil por UF para um período.

    Args:
        periodo: String no formato 'AAAA-AAAA' ou 'AAAA|AAAA|AAAA'

    Returns:
        DataFrame com colunas: uf_codigo, uf_nome, ano, taxa_mortalidade
    """
    # Período: converte "2000-2022" para "2000|2001|...|2022"
    if "-" in periodo and "|" not in periodo:
        inicio, fim = periodo.split("-")
        periodo_sidra = "|".join(str(a) for a in range(int(inicio), int(fim) + 1))
    else:
        periodo_sidra = periodo

    url = (
        f"{SIDRA_BASE_URL}/{TABELA_MORTALIDADE}/periodos/{periodo_sidra}"
        f"/variaveis/{VARIAVEL_MORTALIDADE}"
        f"?localidades=N3[all]"  # N3 = UF
        f"&classificacao=0"      # sem classificação adicional
    )

    logger.info(f"Extraindo mortalidade infantil | período: {periodo}")
    logger.info(f"URL: {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    raw = response.json()
    return _parse_sidra_response(raw)


def _parse_sidra_response(raw: list) -> pd.DataFrame:
    """Normaliza a resposta da API SIDRA para DataFrame limpo."""
    records = []

    for agregado in raw:
        for resultado in agregado.get("resultados", []):
            for serie in resultado.get("series", []):
                localidade = serie.get("localidade", {})
                uf_codigo = localidade.get("id", "")
                uf_nome = localidade.get("nome", "")

                for ano, valor in serie.get("serie", {}).items():
                    records.append({
                        "uf_codigo": uf_codigo,
                        "uf_nome": uf_nome,
                        "ano": int(ano),
                        "taxa_mortalidade_infantil": valor,
                    })

    df = pd.DataFrame(records)
    logger.info(f"Extraídos {len(df)} registros | {df['ano'].nunique()} anos | {df['uf_codigo'].nunique()} UFs")
    return df
