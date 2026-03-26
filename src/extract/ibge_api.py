"""
Extrai dados da API SIDRA/IBGE.

Tabela utilizada:
  - Tabela 2681: Número de óbitos por UF (Registro Civil)
    https://sidra.ibge.gov.br/tabela/2681

  Variável 343 = Número de óbitos ocorridos no ano
  Classificação padrão: Sexo=Total, Mês=Total, Local=Total,
                        Idade=Total, Natureza=Total

Documentação da API SIDRA:
  https://servicodados.ibge.gov.br/api/docs/agregados
"""

import requests
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

SIDRA_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

TABELA_OBITOS   = "2681"
VARIAVEL_OBITOS = "343"
CHUNK_SIZE = 3  # API retorna 500 com muitos anos simultâneos


def fetch_obitos_por_uf(periodo: str = "2010-2022") -> pd.DataFrame:
    """
    Busca o número de óbitos por UF para um período.
    Faz requisições em blocos para respeitar os limites da API SIDRA.

    Args:
        periodo: String no formato 'AAAA-AAAA'

    Returns:
        DataFrame com colunas: uf_codigo, uf_nome, ano, total_obitos
    """
    inicio, fim = periodo.split("-")
    anos = list(range(int(inicio), int(fim) + 1))
    chunks = [anos[i:i + CHUNK_SIZE] for i in range(0, len(anos), CHUNK_SIZE)]

    logger.info(f"Extraindo óbitos por UF | {periodo} | {len(chunks)} blocos de até {CHUNK_SIZE} anos")

    dfs = []
    for chunk in chunks:
        periodo_str = "|".join(str(a) for a in chunk)
        url = (
            f"{SIDRA_BASE_URL}/{TABELA_OBITOS}/periodos/{periodo_str}"
            f"/variaveis/{VARIAVEL_OBITOS}"
            f"?localidades=N3[all]"
        )
        logger.info(f"  Buscando {chunk[0]}–{chunk[-1]}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        dfs.append(_parse_sidra_response(response.json()))

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total: {len(df)} registros | {df['ano'].nunique()} anos | {df['uf_codigo'].nunique()} UFs")
    return df


def _parse_sidra_response(raw: list) -> pd.DataFrame:
    """Normaliza a resposta da API SIDRA para DataFrame limpo."""
    records = []

    for variavel in raw:
        for resultado in variavel.get("resultados", []):
            for serie in resultado.get("series", []):
                localidade = serie.get("localidade", {})
                uf_codigo  = localidade.get("id", "")
                uf_nome    = localidade.get("nome", "")

                for ano, valor in serie.get("serie", {}).items():
                    records.append({
                        "uf_codigo":    uf_codigo,
                        "uf_nome":      uf_nome,
                        "ano":          int(ano),
                        "total_obitos": valor,
                    })

    return pd.DataFrame(records)
