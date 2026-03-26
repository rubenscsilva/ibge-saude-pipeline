"""
Transforma e valida o DataFrame bruto do IBGE antes da carga no BigQuery.
"""

import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

CODIGOS_UF_VALIDOS = {
    "11", "12", "13", "14", "15", "16", "17",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "31", "32", "33", "35",
    "41", "42", "43",
    "50", "51", "52", "53",
}

REGIOES = {
    "11": "Norte",    "12": "Norte",    "13": "Norte",   "14": "Norte",
    "15": "Norte",    "16": "Norte",    "17": "Norte",
    "21": "Nordeste", "22": "Nordeste", "23": "Nordeste", "24": "Nordeste",
    "25": "Nordeste", "26": "Nordeste", "27": "Nordeste", "28": "Nordeste",
    "29": "Nordeste",
    "31": "Sudeste",  "32": "Sudeste",  "33": "Sudeste",  "35": "Sudeste",
    "41": "Sul",      "42": "Sul",      "43": "Sul",
    "50": "Centro-Oeste", "51": "Centro-Oeste", "52": "Centro-Oeste",
    "53": "Centro-Oeste",
}


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de transformação:
    1. Cast de tipos
    2. Limpeza de valores nulos / inválidos
    3. Enriquecimento com coluna de região
    4. Validações de qualidade
    """
    logger.info("Iniciando transformação...")
    df = df.copy()

    # 1. Cast
    df["total_obitos"] = pd.to_numeric(df["total_obitos"], errors="coerce")
    df["ano"]          = df["ano"].astype(int)
    df["uf_codigo"]    = df["uf_codigo"].astype(str).str.strip()
    df["uf_nome"]      = df["uf_nome"].str.strip()

    # 2. Remover nulos e zeros
    linhas_antes = len(df)
    df = df.dropna(subset=["total_obitos"])
    df = df[df["total_obitos"] > 0]
    logger.info(f"Removidas {linhas_antes - len(df)} linhas nulas/inválidas")

    # 3. Filtrar UFs válidas
    df = df[df["uf_codigo"].isin(CODIGOS_UF_VALIDOS)]

    # 4. Enriquecer com região
    df["regiao"] = df["uf_codigo"].map(REGIOES)

    # 5. Ordenar
    df = df.sort_values(["ano", "uf_codigo"]).reset_index(drop=True)

    _validar(df)
    logger.info(f"Transformação concluída: {len(df)} registros válidos")
    return df


def _validar(df: pd.DataFrame) -> None:
    assert len(df) > 0, "DataFrame vazio após transformação"
    assert df["total_obitos"].between(1, 500_000).all(), \
        "total_obitos fora do intervalo esperado"
    assert df["uf_codigo"].isin(CODIGOS_UF_VALIDOS).all(), \
        "Códigos de UF inválidos encontrados"
    assert df["regiao"].notna().all(), "UFs sem região mapeada"
    logger.info("Validações de qualidade: OK")
