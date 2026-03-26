"""Testes unitários para a camada de transformação."""

import pandas as pd
from src.transform.clean import transform


def _base_df():
    return pd.DataFrame({
        "uf_codigo":    ["35", "33", "35", "33"],
        "uf_nome":      ["São Paulo", "Rio de Janeiro", "São Paulo", "Rio de Janeiro"],
        "ano":          [2020, 2020, 2021, 2021],
        "total_obitos": ["264000", "131000", "271000", "138000"],
    })


def test_colunas_saida():
    df = transform(_base_df())
    assert "regiao" in df.columns
    assert "total_obitos" in df.columns


def test_tipos():
    df = transform(_base_df())
    assert df["total_obitos"].dtype in (float, "int64", "int32")
    assert df["ano"].dtype == int


def test_regiao_preenchida():
    df = transform(_base_df())
    assert df["regiao"].notna().all()
    assert set(df["regiao"].unique()) == {"Sudeste"}


def test_remove_nulos():
    df_input = _base_df()
    df_input.loc[0, "total_obitos"] = None
    df = transform(df_input)
    assert len(df) == 3


def test_remove_uf_invalida():
    df_input = _base_df()
    df_input.loc[0, "uf_codigo"] = "99"
    df = transform(df_input)
    assert "99" not in df["uf_codigo"].values
