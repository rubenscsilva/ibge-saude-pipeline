"""Testes unitários para a camada de transformação."""

import pandas as pd
import pytest
from src.transform.clean import transform


def _base_df():
    return pd.DataFrame({
        "uf_codigo": ["35", "33", "35", "33"],
        "uf_nome": ["São Paulo", "Rio de Janeiro", "São Paulo", "Rio de Janeiro"],
        "ano": [2020, 2020, 2021, 2021],
        "taxa_mortalidade_infantil": ["11.2", "12.8", "10.9", "12.5"],
    })


def test_transform_output_columns():
    df = transform(_base_df())
    assert "regiao" in df.columns
    assert "taxa_mortalidade_infantil" in df.columns


def test_transform_tipos():
    df = transform(_base_df())
    assert df["taxa_mortalidade_infantil"].dtype == float
    assert df["ano"].dtype == int


def test_transform_regiao_preenchida():
    df = transform(_base_df())
    assert df["regiao"].notna().all()
    assert set(df["regiao"].unique()) == {"Sudeste"}


def test_transform_remove_nulos():
    df_input = _base_df()
    df_input.loc[0, "taxa_mortalidade_infantil"] = None
    df = transform(df_input)
    assert len(df) == 3


def test_transform_remove_uf_invalida():
    df_input = _base_df()
    df_input.loc[0, "uf_codigo"] = "99"  # UF inexistente
    df = transform(df_input)
    assert "99" not in df["uf_codigo"].values
