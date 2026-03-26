"""Testes unitários para a camada de extração."""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from src.extract.ibge_api import _parse_sidra_response


MOCK_SIDRA_RESPONSE = [
    {
        "resultados": [
            {
                "series": [
                    {
                        "localidade": {"id": "35", "nome": "São Paulo"},
                        "serie": {"2019": "11.5", "2020": "11.2", "2021": "10.9"},
                    },
                    {
                        "localidade": {"id": "33", "nome": "Rio de Janeiro"},
                        "serie": {"2019": "13.1", "2020": "12.8", "2021": "12.5"},
                    },
                ]
            }
        ]
    }
]


def test_parse_sidra_response_shape():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert len(df) == 6  # 2 UFs x 3 anos


def test_parse_sidra_response_columns():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert set(df.columns) == {"uf_codigo", "uf_nome", "ano", "taxa_mortalidade_infantil"}


def test_parse_sidra_response_tipos():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert df["ano"].dtype == int


def test_parse_sidra_response_valores():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    sp_2019 = df[(df["uf_codigo"] == "35") & (df["ano"] == 2019)]
    assert sp_2019["taxa_mortalidade_infantil"].values[0] == "11.5"
