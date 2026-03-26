"""Testes unitários para a camada de extração."""

import pandas as pd
from src.extract.ibge_api import _parse_sidra_response

MOCK_SIDRA_RESPONSE = [
    {
        "id": "343",
        "variavel": "Número de óbitos ocorridos no ano",
        "resultados": [
            {
                "series": [
                    {
                        "localidade": {"id": "35", "nome": "São Paulo"},
                        "serie": {"2020": "264000", "2021": "271000"},
                    },
                    {
                        "localidade": {"id": "33", "nome": "Rio de Janeiro"},
                        "serie": {"2020": "131000", "2021": "138000"},
                    },
                ]
            }
        ],
    }
]


def test_parse_shape():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert len(df) == 4  # 2 UFs x 2 anos


def test_parse_columns():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert set(df.columns) == {"uf_codigo", "uf_nome", "ano", "total_obitos"}


def test_parse_ano_int():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    assert df["ano"].dtype == int


def test_parse_valores():
    df = _parse_sidra_response(MOCK_SIDRA_RESPONSE)
    sp_2020 = df[(df["uf_codigo"] == "35") & (df["ano"] == 2020)]
    assert sp_2020["total_obitos"].values[0] == "264000"
