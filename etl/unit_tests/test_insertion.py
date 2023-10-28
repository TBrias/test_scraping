import sys

import pandas as pd
import pytest

sys.path.append('../etl')
import insertion


@pytest.fixture
def sample_dataframe():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "titre": ["Titre 1", "Titre 2", "Titre 3"],
        "juridiction": ["Juridiction 1", "Juridiction 2", "Juridiction 3"],
        "date": ["01 janvier 2023", "02 février 2023", "03 mars 2023"],
        "numero_decision": ["Dec 1", "Dec 2", "Dec 3"],
        "texte": ["Texte 1", "Texte 2", "Texte 3"]
    })
    return df

def test_transform_to_dict(sample_dataframe):
    df_dict = insertion.transform_to_dict(sample_dataframe)
    assert len(df_dict) == 3
    assert df_dict[0]["date"] == "2023-01-01"
    assert df_dict[2]["numero_decision"] == "Dec 3"

def test_parse_to_strict_date_optional_time():
    date_str = insertion.parse_to_strict_date_optional_time("01 janvier 2023")
    assert date_str == "2023-01-01"

    

