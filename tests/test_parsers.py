import pandas as pd

from metacontextify.utils.parsers import read_id_file, read_mgnify_similarity_search_json


def test_mgnify_json_parser():
    result = read_mgnify_similarity_search_json("tests/PF00313_hits.json")
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 122704
    assert set(result.columns) == set(["protein_id", "assembly_id"])
    assert (
        result[result["protein_id"] == "MGYP000111278967"]["assembly_id"].values[0] == "ERZ1694526"
    )


def test_id_parser():
    result = read_id_file("tests/samples.txt", "sample")
    assert isinstance(result, pd.Series)
    assert result.shape[0] == 1488
    assert result.name == "sample_id"
