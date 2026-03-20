import pandas as pd

from metacontextify.data_retrievers.mgnify import (
    assembly_to_sample,
    genome_to_sample,
    get_mgnify_sample_metadata,
    protein_to_assembly_from_website,
)
from metacontextify.utils.http import retry_request, validate_json

# protein to assembly


def test_proteins_from_website():
    protein_ids = [
        "MGYP000858010875",
        "MGYP000159137439",
        "MGYP001252391544",
        "MGYP001428012659",
        "MGYP003638789697",
    ]
    result = protein_to_assembly_from_website(protein_ids)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["protein_id", "assembly_id"]
    assert result.shape[0] == 27


# Assemblies to samples


def test_list_assembly_ids():
    """
    First two assemblies are non-marine, so will not be part of results dataframe
    """
    assembly_ids = ["ERZ312748", "ERZ312752", "ERZ304795", "ERZ505226"]
    result = assembly_to_sample(assembly_ids)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["assembly_id", "sample_id"]
    assert result.shape[0] == 4
    assert result[result["assembly_id"] == "ERZ304795"]["sample_id"].values[0] == "ERS1188259"
    assert result[result["assembly_id"] == "ERZ505226"]["sample_id"].values[0] == "SRS1589155"


# genomes to samples


def test_list_genome_ids():
    """
    First two genomes are non-marine, so will not be part of results dataframe
    """
    genome_ids = ["MGYG000307600", "MGYG000512084", "MGYG000296006", "MGYG000296008"]
    result = genome_to_sample(genome_ids)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["genome_id", "is_marine", "ena_sample_id"]
    assert result.shape[0] == 2
    assert (
        result[result["genome_id"] == "MGYG000296006"]["ena_sample_id"].values[0] == "SAMEA9695022"
    )
    assert (
        result[result["genome_id"] == "MGYG000296008"]["ena_sample_id"].values[0] == "SAMEA9694413"
    )


# Samples to metadata


def test_list_sample_ids():
    """
    First two samples are non-marine, so will not be part of results dataframe
    """
    sample_ids = ["SRS26124488", "ERS4662755", "ERS20387607", "ERS12744197"]
    result = get_mgnify_sample_metadata(sample_ids)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 2


def test_series_sample_ids():
    """
    Same as test_list_sample_ids, but now starting from a pandas series.
    """
    sample_ids = ["SRS26124488", "ERS4662755", "ERS20387607", "ERS12744197"]
    result = get_mgnify_sample_metadata(pd.Series(sample_ids))
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 2


def test_list_sample_ids_big():
    with open("./tests/samples.txt", "r") as f:
        sample_ids = f.readlines()
    sample_ids = [x.strip() for x in sample_ids]
    assert len(sample_ids) == 1488
    result = get_mgnify_sample_metadata(sample_ids)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1488
