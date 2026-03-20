import pandas as pd

from metacontextify.data_retrievers.ena import get_ena_sample_metadata


def test_list_sample_ids():
    """
    First two samples are non-marine, so will not be part of results dataframe
    """
    sample_ids = ["SAMEA9695022", "SAMEA9694413", "SAMD00000325"]
    result = get_ena_sample_metadata(sample_ids)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == [
        "sample_id",
        "lat",
        "lon",
        "depth",
        "sample_date",
        "temperature",
        "salinity",
        "ph",
    ]
    assert result.shape[0] == 3
