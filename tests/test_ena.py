import pandas as pd

from metacontextify.data_retrievers.ena import get_ena_sample_metadata, get_ena_run_metadata


def test_list_sample_ids():
    sample_ids = ["SAMEA9695022", "SAMEA9694413", "SAMD00000325", "random"]
    result, failed_ids = get_ena_sample_metadata(sample_ids)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set([
        "ena_sample_id",
        "lat",
        "lon",
        "depth",
        "depth_raw",
        "sample_date",
        "temperature",
        "salinity",
        "ph",
    ])
    assert result.shape[0] == 3
    assert failed_ids == ["random"]

def test_list_run_ids():
    run_ids = ["DRR022199", "DRR022202", "DRR000013", "random"]
    result, failed_ids = get_ena_run_metadata(run_ids)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set([
        "ena_run_id",
        "ena_sample_id",
        "lat",
        "lon",
        "depth",
        "depth_raw",
        "sample_date",
        "temperature",
        "salinity",
        "ph",
    ])
    assert result.shape[0] == 3
    assert failed_ids == ["random"]