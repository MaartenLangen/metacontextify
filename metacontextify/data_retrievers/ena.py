"""
Data retrieval functions for ENA (European Nucleotide Archive) API.
"""

from tqdm import tqdm
from typing import Iterable

import pandas as pd

from ..utils.parsers import parse_dates
from ..utils import http
from ..utils.logging import get_logger

logger = get_logger("data_retrievers.ena")

BATCH_SIZE = 100


def _get_ena_sample_metadata_batch(sample_ids: list[str]) -> list:
    """
    Retrieve ENA sample metadata for a single batch of sample IDs.

    Parameters
    ----------
    sample_ids : list of str
        ENA sample IDs in the batch.

    Returns
    -------
    list
        List of dictionaries returned by the ENA API for the requested samples.
    """
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    params = {
        "result": "sample",
        "includeAccessions": ",".join(sample_ids),
        "fields": "lat,lon,depth,collection_date,temperature,salinity,ph",
        "format": "json",
    }
    response = http.retry_request(url, params)
    json = http.validate_json(response)

    return json


def get_ena_sample_metadata(sample_ids: Iterable[str]) -> pd.DataFrame:
    """
    Retrieve sample metadata from ENA.

    Parameters
    ----------
    sample_ids : iterable of str
        ENA sample IDs.

    Returns
    -------
    pd.DataFrame
        test DataFrame with columns: sample_id, lat, lon, sample_date,
        depth, temp, sal, pH, and other environmental properties.
    """
    logger.info("Retrieving ENA sample metadata")
    sample_ids = list(sample_ids)
    rows: list = []

    for start in tqdm(range(0, len(sample_ids), BATCH_SIZE)):
        batch = sample_ids[start : start + BATCH_SIZE]
        if not batch:
            continue
        rows.extend(_get_ena_sample_metadata_batch(batch))

    # Cast columns to appropriate type
    samples_df = pd.DataFrame(rows)
    numeric_cols = [
        "lat",
        "lon",
        "depth",
        "temperature",
        "salinity",
        "ph",
    ]
    samples_df['depth'] = samples_df['depth'].apply(lambda x: x.replace('m', ''))
    for col in numeric_cols:
        samples_df[col] = samples_df[col].apply(lambda x: x.replace(',', '.'))
        samples_df[col] = pd.to_numeric(samples_df[col], errors="coerce")

    samples_df["collection_date"] = samples_df['collection_date'].apply(parse_dates)
    samples_df = samples_df.rename(
        columns={"sample_accession": "sample_id", "collection_date": "sample_date"}
    )

    return samples_df
