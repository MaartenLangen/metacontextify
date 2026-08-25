"""
Data retrieval functions for ENA (European Nucleotide Archive) API.
"""

from tqdm import tqdm
from typing import Iterable, Dict, Any

import pandas as pd
import requests

from ..utils.parsers import parse_dates, parse_depths
from ..utils import http
from ..utils.logging import get_logger

logger = get_logger("data_retrievers.ena")

BATCH_SIZE = 100


def _get_ena_metadata_batch(
    ids: list[str],
    ena_type: str
) -> list[Dict[str, Any]]:
    """
    Retrieve ENA  metadata for a single batch of IDs.

    Parameters
    ----------
    ids : list[str]
        ENA accessions to query in a single request.
    ena_type: str
        Type of ENA identifiers. Options: sample, run

    Returns
    -------
    Dict[str, Any]
        Parsed JSON payload returned by the ENA API.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP request fails or ENA returns an error response.
    """
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    if ena_type == 'sample':
        params = {
            "result": "sample",
            "includeAccessions": ",".join(ids),
            "fields": "sample_accession,lat,lon,depth,collection_date,temperature,salinity,ph",
            "format": "json",
        }
    elif ena_type == 'run':
        params = {
            "result": "read_run",
            "includeAccessions": ",".join(ids),
            "fields": "run_accession,sample_accession,lat,lon,depth,collection_date,temperature,salinity,ph",
            "format": "json",
        }
    response = http.retry_request(url, params)
    return http.validate_json(response)


def _get_ena_metadata_batch_with_fallback(
    ids: list[str],
    ena_type: str
) -> tuple[list[dict], list[str]]:
    """
    Try one batch first. If ENA rejects it, split the batch in half
    until the invalid accession(s) are isolated.

    Parameters
    ----------
    ids : list[str]
        ENA accessions to query.
    ena_type: str
        Type of ENA identifiers. Options: sample, run

    Returns
    -------
    tuple[list[dict], list[str]]
        A tuple containing the retrieved metadata rows and the
        accessions that could not be retrieved.
    """
    try:
        return _get_ena_metadata_batch(ids, ena_type), []
    except requests.exceptions.RequestException:

        if len(ids) == 1:
            return [], ids

        midpoint = len(ids) // 2
        left_rows, left_failed = _get_ena_metadata_batch_with_fallback(
            ids[:midpoint],
            ena_type
        )
        right_rows, right_failed = _get_ena_metadata_batch_with_fallback(
            ids[midpoint:],
            ena_type
        )
        return left_rows + right_rows, left_failed + right_failed

def _get_ena_metadata(
        ids: list[str],
        ena_type: str
) -> tuple[list[dict], list[str]]:
    """
    Retrieve metadata from ENA.
    
    Parameters
    ----------
    ids : list[str]
        ENA accessions to query.
    ena_type: str
        Type of ENA identifiers. Options: sample, run
    
    Returns
    -------
    tuple[list[dict], list[str]]
        A tuple containing the retrieved metadata rows and the
        accessions that could not be retrieved.
    """
    rows: list[dict] = []
    failed_ids: list[str] = []

    for start in tqdm(range(0, len(ids), BATCH_SIZE)):
        batch = ids[start : start + BATCH_SIZE]
        if not batch:
            continue
    
        batch_rows, batch_failed_ids = _get_ena_metadata_batch_with_fallback(
            batch,
            ena_type
        )
        rows.extend(batch_rows)
        failed_ids.extend(batch_failed_ids)
    
    failed_ids = list(set(failed_ids))

    return rows, failed_ids
    
def _get_ena_dataframe(rows: list[dict]) -> pd.DataFrame:
    """
    Parse the rows returned by ENA api to a dataframe

    Parameters
    ----------
    rows : List
        the retrieved metadata rows
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns: (ena_run_id), ena_sample_id, lat, lon, 
        depth_raw, depth, sample_date, temperature, salinity, ph.
    """
    df = pd.DataFrame(rows)
    df.rename(columns={
        'sample_accession': 'ena_sample_id',
        'run_accession': 'ena_run_id',
        'depth': 'depth_raw',
        "collection_date": "sample_date"
    }, inplace=True)
    numeric_cols = [
        "lat",
        "lon",
        "depth",
        "temperature",
        "salinity",
        "ph",
    ]
    
    df["depth"] = df["depth_raw"].apply(
        parse_depths
    )
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x.replace(",", ".") if isinstance(x, str) else x
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    if "sample_date" in df.columns:
        df["sample_date"] = df["sample_date"].apply(parse_dates)

    return df


def get_ena_sample_metadata(
    sample_ids: Iterable[str],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Retrieve sample metadata from ENA.

    Parameters
    ----------
    sample_ids : iterable of str
        ENA sample accessions to retrieve.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: ena_sample_id, lat, lon, 
        depth_raw, depth, sample_date, temperature, salinity, ph.
    list[str]
        Sample accessions that could not be retrieved.
    """
    logger.info("Retrieving ENA sample metadata")
    sample_ids = list(sample_ids)

    rows, failed_ids = _get_ena_metadata(sample_ids, 'sample')

    if len(rows) == 0:
        samples_df = pd.DataFrame(
            [],
            columns=[
                "ena_sample_id",
                "lat",
                "lon",
                "depth",
                "sample_date",
                "temperature",
                "salinity",
                "ph",
            ]
        )
        return (samples_df, failed_ids)

    samples_df = _get_ena_dataframe(rows)

    return samples_df, failed_ids

def get_ena_run_metadata(
    run_ids: Iterable[str],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Retrieve sample metadata from ENA.

    Parameters
    ----------
    sample_ids : iterable of str
        ENA sample accessions to retrieve.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: ena_run_id, ena_sample_id, lat, lon, 
        depth_raw, depth, sample_date, temperature, salinity, ph.
    list[str]
        Sample accessions that could not be retrieved.
    """
    logger.info("Retrieving ENA sample metadata")
    run_ids = list(run_ids)

    rows, failed_ids = _get_ena_metadata(run_ids, 'run')

    if len(rows) == 0:
        samples_df = pd.DataFrame(
            [],
            columns=[
                "ena_run_id",
                "ena_sample_id",
                "lat",
                "lon",
                "depth",
                "sample_date",
                "temperature",
                "salinity",
                "ph",
            ]
        )
        return (samples_df, failed_ids)

    samples_df = _get_ena_dataframe(rows)

    return samples_df, failed_ids
