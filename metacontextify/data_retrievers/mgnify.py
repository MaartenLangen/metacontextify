"""
Data retrieval functions for MGnify API.
"""

from pathlib import Path
from tqdm import tqdm
from typing import Iterable, Union

import duckdb
import pandas as pd
from bs4 import BeautifulSoup

from ..utils import http
from ..utils.parsers import parse_dates, parse_depths
from ..utils.logging import get_logger

logger = get_logger("data_retrievers.mgnify")

BATCH_SIZE = 100


def _protein_ids_to_ints(protein_ids: Iterable[str]) -> list[int]:
    protein_ints = []
    for id in protein_ids:
        try:
            assert id.upper().startswith('MGYP')
            protein_ints.append(int(id[4:]))
        except:
            continue
    return protein_ints


def _protein_int_to_id(protein_int: int) -> str:
    return f"MGYP{protein_int:012d}"


def protein_to_assembly(protein_ids: Iterable[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    Map MGnify protein IDs to assembly IDs by querying MGnify website.

    Parameters
    ----------
    protein_ids : iterable of str
        MGnify protein IDs to map.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: protein_id, assembly_id
        Note: proteins can be associated with multiple assemblies. One row per assembly
    list[str]
        IDs for which retrieval raised a request error.
    """
    logger.info("Retrieving MGnify protein metadata")

    protein_ids_ints = _protein_ids_to_ints(protein_ids)

    proteins_and_assemblies_df = duckdb.sql(
        """
        SELECT protein_id, assembly_accession AS assembly_id
        FROM 'https://ftp.ebi.ac.uk/pub/databases/metagenomics/peptide_database/2026_07/mgy_clusters_metadata.parquet'
        WHERE protein_id = ANY(?)
        """,
        params=[protein_ids_ints]
    ).to_df()
    proteins_and_assemblies_df['protein_id'] = proteins_and_assemblies_df['protein_id'].apply(_protein_int_to_id)

    failed_ids = list(set(protein_ids) - set(proteins_and_assemblies_df['protein_id']))
    
    return proteins_and_assemblies_df, failed_ids


def _assembly_to_sample_worker(assembly_id: str) -> dict:
    """
    Map MGnify assembly ID to sample ID for a single entry.

    Parameters
    ----------
    assembly_id : str
        MGnify assembly ID.

    Returns
    -------
    dict
        A dictionary with items:
        assembly_id, sample_id
    """
    url = f"https://www.ebi.ac.uk/metagenomics/api/v2/assemblies/{assembly_id}"
    response = http.retry_request(url)
    json = http.validate_json(response)

    sample_id = json.get('sample_accession')

    return {"assembly_id": assembly_id, "sample_id": sample_id}


def assembly_to_sample(assembly_ids: Iterable[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    Map MGnify assembly IDs to sample IDs.

    Parameters
    ----------
    assembly_ids : iterable of str
        MGnify assembly IDs to map.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: assembly_id, sample_id
    list[str]
        IDs for which retrieval raised a request error.
    """
    logger.info("Retrieving MGnify assembly metadata")

    rows = []
    failed_ids = []
    for id in tqdm(assembly_ids):
        try:
            rows.append(_assembly_to_sample_worker(id))
        except http.RequestException:
            failed_ids.append(id)

    if len(rows) > 0:
        result = pd.DataFrame(rows)
    else:
        result = pd.DataFrame([], columns=['assembly_id', 'sample_id'])

    return result, failed_ids


def _genome_to_sample_worker(genome_id: str) -> dict:
    """
    Map MGnify genome IDs to ENA sample IDs for a single entry.

    Parameters
    ----------
    genome_id : str
        MGnify genome ID.

    Returns
    -------
    dict
        A dictionary with items:
        genome_id, is_marine, ena_sample_id.
    """
    url = f"https://www.ebi.ac.uk/metagenomics/api/v2/genomes/{genome_id}"
    response = http.retry_request(url)
    json = http.validate_json(response)

    sample_id = json.get('ena_sample_accession')
    biome = json.get('biome', {}).get('lineage', '')
    is_marine = biome.startswith("root:Environmental:Aquatic:Marine")

    return {
        "genome_id": genome_id,
        "is_marine": is_marine,
        "ena_sample_id": sample_id,
    }


def genome_to_sample(genome_ids: Iterable[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    Map MGnify genome IDs to sample IDs.

    Parameters
    ----------
    genome_ids : iterable of str
        MGnify genome IDs to map.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: genome_id, is_marine, ena_sample_id
    list[str]
        IDs for which retrieval raised a request error.
    """
    logger.info("Retrieving MGnify genome metadata")
    rows = []
    failed_ids = []
    for id in tqdm(genome_ids):
        try:
            rows.append(_genome_to_sample_worker(id))
        except http.RequestException:
            failed_ids.append(id)

    if len(rows) > 0:
        results = pd.DataFrame(rows)
    else:
        results = pd.DataFrame(
            [],
            columns = [
                'genome_id',
                'is_marine',
                'ena_sample_id'
            ]
        )

    return results, failed_ids


def _get_mgnify_sample_metadata_worker(sample_id: str) -> dict:
    """
    Retrieve sample metadata from MGnify for a single entry.

    Parameters
    ----------
    sample_id : str
        MGnify sample ID.

    Returns
    -------
    dict
        A dictionary with items:
        sample_id, is_marine, lat, lon, sample_date, depth,
        temperature, ph.
    """

    results = {'sample_id': sample_id}

    # Call MGnify API
    url = f"https://www.ebi.ac.uk/metagenomics/api/v2/samples/{sample_id}"
    response = http.retry_request(url)
    json = http.validate_json(response)
    retrieved_metadata = json.get("metadata", {})

    results['lat'] = retrieved_metadata.get('lat', None)
    results['lon'] = retrieved_metadata.get('lon', None)
    results['sample_date'] = retrieved_metadata.get('collection_date', None)
    results['depth_raw'] = retrieved_metadata.get('depth', None)
    results['temperature'] = retrieved_metadata.get('temperature', None)
    results['ph'] = retrieved_metadata.get('ph', None)
    biome = json.get('studies', [{}])[0].get('biome', {}).get('lineage', '')
    results['is_marine'] = biome.startswith("root:Environmental:Aquatic:Marine")

    return results


def get_mgnify_sample_metadata(sample_ids: Iterable[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    Retrieve sample metadata from MGnify.

    Parameters
    ----------
    sample_ids : iterable of str
        MGnify sample IDs.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: sample_id, is_marine, lat, lon, sample_date,
        depth, temperature, ph.
    list[str]
        IDs for which retrieval raised a request error.
    """
    logger.info("Retrieving MGnify sample metadata")
    rows = []
    failed_ids = []
    for id in tqdm(sample_ids):
        try:
            rows.append(_get_mgnify_sample_metadata_worker(id))
        except http.RequestException:
            failed_ids.append(id)

    if len(rows) == 0:
        samples_df = pd.DataFrame(
            [],
            columns=[
                'sample_id',
                'lat',
                'lon',
                'sample_date',
                'depth_raw',
                'temperature',
                'ph',
                'is_marine',
                'depth'
            ]
        )
        return samples_df, failed_ids

    # Cast columns to appropriate type
    samples_df = pd.DataFrame(
        rows
    )
    numeric_cols = [
        "lat",
        "lon",
        "temperature",
        "ph",
    ]
    for col in numeric_cols:
        samples_df[col] = pd.to_numeric(samples_df[col], errors="coerce")

    samples_df["sample_date"] = samples_df['sample_date'].apply(parse_dates)
    samples_df["depth"] = samples_df['depth_raw'].apply(parse_depths)

    return samples_df, failed_ids
