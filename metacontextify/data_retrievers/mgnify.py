"""
Data retrieval functions for MGnify API.
"""

from pathlib import Path
from tqdm import tqdm
from typing import Iterable, Union

import pandas as pd
from bs4 import BeautifulSoup

from ..utils import http
from ..utils.parsers import parse_dates
from ..utils.logging import get_logger

logger = get_logger("data_retrievers.mgnify")

BATCH_SIZE = 100


def _protein_to_assembly_from_website_worker(id) -> dict:
    """
    Retrieve assembly IDs associated with a single MGnify protein ID.

    Parameters
    ----------
    id : str
        MGnify protein ID.

    Returns
    -------
    dict
        Dictionary with keys: protein_id, assembly_ids.
    """

    response = http.retry_request(f"https://www.ebi.ac.uk/metagenomics/proteins/{id}/")
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract assemblies from table
    table = soup.find(id="assembly-table")
    assemblies = []
    if table:
        for row in table.select("tbody tr"):
            cells = row.find_all("td")
            if len(cells) > 1:
                assemblies.append(cells[1].get_text(strip=True))

    return {"protein_id": id, "assembly_ids": assemblies}


def protein_to_assembly_from_website(protein_ids: Iterable[str]) -> pd.DataFrame:
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
    """
    logger.info("Retrieving MGnify protein metadata")

    rows = [_protein_to_assembly_from_website_worker(x) for x in tqdm(protein_ids)]

    result = pd.DataFrame(rows)
    result = result.explode(["assembly_ids"]).rename(columns={"assembly_ids": "assembly_id"})
    return result


def _assembly_to_sample_batch(assembly_ids: Iterable[str]) -> list:
    """
    Map MGnify assembly IDs to sample IDs for a single batch.

    Parameters
    ----------
    assembly_ids : iterable of str
        MGnify assembly IDs for this batch (expected length <= 100).

    Returns
    -------
    list
        Rows where each element is a dictionary with columns:
        assembly_id, sample_id
    """
    assembly_ids = list(assembly_ids)
    url = "https://www.ebi.ac.uk/metagenomics/api/v1/assemblies"
    params = {"accession": ",".join(assembly_ids), "page_size": BATCH_SIZE}
    response = http.retry_request(url, params)
    json = http.validate_json(response)

    # Iterate over results pages
    has_next = True
    rows = []
    
    while has_next:
        for entry in json["data"]:
            assembly_id = entry["id"]
            # Extract sample IDs from relationships > samples > data
            samples_data = entry.get("relationships", {}).get("samples", {}).get("data", [])
            for sample in samples_data:
                sample_id = sample.get("id")
                if sample_id:
                    rows.append({"assembly_id": assembly_id, "sample_id": sample_id})
        # Check if next results page
        if json.get('links', {}).get('next', None):
            has_next = True
            next_url = json.get('links', {}).get('next', None)
            response = http.retry_request(next_url)
            json = http.validate_json(response)
        else:
            has_next = False

    return rows


def assembly_to_sample(assembly_ids: Iterable[str]) -> pd.DataFrame:
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
    """
    logger.info("Retrieving MGnify assembly metadata")
    assembly_ids_list = list(assembly_ids)
    rows: list = []

    for start in tqdm(range(0, len(assembly_ids_list), BATCH_SIZE)):
        batch = assembly_ids_list[start : start + BATCH_SIZE]
        if not batch:
            continue
        rows.extend(_assembly_to_sample_batch(batch))

    return pd.DataFrame(rows)


def _genome_to_sample_batch(genome_ids: Iterable[str]) -> list:
    """
    Map MGnify genome IDs to ENA sample IDs for a single batch.

    Parameters
    ----------
    genome_ids : iterable of str
        MGnify genome IDs for this batch (expected length <= 100).

    Returns
    -------
    list
        Rows where each element is a dictionary with columns:
        genome_id, is_marine, ena_sample_id.
    """
    genome_ids = list(genome_ids)
    url = "https://www.ebi.ac.uk/metagenomics/api/v1/genomes"
    params = {"accession": ",".join(genome_ids), "page_size": BATCH_SIZE}
    response = http.retry_request(url, params)
    json = http.validate_json(response)

    # Iterate over results pages
    has_next = True
    rows = []

    while has_next:
        # parse results
        for entry in json["data"]:
            genome_id = entry["id"]
            ena_sample_id = entry["attributes"]["ena-sample-accession"]
            biome = entry["relationships"]["biome"]["data"]["id"]
            if not biome.startswith("root:Environmental:Aquatic:Marine"):
                continue
            rows.append(
                {
                    "genome_id": genome_id,
                    "is_marine": True,
                    "ena_sample_id": ena_sample_id,
                }
            )
        # check if next results page
        if json.get('links', {}).get('next', None):
            has_next = True
            next_url = json.get('links', {}).get('next', None)
            response = http.retry_request(next_url)
            json = http.validate_json(response)
        else:
            has_next = False

    return rows


def genome_to_sample(genome_ids: Iterable[str]) -> pd.DataFrame:
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
    """
    logger.info("Retrieving MGnify genome metadata")
    genome_ids_list = list(genome_ids)
    rows: list = []

    for start in tqdm(range(0, len(genome_ids_list), BATCH_SIZE)):
        batch = genome_ids_list[start : start + BATCH_SIZE]
        if not batch:
            continue
        rows.extend(_genome_to_sample_batch(batch))

    return pd.DataFrame(rows)


def _get_mgnify_sample_metadata_batch(sample_ids: Iterable[str]) -> list:
    """
    Retrieve sample metadata from MGnify for a single batch.

    Parameters
    ----------
    sample_ids : iterable of str
        MGnify sample IDs for this batch (expected length <= 100).

    Returns
    -------
    list
        Rows where each element is a dictionary with columns:
        sample_id, is_marine, lat, lon, sample_date, depth, depth_units,
        temperature, temperature_units, salinity, salinity_units, nitrate,
        nitrate_units, oxygen, oxygen_units, pH.
    """

    # Call MGnify API
    sample_ids = list(sample_ids)
    url = "https://www.ebi.ac.uk/metagenomics/api/v1/samples"
    params = {
        "accession": ",".join(sample_ids),
        "biome_name": "root:Environmental:Aquatic:Marine",
        "page_size": BATCH_SIZE,
    }
    response = http.retry_request(url, params)
    json = http.validate_json(response)

    # Parse the json for lat, lon, depth, date, temperature
    rows = []
    key_map = {
        "depth": ("depth", "depth_units"),
        "geographic location (depth)": ("depth", "depth_units"),
        "temperature": ("temperature", "temperature_units"),
        "salinity": ("salinity", "salinity_units"),
        "salinity sensor": ("salinity", "salinity_units"),
        "nitrate": ("nitrate", "nitrate_units"),
        "nitrate sensor": ("nitrate", "nitrate_units"),
        "oxygen": ("oxygen", "oxygen_units"),
        "oxygen sensor": ("oxygen", "oxygen_units"),
        "pH": ("pH", None),
    }

    # Iterate over results pages
    has_next = True

    while has_next:
        for sample_data in json.get("data", []):
            sample_id = sample_data.get("id")
            attributes = sample_data.get("attributes", {})
            lat = attributes.get("latitude")
            lon = attributes.get("longitude")
            date = attributes.get("collection-date")

            # parse other metadata if available
            metadata_items = attributes.get("sample-metadata", [])
            # initialize the metadata fields of interest
            parsed = {
                "depth": None,
                "depth_units": None,
                "temperature": None,
                "temperature_units": None,
                "salinity": None,
                "salinity_units": None,
                "nitrate": None,
                "nitrate_units": None,
                "oxygen": None,
                "oxygen_units": None,
                "pH": None,
            }
            # iterate over the available metadata fields and check if they match one of the keys of interest
            for metadata_item in metadata_items:
                key = metadata_item.get("key")
                value = metadata_item.get("value")
                if isinstance(value, str):
                    value = value.replace(",", ".")
                unit = metadata_item.get("unit")
                if key == "collection date" and date is None:
                    date = value
                    continue
                if key in key_map:
                    field, unit_field = key_map[key]
                    parsed[field] = value
                    if unit_field:
                        parsed[unit_field] = unit
            rows.append(
                {
                    "sample_id": sample_id,
                    "is_marine": True,
                    "lat": lat,
                    "lon": lon,
                    "sample_date": date,
                    "depth": parsed["depth"],
                    "depth_units": parsed["depth_units"],
                    "temperature": parsed["temperature"],
                    "temperature_units": parsed["temperature_units"],
                    "salinity": parsed["salinity"],
                    "salinity_units": parsed["salinity_units"],
                    "nitrate": parsed["nitrate"],
                    "nitrate_units": parsed["nitrate_units"],
                    "oxygen": parsed["oxygen"],
                    "oxygen_units": parsed["oxygen_units"],
                    "pH": parsed["pH"],
                }
            )
        # Check if next results page
        if json.get('links', {}).get('next', None):
            has_next = True
            next_url = json.get('links', {}).get('next', None)
            response = http.retry_request(next_url)
            json = http.validate_json(response)
        else:
            has_next = False

    return rows


def get_mgnify_sample_metadata(sample_ids: Iterable[str]) -> pd.DataFrame:
    """
    Retrieve sample metadata from MGnify, batching requests in chunks of 100.

    Parameters
    ----------
    sample_ids : iterable of str
        MGnify sample IDs.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: sample_id, is_marine, lat, lon, sample_date,
        depth, depth_units, temperature, temperature_units, salinity,
        salinity_units, nitrate, nitrate_units, oxygen, oxygen_units, pH.
    """
    logger.info("Retrieving MGnify sample metadata")
    sample_ids_list = list(sample_ids)
    rows: list = []

    for start in tqdm(range(0, len(sample_ids_list), BATCH_SIZE)):
        batch = sample_ids_list[start : start + BATCH_SIZE]
        if not batch:
            continue
        rows.extend(_get_mgnify_sample_metadata_batch(batch))

    # Cast columns to appropriate type
    samples_df = pd.DataFrame(rows)
    numeric_cols = [
        "lat",
        "lon",
        "depth",
        "temperature",
        "salinity",
        "nitrate",
        "oxygen",
        "pH",
    ]
    for col in numeric_cols:
        samples_df[col] = pd.to_numeric(samples_df[col], errors="coerce")

    samples_df["sample_date"] = samples_df['sample_date'].apply(parse_dates)

    return samples_df
