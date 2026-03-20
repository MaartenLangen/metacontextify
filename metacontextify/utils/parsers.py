"""
Parser functions for reading and processing various input file formats.
"""

import json
from pathlib import Path
from typing import Literal, Union

import pandas as pd

from .logging import get_logger

logger = get_logger("data_retrievers.parsers")


def read_mgnify_similarity_search_json(
        json_path: Union[str, Path],
        nb_hits: int = -1
) -> pd.DataFrame:
    """
    Parse MGnify similarity search JSON results.

    Parameters
    ----------
    json_path : str or Path
        Path to JSON file from MGnify similarity search results.
    nb_hits: int
        Read only the first nb_hits. Set to -1 to retrieve data for all hits.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: protein_id, assembly_id
    """
    logger.info("Reading json")
    rows: list
    with open(json_path, "r") as file:
        data = json.load(file)
        rows = [
            {"protein_id": hit["acc"], "assembly_id": [x[0] for x in hit["assemblies"]]}
            for hit in data["results"]["hits"]
        ]
    result = pd.DataFrame(rows)
    if nb_hits > 0:
        result = result.head(nb_hits)
    result = result.explode(column=["assembly_id"])
    return result


def read_id_file(
    file_path: Union[str, Path],
    id_type: Literal["protein", "genome", "assembly", "sample", "ena_sample"] = "protein",
) -> pd.Series:
    """
    Read a file containing a list of IDs (one per line).

    Parameters
    ----------
    file_path : str or Path
        Path to text file with one ID per line.
    id_type : {"protein", "genome", "assembly", "sample", "ena_sample"}, default="protein"
        Type of ID in the file. Determines the Series name.

    Returns
    -------
    pd.Series
        Series containing the IDs with appropriate name based on id_type.
    """
    logger.info("Reading file with IDs")

    file_path = Path(file_path)

    with open(file_path, "r") as f:
        ids = [line.strip() for line in f if line.strip()]

    column_name = {
        "protein": "protein_id",
        "genome": "genome_id",
        "assembly": "assembly_id",
        "sample": "sample_id",
        "ena_sample": "ena_sample_id",
    }.get(id_type, "id")

    return pd.Series(ids, name=column_name)


def parse_dates(date_str: str):
    """
    Parse a date string to a timezone-naive pandas Timestamp.

    Parameters
    ----------
    date_str : str
        Date string to parse.

    Returns
    -------
    pd.Timestamp or None
        Parsed timestamp without timezone information, or None when
        parsing fails.
    """
    try:
        date = pd.Timestamp(date_str).tz_localize(None)
    except:
        date = None
    return date