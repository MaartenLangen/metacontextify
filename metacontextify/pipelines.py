"""
Pipeline functions for processing environmental metadata from MGnify sequences.
"""

from pathlib import Path
from typing import Literal, Optional, Union

import pandas as pd

from .data_retrievers import cmems, ena, mgnify
from .utils import parsers
from .utils.logging import get_logger

logger = get_logger("pipelines")


def get_properties_for_mgnify_search_results(
    json_path: Union[str, Path], 
    nb_hits: int = -1,
) -> pd.DataFrame:
    """
    Extract environmental properties for MGnify similarity search results.

    Parameters
    ----------
    json_path : str or Path
        Path to JSON file with MGnify similarity search results.
    nb_hits: int
        Read only the first nb_hits. Set to -1 to retrieve data for all hits.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: protein_id, assembly_id, sample_id,
        lat, lon, date, depth, temp, sal, pH, etc.
    """
    # Parse search results
    proteins_and_assemblies_df = parsers.read_mgnify_similarity_search_json(json_path, nb_hits)

    # Retrieve properties for the assemblies via their related samples
    relevant_assemblies = proteins_and_assemblies_df["assembly_id"].drop_duplicates()
    assemblies_and_samples_properties_df = get_properties_for_mgnify_assemblies(
        relevant_assemblies,
    )

    # Merge results in one dataframe
    results_df = pd.merge(
        proteins_and_assemblies_df,
        assemblies_and_samples_properties_df,
        on="assembly_id",
        how="outer",
    )

    logger.info("Finished property retrieval")

    return results_df


def get_properties_for_mgnify_proteins(
    protein_ids: pd.Series,
) -> pd.DataFrame:
    """
    Extract environmental properties for MGnify protein IDs.

    Parameters
    ----------
    protein_ids: pd.Series
        Series with MGnify Protein IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: protein_id, sample_id, lat, lon, date, depth, temp, sal, pH, etc.
    """

    # Map proteins to assemblies
    proteins_and_assemblies_df = mgnify.protein_to_assembly_from_website(protein_ids)

    # Retrieve properties for the assemblies via their related samples
    relevant_assemblies = proteins_and_assemblies_df["assembly_id"].drop_duplicates()
    assemblies_and_samples_properties_df = get_properties_for_mgnify_assemblies(
        relevant_assemblies,
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        proteins_and_assemblies_df,
        assemblies_and_samples_properties_df,
        on="assembly_id",
        how="outer",
    )

    return results_df


def get_properties_for_mgnify_genomes(
        genome_ids: pd.Series,
) -> pd.DataFrame:
    """
    Extract environmental properties for MGnify genome IDs.

    Parameters
    ----------
    genome_ids : pd.Series
        Series with MGnify Genome IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: genome_id, sample_id, lat, lon, date, depth, temp, sal, pH, etc.
    """
    # Map genomes to samples
    genomes_and_samples_df = mgnify.genome_to_sample(genome_ids)

    # Get sample metadata
    relevant_samples = genomes_and_samples_df["sample_id"].drop_duplicates()
    sample_properties_df = get_properties_for_ena_samples(
        relevant_samples,
    )

    # Merge in one results dataframe
    results_df = pd.merge(genomes_and_samples_df, sample_properties_df, on="sample_id", how="outer")

    return results_df


def get_properties_for_mgnify_assemblies(
    assembly_ids: pd.Series,
) -> pd.DataFrame:
    """
    Extract environmental properties for MGnify Assembly IDs.

    Parameters
    ----------
    assembly_ids : pd.Series
        Series with MGnify Assembly IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: assembly_id, sample_id, lat, lon, date, depth, temp, sal, pH, etc.
    """
    # Map assemblies to samples
    assemblies_and_samples_df = mgnify.assembly_to_sample(assembly_ids)

    # Get sample metadata
    relevant_samples = assemblies_and_samples_df["sample_id"].drop_duplicates()
    sample_properties_df = get_properties_for_mgnify_samples(
        relevant_samples,
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        assemblies_and_samples_df, sample_properties_df, on="sample_id", how="outer"
    )

    return results_df


def get_properties_for_mgnify_samples(
        sample_ids: pd.Series,
) -> pd.DataFrame:
    """
    Extract environmental properties for MGnify sample IDs.

    Parameters
    ----------
    sample_ids: pd.Series
        Series with MGnify Sample IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: sample_id, lat, lon, date, depth, temp, sal, pH, etc.
    """
    # Get sample metadata
    sample_metadata_df = mgnify.get_mgnify_sample_metadata(sample_ids)

    # Get CMEMS properties
    environmental_properties_df = cmems.get_properties(
        sample_metadata_df,
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        sample_metadata_df, 
        environmental_properties_df, 
        left_index=True, 
        right_index=True,
        suffixes=['_metadata', '_copernicus']
    )

    return results_df


def get_properties_for_ena_samples(
    sample_ids: pd.Series,
) -> pd.DataFrame:
    """
    Extract environmental properties for ENA Sample IDs.

    Parameters
    ----------
    sample_ids: pd.Series
        Series with ENA Sample IDs

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: sample_id, lat, lon, date, depth, temp, sal, pH, etc.
    """
    # Get sample metadata
    sample_metadata_df = ena.get_ena_sample_metadata(sample_ids)

    # Get CMEMS properties
    environmental_properties_df = cmems.get_properties(
        sample_metadata_df,
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        sample_metadata_df, 
        environmental_properties_df, 
        left_index=True, 
        right_index=True,
        suffixes=['_metadata', '_copernicus']
    )

    return results_df


def get_properties_for_id_file(
    id_file_path: Union[str, Path],
    id_type: Literal["protein", "genome", "assembly", "sample", "ena_sample"],
) -> pd.DataFrame:
    """
    Retrieve environmental properties for IDs listed in a text file.

    Parameters
    ----------
    id_file_path : str or Path
        Path to a text file containing one ID per line.
    id_type : {"protein", "genome", "assembly", "sample", "ena_sample"}
        Type of IDs in the input file.

    Returns
    -------
    pd.DataFrame
        DataFrame containing identifiers, metadata, and retrieved
        environmental properties.
    """
    # Read IDs
    query_ids = parsers.read_id_file(id_file_path, id_type=id_type)

    # Call the appropriate function:
    if id_type == "protein":
        result = get_properties_for_mgnify_proteins(query_ids)
    elif id_type == "genome":
        result = get_properties_for_mgnify_genomes(query_ids)
    elif id_type == "assembly":
        result = get_properties_for_mgnify_assemblies(query_ids)
    elif id_type == "sample":
        result = get_properties_for_mgnify_samples(query_ids)
    elif id_type == "ena_sample":
        result = get_properties_for_ena_samples(query_ids)

    logger.info("Finished property retrieval")
    return result


def get_properties_for_locations_file(
        locations_file_path: Union[str, Path],
) -> pd.DataFrame:
    """
    Retrieve CMEMS properties for locations listed in a CSV file.

    Parameters
    ----------
    locations_file_path : str or Path
        Path to CSV file containing at least the columns required by
        CMEMS retrieval (lat, lon, sample_date and depth).

    Returns
    -------
    pd.DataFrame
        DataFrame containing environmental properties retrieved from CMEMS.
    """
    # Read CSV file
    input_df = pd.read_csv(locations_file_path)

    # Retrieve properties
    result = cmems.get_properties(input_df)

    result = pd.merge(
        input_df, result, 
        how='left', left_index=True, right_index=True
    )

    logger.info("Finished property retrieval")
    return result