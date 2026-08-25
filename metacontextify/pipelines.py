"""
Pipeline functions for processing environmental metadata from MGnify sequences.
"""

from pathlib import Path
from typing import Literal, Union

import pandas as pd

from .data_retrievers import cmems, ena, mgnify
from .utils import parsers
from .utils.logging import get_logger

logger = get_logger("pipelines")


def get_properties_for_mgnify_proteins(
    protein_ids: pd.Series,
    properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract environmental properties for MGnify protein IDs.

    Parameters
    ----------
    protein_ids: pd.Series
        Series with MGnify Protein IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    pd.DataFrame 
        DataFrame with protein ids for which metadata retrieval failed. 
        Depending on where in the process the failer arose, assembly id
        or sample id can be present as well.
        
    """

    # Map proteins to assemblies
    proteins_and_assemblies_df, failed_protein_ids = mgnify.protein_to_assembly(
        protein_ids
    )

    # Retrieve properties for the assemblies via their related samples
    relevant_assemblies = proteins_and_assemblies_df["assembly_id"].drop_duplicates()
    (
        assemblies_and_samples_properties_df, 
        failed_assemblies_df
    ) = get_properties_for_mgnify_assemblies(
        relevant_assemblies,
        properties
    )

    # Merge the completed results in one dataframe
    results_df = pd.merge(
        proteins_and_assemblies_df,
        assemblies_and_samples_properties_df,
        on="assembly_id",
        how="right",
    )
    # Merge the failed results in another dataframe
    incomplete_df = pd.DataFrame({
        "protein_id": failed_protein_ids,
        "assembly_id": pd.NA,
        "sample_id": pd.NA
    })
    incomplete_df = pd.concat([
        incomplete_df,
        pd.merge(
            proteins_and_assemblies_df,
            failed_assemblies_df,
            how='right', on='assembly_id'
        )
    ], ignore_index=True)
    

    return results_df, incomplete_df


def get_properties_for_mgnify_genomes(
        genome_ids: pd.Series,
        properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract environmental properties for MGnify genome IDs.

    Parameters
    ----------
    genome_ids : pd.Series
        Series with MGnify Genome IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    pd.DataFrame 
        DataFrame with genome ids for which metadata retrieval failed. 
        Depending on where in the process the failer arose, ena sample id 
        can be present as well.
    """
    # Map genomes to samples
    genomes_and_samples_df, failed_genome_ids = mgnify.genome_to_sample(genome_ids)

    # Get sample metadata
    relevant_samples = genomes_and_samples_df["ena_sample_id"].drop_duplicates()
    sample_properties_df, failed_ena_sample_ids = get_properties_for_ena_samples(
        relevant_samples,
        properties
    )

    # Merge the completed results in one dataframe
    results_df = pd.merge(genomes_and_samples_df, sample_properties_df, on="ena_sample_id", how="outer")

    # Merge the failed results in another dataframe
    incomplete_df = pd.DataFrame({
        "genome_id": failed_genome_ids,
        "ena_sample_id": pd.NA
    })
    incomplete_df = pd.concat([
        incomplete_df,
        genomes_and_samples_df[
            genomes_and_samples_df['ena_sample_id'].isin(failed_ena_sample_ids)
        ][["genome_id", "ena_sample_id"]].copy()
    ], ignore_index=True)

    return results_df, incomplete_df


def get_properties_for_mgnify_assemblies(
    assembly_ids: pd.Series,
    properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract environmental properties for MGnify Assembly IDs.

    Parameters
    ----------
    assembly_ids : pd.Series
        Series with MGnify Assembly IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metdata
    pd.DataFrame 
        DataFrame with failed assembly ids and where available sample ids.
        (if no sample id, API call for assembly id failed, else the
        API call for the sample id failed)
    pd.DataFrame 
        DataFrame with assembly ids for which metadata retrieval failed. 
        Depending on where in the process the failer arose, sample id 
        can be present as well.
    """
    # Map assemblies to samples
    assemblies_and_samples_df, failed_assembly_ids = mgnify.assembly_to_sample(assembly_ids)

    # Get sample metadata
    relevant_samples = assemblies_and_samples_df["sample_id"].drop_duplicates()
    sample_properties_df, failed_sample_ids = get_properties_for_mgnify_samples(
        relevant_samples,
        properties
    )

    # Merge the completed results in one dataframe
    results_df = pd.merge(
        assemblies_and_samples_df, sample_properties_df, on="sample_id", how="right"
    )

    # Merge the failed results in another dataframe
    incomplete_df = pd.DataFrame({
        "assembly_id": failed_assembly_ids,
        "sample_id": pd.NA
    })
    incomplete_df = pd.concat([
        incomplete_df,
        assemblies_and_samples_df[
            assemblies_and_samples_df['sample_id'].isin(failed_sample_ids)
        ].copy()
    ], ignore_index=True)


    return results_df, incomplete_df


def get_properties_for_mgnify_samples(
        sample_ids: pd.Series,
        properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, list[str]]:
    """
    Extract environmental properties for MGnify sample IDs.

    Parameters
    ----------
    sample_ids: pd.Series
        Series with MGnify Sample IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    list[str]
        mgnify sample ids for which no metadata could be retrieved
    """
    # Get sample metadata
    sample_metadata_df, failed_sample_ids = mgnify.get_mgnify_sample_metadata(sample_ids)
    # Get CMEMS properties
    environmental_properties_df = cmems.get_properties(
        sample_metadata_df,
        properties
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        sample_metadata_df, 
        environmental_properties_df, 
        left_index=True, 
        right_index=True,
        suffixes=['_metadata', '_copernicus']
    )

    return results_df, failed_sample_ids


def get_properties_for_ena_samples(
    sample_ids: pd.Series,
    properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, list[str]]:
    """
    Extract environmental properties for ENA Sample IDs.

    Parameters
    ----------
    sample_ids: pd.Series
        Series with ENA Sample IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    list
        ena sample ids for which no metadata could be retrieved
    """
    # Get sample metadata
    sample_metadata_df, failed_sample_ids = ena.get_ena_sample_metadata(sample_ids)

    # Get CMEMS properties
    environmental_properties_df = cmems.get_properties(
        sample_metadata_df,
        properties
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        sample_metadata_df, 
        environmental_properties_df, 
        left_index=True, 
        right_index=True,
        suffixes=['_metadata', '_copernicus']
    )

    return results_df, failed_sample_ids

def get_properties_for_ena_runs(
    run_ids: pd.Series,
    properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, list[str]]:
    """
    Extract environmental properties for ENA Run IDs.

    Parameters
    ----------
    run_ids: pd.Series
        Series with ENA Run IDs
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    list
        ena run ids for which no metadata could be retrieved
    """
    # Get sample metadata
    run_metadata_df, failed_run_ids = ena.get_ena_run_metadata(run_ids)

    # Get CMEMS properties
    environmental_properties_df = cmems.get_properties(
        run_metadata_df,
        properties
    )

    # Merge in one results dataframe
    results_df = pd.merge(
        run_metadata_df, 
        environmental_properties_df, 
        left_index=True, 
        right_index=True,
        suffixes=['_metadata', '_copernicus']
    )

    return results_df, failed_run_ids


def get_properties_for_mgnify_search_results(
    json_path: Union[str, Path], 
    properties: set[str] = parsers.ALL_PROPERTIES,
    nb_hits: int = -1
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract environmental properties for MGnify similarity search results.

    Parameters
    ----------
    json_path : str or Path
        Path to JSON file with MGnify similarity search results.
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton
    nb_hits: int
        Read only the first nb_hits. Set to -1 to retrieve data for all hits.

    Returns
    -------
    pd.DataFrame
        DataFrame with retrieved metadata
    pd.DataFrame 
        DataFrame with protein ids for which metadata retrieval failed. 
        Depending on where in the process the failer arose, assembly id
        or sample id can be present as well.
    """
    # Parse search results
    proteins_and_assemblies_df = parsers.read_mgnify_similarity_search_json(json_path, nb_hits)

    # Retrieve properties for the assemblies via their related samples
    relevant_assemblies = proteins_and_assemblies_df["assembly_id"].drop_duplicates()
    (
        assemblies_and_samples_properties_df, 
        incomplete_assembly_df
    ) = get_properties_for_mgnify_assemblies(
        relevant_assemblies,
        properties
    )

    # Merge the completed results in one dataframe
    results_df = pd.merge(
        proteins_and_assemblies_df,
        assemblies_and_samples_properties_df,
        on="assembly_id",
        how="right",
    )

    # Merge the failed results in another dataframe
    incomplete_df = pd.merge(
        proteins_and_assemblies_df,
        incomplete_assembly_df,
        on='assembly_id',
        how='right'
    )

    logger.info("Finished property retrieval")
    if incomplete_df.shape[0] > 0:
        logger.warning(
            "Not all data could be retrieved from the " +
            "external APIs. The identifiers with missing " +
            "data are written to file. Check the " +
            "validity of these identifiers and that the " +
            "used API servers (mgnify, ena, cmems) " +
            "are available. If you think there is a problem " +
            "with the package, feel free to leave an issue " +
            "or pull request on github. " 
        )

    return results_df, incomplete_df


def get_properties_for_id_file(
    id_file_path: Union[str, Path],
    id_type: Literal["protein", "genome", "assembly", "sample", "ena_sample"],
    properties: set[str] = parsers.ALL_PROPERTIES
) -> tuple[pd.DataFrame, pd.DataFrame] | tuple[pd.DataFrame, list[str]]:
    """
    Retrieve environmental properties for IDs listed in a text file.

    Parameters
    ----------
    id_file_path : str or Path
        Path to a text file containing one ID per line.
    id_type : {"protein", "genome", "assembly", "sample", "ena_sample", "ena_run"}
        Type of IDs in the input file.
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame containing identifiers, metadata, and retrieved
        environmental properties.
    pd.DataFrame | list[str]
        - If id_type == protein, genome, assembly: DataFrame with 
        protein ids for which metadata retrieval failed. 
        Depending on where in the process the failer arose, assembly id
        or sample id can be present as well.
        - If id_type. sample, ena_sample: list with ids for which 
        metadata retrieval failed
    """
    # Read IDs
    query_ids = parsers.read_id_file(id_file_path, id_type=id_type)

    # Call the appropriate function:
    if id_type == "protein":
        result, incomplete = get_properties_for_mgnify_proteins(query_ids, properties)
    elif id_type == "genome":
        result, incomplete = get_properties_for_mgnify_genomes(query_ids, properties)
    elif id_type == "assembly":
        result, incomplete = get_properties_for_mgnify_assemblies(query_ids, properties)
    elif id_type == "sample":
        result, incomplete = get_properties_for_mgnify_samples(query_ids, properties)
    elif id_type == "ena_sample":
        result, incomplete = get_properties_for_ena_samples(query_ids, properties)
    elif id_type == "ena_run":
        result, incomplete = get_properties_for_ena_runs(query_ids, properties)

    logger.info("Finished property retrieval")
    if (
        isinstance(incomplete, pd.DataFrame)
        and
        (incomplete.shape[0] > 0)
    ) or (
        isinstance(incomplete, list)
        and
        (len(incomplete) > 0)
    ):
        logger.warning(
            "Not all data could be retrieved from the " +
            "external APIs. The identifiers with missing " +
            "data are written to file. Check the " +
            "validity of these identifiers and that the " +
            "used API servers (mgnify, ena, cmems) " +
            "are available. If you think there is a problem " +
            "with the package, feel free to leave an issue " +
            "or pull request on github. " 
            )
    return result, incomplete


def get_properties_for_locations_file(
        locations_file_path: Union[str, Path],
        properties: set[str] = parsers.ALL_PROPERTIES
) -> pd.DataFrame:
    """
    Retrieve CMEMS properties for locations listed in a CSV file.

    Parameters
    ----------
    locations_file_path : str or Path
        Path to CSV file containing at least the columns required by
        CMEMS retrieval (lat, lon, sample_date and depth).
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame containing environmental properties retrieved from CMEMS.
    """
    # Read CSV file
    input_df = pd.read_csv(locations_file_path)

    # Retrieve properties
    result = cmems.get_properties(input_df, properties)

    result = pd.merge(
        input_df, result, 
        how='left', left_index=True, right_index=True
    )

    logger.info("Finished property retrieval")
    return result