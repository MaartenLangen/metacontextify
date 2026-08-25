import pandas as pd
import pytest

from metacontextify.pipelines import (
    get_properties_for_ena_samples,
    get_properties_for_mgnify_assemblies, 
    get_properties_for_mgnify_genomes,
    get_properties_for_mgnify_proteins,
    get_properties_for_mgnify_samples,
    get_properties_for_mgnify_search_results,
)

# Tests
# -----

def test_properties_for_ena_samples(
    mock_cmems_properties_from_dataset,
    mock_ena_sample_metadata,
):
    sample_ids = pd.Series(['ena_sample_1', 'ena_sample_2'], name='sample_id')

    results, failed_sample_ids = get_properties_for_ena_samples(sample_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(failed_sample_ids, list)
    assert results.shape[0] == 1
    assert results.shape[1] == 21
    assert len(failed_sample_ids) == 1

def test_properties_for_ena_samples_one_fake():
    sample_ids = pd.Series(['fake'], name='sample_id')

    results, failed_sample_ids = get_properties_for_ena_samples(sample_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(failed_sample_ids, list)
    assert results.shape[0] == 0
    assert results.shape[1] == 21
    assert len(failed_sample_ids) == 1


def test_properties_for_mgnify_samples(
    mock_cmems_properties_from_dataset,
    mock_mgnify_sample_metadata,
):
    sample_ids = pd.Series(['sample_1', 'sample_2'], name='sample_id')

    results, failed_sample_ids = get_properties_for_mgnify_samples(sample_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(failed_sample_ids, list)
    assert results.shape[0] == 1
    assert results.shape[1] == 22
    assert len(failed_sample_ids) == 1

def test_properties_for_mgnify_samples_one_fake():
    sample_ids = pd.Series(['fake'], name='sample_id')

    results, failed_sample_ids = get_properties_for_mgnify_samples(sample_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(failed_sample_ids, list)
    assert results.shape[0] == 0
    assert results.shape[1] == 22
    assert len(failed_sample_ids) == 1


def test_properties_for_mgnify_proteins(
    mock_cmems_properties_from_dataset,
    mock_mgnify_protein_to_assembly,
    mock_mgnify_assembly_to_sample,
    mock_mgnify_sample_metadata,
):
    protein_ids = pd.Series(['protein_1', 'protein_2', 'protein_3'], name='protein_id')

    results, incomplete = get_properties_for_mgnify_proteins(protein_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(incomplete, pd.DataFrame)
    assert results.shape[0] == 1
    assert results.shape[1] == 24
    assert incomplete.shape[0] == 3
    assert incomplete.shape[1] == 3

def test_properties_for_mgnify_proteins_one_fake():
    protein_ids = pd.Series(['fake'], name='protein_id')

    results, incomplete = get_properties_for_mgnify_proteins(protein_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(incomplete, pd.DataFrame)
    assert results.shape[0] == 0
    assert results.shape[1] == 24
    assert incomplete.shape[0] == 1
    assert incomplete.shape[1] == 3


def test_properties_for_mgnify_genomes(
    mock_cmems_properties_from_dataset,
    mock_mgnify_genome_to_sample,
    mock_ena_sample_metadata,
):
    genome_ids = pd.Series(['genome_1', 'genome_2', 'genome_3'], name='genome_id')

    results, incomplete = get_properties_for_mgnify_genomes(genome_ids)

    assert isinstance(results, pd.DataFrame)
    assert results.shape[0] == 2
    assert results.shape[1] == 23
    assert incomplete.shape[0] == 2
    assert incomplete.shape[1] == 2

def test_properties_for_mgnify_genomes_one_fake():
    genome_ids = pd.Series(['unknown'], name='genome_id')

    results, incomplete = get_properties_for_mgnify_genomes(genome_ids)

    assert isinstance(results, pd.DataFrame)
    assert results.shape[0] == 0
    assert results.shape[1] == 23
    assert incomplete.shape[0] == 1
    assert incomplete.shape[1] == 2

def test_properties_for_assemblies(
    mock_mgnify_sample_metadata,
    mock_mgnify_assembly_to_sample, 
    mock_cmems_properties_from_dataset
):
    assembly_ids = pd.Series(['assembly_1', 'assembly_2'], name='assembly_id')

    results, incomplete = get_properties_for_mgnify_assemblies(assembly_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(incomplete, pd.DataFrame)
    assert results.shape[0] == 1
    assert results.shape[1] == 23
    assert incomplete.shape[0] == 2
    assert incomplete.shape[1] == 2

def test_properties_for_assemblies_one_fake():
    assembly_ids = pd.Series(['fake'], name='assembly_id')

    results, incomplete = get_properties_for_mgnify_assemblies(assembly_ids)

    assert isinstance(results, pd.DataFrame)
    assert isinstance(incomplete, pd.DataFrame)
    assert results.shape[0] == 0
    assert results.shape[1] == 23
    assert incomplete.shape[0] == 1
    assert incomplete.shape[1] == 2


def test_mgnify_search_results(
    mock_cmems_properties_from_dataset
):

    result, incomplete = get_properties_for_mgnify_search_results(
        "tests/PF00313_hits.json",
        nb_hits=100,
    )

    assert isinstance(result, pd.DataFrame)
    assert isinstance(incomplete, pd.DataFrame)
    assert result.shape[1] == 24
    assert len(set(result['protein_id']).union(set(incomplete['protein_id']))) == 100 

