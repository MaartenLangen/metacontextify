import pytest
from unittest.mock import patch
import pandas as pd

#import metacontextify.data_retrievers.cmems

# Fixtures
# --------

@pytest.fixture
def mock_cmems_properties_from_dataset():
    with patch(
            "metacontextify.data_retrievers.cmems._get_properties_from_dataset"
        ) as mock:

        def mock_implementation(
                coords, ds_static, ds, resolution, properties
        ):
            return [1] * len(properties) + [False, False]
        
        mock.side_effect = mock_implementation

        yield mock


@pytest.fixture
def mock_ena_sample_metadata():
    with patch(
        "metacontextify.data_retrievers.ena.get_ena_sample_metadata"
    ) as mock:
        mock.return_value = (
            pd.DataFrame({
                'ena_sample_id': ['ena_sample_1'],
                'lat': [1],
                'lon': [1],
                "depth": [0],
                "sample_date": [pd.to_datetime('31-12-2024', dayfirst=True)],
                "temperature": [10],
                "salinity": [36],
                "ph": [7],
            }),
            ['ena_sample_2']
        )
        yield mock


@pytest.fixture
def mock_mgnify_protein_to_assembly():
    with patch(
        "metacontextify.data_retrievers.mgnify.protein_to_assembly"
    ) as mock:
        mock.return_value = (
            pd.DataFrame({
                'protein_id': ['protein_1', 'protein_2'],
                'assembly_id': ['assembly_1', 'assembly_2']
            }),
            ['protein_3']
        )
        yield mock


@pytest.fixture
def mock_mgnify_assembly_to_sample():
    with patch(
        "metacontextify.data_retrievers.mgnify.assembly_to_sample"
    ) as mock:
        mock.return_value = (
            pd.DataFrame({
                "assembly_id": ["assembly_1", "assembly_1"],
                "sample_id": ["sample_1", "sample_2"],
            }),
            ["assembly_2"]
        )
        yield mock


@pytest.fixture
def mock_mgnify_genome_to_sample():
    with patch(
        "metacontextify.data_retrievers.mgnify.genome_to_sample"
    ) as mock:
        mock.return_value = (
            pd.DataFrame({
                "genome_id": ["genome_1", "genome_2"],
                "is_marine": [True, False],
                "ena_sample_id": ["ena_sample_1", "ena_sample_2"],
            }),
            ["genome_3"]
        )
        yield mock


@pytest.fixture
def mock_mgnify_sample_metadata():
    with patch(
        "metacontextify.data_retrievers.mgnify.get_mgnify_sample_metadata"
    ) as mock:
        mock.return_value = (
            pd.DataFrame({
                'sample_id': ['sample_1'],
                'is_marine': [True],
                'lat': [1],
                'lon': [1],
                'depth_raw': '0m',
                "depth": [0],
                "sample_date": [pd.to_datetime('31-12-2024', dayfirst=True)],
                "temperature": [10],
                "ph": [7],
            }),
            ['sample_2']
        )
        yield mock