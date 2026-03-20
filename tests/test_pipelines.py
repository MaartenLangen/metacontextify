from unittest.mock import patch

import pandas as pd
import pytest

from metacontextify.pipelines import (
    get_properties_for_mgnify_proteins,
    get_properties_for_mgnify_search_results,
)

@pytest.fixture
def mock_phys():
    with patch("metacontextify.data_retrievers.cmems.get_phys") as mock:
        mock.return_value = [10.0, 37.0, False]
        yield mock


@pytest.fixture
def mock_chem():
    with patch("metacontextify.data_retrievers.cmems.get_chem") as mock:
        mock.return_value = [0.05, 0.05, 6.0, 0.05, 0.05, False]
        yield mock


def test_mgnify_search_results(mock_phys, mock_chem):
    result = get_properties_for_mgnify_search_results("tests/PF00313_hits.json", nb_hits=100)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 112
    assert result.shape[1] == 27
