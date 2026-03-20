import os
import warnings
from pathlib import Path

import pandas as pd
import pytest
import xarray
from dotenv import load_dotenv

from metacontextify.data_retrievers.cmems import (
    CREDS_FILE,
    _open_chem,
    _open_phys,
    get_properties,
    login,
)

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


@pytest.fixture
def credentials():
    """Fixture to get credentials from environment variables."""
    username = os.getenv("CMEMS_USERNAME")
    password = os.getenv("CMEMS_PASSWORD")

    if not username or not password:
        pytest.skip("CMEMS_USERNAME and CMEMS_PASSWORD environment variables must be set")

    return username, password


@pytest.fixture
def small_df():
    sample_date = pd.Timestamp(year=2004, month=1, day=1)
    df = pd.DataFrame(
        [
            [50.86374144949997, 4.687394178510209, 0, sample_date], # on land
            [42.677, 3.182, 0, sample_date],  # should work for phys
            [42.582, 3.077, 0, sample_date],  # should get temp by neighbour for phys
            [42.748, 3.088, 35, sample_date],  # depth too deep
        ],
        columns=["lat", "lon", "depth", "sample_date"],
    )
    return df

@pytest.fixture
def large_df():
    """Mock dataframe with 100 locations in the Southern Ocean"""
    data = []
    sample_date = pd.Timestamp(year=2004, month=1, day=1)
    for i in range(0,100):
        data.append(
            [
                -60, i, 0, sample_date
            ]
        )
    df = pd.DataFrame(
        data, columns=["lat", "lon", "depth", "sample_date"]
    )
    return df

def test_login_fail(credentials):
    username, _ = credentials

    # Remove credential file, if it exists
    if CREDS_FILE.is_file():
        os.remove(CREDS_FILE)
    # Use a wrong password to test failure
    try:
        login(username, "wrong_password")
    except RuntimeError:
        assert not CREDS_FILE.is_file()


def test_login_succes(credentials):
    username, password = credentials

    # Remove credential file, if it exists
    if CREDS_FILE.is_file():
        os.remove(CREDS_FILE)
    # Try to create credentials file
    login(username, password)
    # make sure a file exists
    assert CREDS_FILE.is_file()


def test_open_phys():
    static_ds, ds = _open_phys()
    assert isinstance(static_ds, xarray.Dataset)
    assert isinstance(ds, xarray.Dataset)


def test_open_chem():
    static_ds, ds = _open_chem()
    assert isinstance(static_ds, xarray.Dataset)
    assert isinstance(ds, xarray.Dataset)


def test_phys_coords_small(small_df):
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        results = get_properties(small_df)
    assert not any(
        isinstance(warning.message, RuntimeWarning)
        and "Mean of empty slice" in str(warning.message)
        for warning in caught_warnings
    )
    assert isinstance(results, pd.DataFrame)
    assert results.shape[0] == 4
    assert set(results.columns) == set(["temperature", "salinity", "is_grid_mean_phys", "nitrate", "oxygen", "pH", "phosphate", "phytoplankton", "is_grid_mean_chem"])

def test_large(large_df): 
    result = get_properties(large_df)
    assert result.shape[0] == 100
    assert set(result.columns) == set(["temperature", "salinity", "is_grid_mean_phys", "nitrate", "oxygen", "pH", "phosphate", "phytoplankton","is_grid_mean_chem"])
