"""
Data retrieval functions for CMEMS (Copernicus Marine Environment Monitoring Service) API.
"""

from importlib.resources import files
from numpy import nan, nanmean
from pathlib import Path
from typing import Optional
from tqdm import tqdm

import appdirs
import copernicusmarine as cmems
import numpy as np
import os
import pandas as pd
import xarray


from ..utils.logging import get_logger
from ..utils.parsers import parse_dates

logger = get_logger("data_retrievers.cmems")

# Credential storage directory
CMEMS_DIR = Path(appdirs.user_config_dir("cmems"))
CREDS_FILE = CMEMS_DIR / ".copernicusmarine-credentials"

# Location of copernicus static datasets
STAT_PHYS = CMEMS_DIR / 'GLO-MFC_001_030_mask_bathy.nc'
STAT_CHEM = files("metacontextify").joinpath("data").joinpath("GLOBAL_REANALYSIS_BIO_001_029_mask.nc")


def login(username: str, password: str) -> None:
    """
    Authenticate with CMEMS API and optionally save credentials.

    Parameters
    ----------
    username : str
        CMEMS username.
    password : str
        CMEMS password.
    """
    os.makedirs(CMEMS_DIR, exist_ok=True)
    if CREDS_FILE.is_file():
        os.remove(CREDS_FILE)
    # Attempt authentication
    isSucces = cmems.login(
        username, password, configuration_file_directory=CMEMS_DIR, force_overwrite=True
    )
    if isSucces:
        logger.info("Login was successful")
    else:
        raise RuntimeError("CMEMS authentication failed. Please check your username and password.")


def _open_phys() -> tuple[xarray.Dataset, xarray.Dataset]:
    """Open CMEMS physical dataset.

    Opens the static global physical properties dataset with bathymetry from CMEMS and the
    daily variant with temperature and salinity data. The static version is downloaded to
    the device

    Returns
    -------
    xarray.Dataset
        CMEMS static physical dataset containing bathymetry data.
    xarray.Dataset
        CMEMS physical dataset containing temperature and salinity data.
    """
    assert CREDS_FILE.exists(), "You must login first!"
    # Static dataset
    # --------------
    # Check if the file is already downloaded, else, download it
    if not STAT_PHYS.is_file():
        cmems.get(
            dataset_id='cmems_mod_glo_phy_my_0.083deg_static',
            output_directory=CMEMS_DIR,
            credentials_file=CREDS_FILE,
            no_directories=True
        )
    ds_stat_phys = xarray.open_dataset(STAT_PHYS)

    # Daily dataset with temperature and salinity
    # -------------------------------------------
    logger.info("Opening CMEMS physical dataset")
    ds_phys = cmems.open_dataset(
        dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m", credentials_file=CREDS_FILE
    )
    return ds_stat_phys, ds_phys


def _open_chem() -> tuple[xarray.Dataset, xarray.Dataset]:
    """Open CMEMS biochemical dataset.

    Opens the static global biochemical properties dataset with bathymetry from CMEMS and the
    monthly variant with biochemical data. The static version is available in the data dir 
    of the package. 

    Returns
    -------
    xarray.Dataset
        CMEMS static biochemical dataset containing bathymetry data.
    xarray.Dataset
        CMEMS biochemical dataset containing biochemical data.
    """
    assert CREDS_FILE.exists(), "You must login first!"
    logger.info("Opening CMEMS chemical datasets")
    # Static dataset
    # --------------
    ds_stat_chem = xarray.open_dataset(STAT_CHEM)

    # Daily dataset with biochemical data
    # -----------------------------------
    ds_chem = cmems.open_dataset(
        dataset_id="cmems_mod_glo_bgc_my_0.25deg_P1M-m", credentials_file=CREDS_FILE
    )
    return ds_stat_chem, ds_chem


def _round_to_grid(lat: float, lon: float, resolution: float) -> tuple:
    """Round coordinates to nearest grid point.

    Rounds latitude and longitude to the nearest grid point based on
    the specified resolution.

    Parameters
    ----------
    lat : float
        Latitude in degrees.
    lon : float
        Longitude in degrees.
    resolution : float
        Grid resolution in degrees.

    Returns
    -------
    tuple
        (lat, lon) rounded to nearest grid point.
    """
    lat = round(lat / resolution) * resolution
    lon = round(lon / resolution) * resolution
    return lat, lon


def get_phys(
        coords: pd.Series, 
        ds_static: xarray.Dataset, 
        ds: xarray.Dataset
) -> list:
    """Retrieve physical properties (temperature and salinity) from CMEMS.

    Queries the CMEMS physical dataset for temperature and salinity at a
    specified location, depth, and date. Handles missing or coastal data
    by attempting averaging over a spatial box.

    Parameters
    ----------
    coords : pd.Series
        Series with keys: 'lat' (latitude), 'lon' (longitude),
        'depth' (depth in meters), 'sample_date' (datetime).
    ds_static: xarray.Dataset
        CMEMS static physical dataset opened by _open_phys
    ds : xarray.Dataset
        CMEMS physical dataset opened by _open_phys

    Returns
    -------
    list
        [temperature, salinity, is_grid_mean] 
        in degrees Celsius and practical salinity units,
        or [nan, nan, nan] if data is unavailable.
    """
    lat = coords['lat']
    lon = coords['lon']
    date = coords['sample_date']
    depth = coords['depth']

    is_grid_mean = False

    # Ignore entries with missing location or date
    # Depth = 0 is taken for entries with missing depth
    if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(date):
        return [nan, nan, nan]
    if pd.isnull(depth):
        depth = 0

    # determine coordinates of the nearest gridpoint
    lat, lon = _round_to_grid(lat, lon, (1 / 12))

    # Retrieve bathymetry at location. 
    # If terrestrial, bath is nan.
    depths = np.array(ds_static['depth'])
    bath, bath_lev = ds_static \
            .sel(
                latitude = lat,
                longitude = lon,
                method = 'nearest'
            )[['deptho', 'deptho_lev']] \
            .to_array().to_numpy()
    if pd.notna(bath): # grid point is marine
        bath_lev = int(bath_lev) - 1 # one-based to zero-based indexing
        # Check if data available at depth level
        depth_mask, bath_mask = ds_static.sel(
            latitude = lat,
            longitude = lon
        ).sel(
            depth = [depth, depths[bath_lev]],
            method='nearest'
        )['mask'].to_numpy()
        if depth_mask:
            result = ds.sel(
                latitude = lat,
                longitude = lon,
                depth = depth,
                time = date,
                method='nearest'
            )
            temperature, salinity = result[[
                'thetao', 'so'
            ]].to_array().to_numpy()
            return [temperature, salinity, is_grid_mean]
        elif (depth < (bath + 0.2*bath)) and bath_mask:
            result = ds.sel(
                latitude = lat,
                longitude = lon,
                depth = depths[bath_lev],
                time = date,
                method='nearest'
            )
            temperature, salinity = result[[
                'thetao', 'so'
            ]].to_array().to_numpy()
            return [temperature, salinity, is_grid_mean]

    # If none of the cases above returned data,    
    # check in grid whether data can be retrieved
    grid_mask = ds_static.sel(
        latitude = slice(lat-1/12, lat+1/12),
        longitude = slice(lon-1/12, lon+1/12)
    ).sel(
        depth = depth,
        method = 'nearest'
    )['mask'].values
    if np.any(grid_mask):
        is_grid_mean = True
        result = ds.sel(
            latitude = slice(lat-1/12, lat+1/12),
            longitude = slice(lon-1/12, lon+1/12)
        ).sel(
            depth = depth,
            time = date,
            method = 'nearest'
        )
        temperature, salinity = result[[
            'thetao', 'so'
        ]].mean(skipna=True).to_array().to_numpy()
        return [temperature, salinity, is_grid_mean]
    else:
        return [nan, nan, nan]


def get_chem(
        coords: pd.Series, 
        ds_static: xarray.Dataset, 
        ds: xarray.Dataset
) -> list:
    """Retrieve chemical properties (nitrate, oxygen, pH, phosphate
    and phytoplancton concentration in carbon) from CMEMS.

    Queries the CMEMS biogeochemical dataset for nitrate, oxygen, pH,
    phosphate and phytoplancton concentration in carbon at a
    specified location, depth, and date. Handles missing or coastal data
    by attempting averaging over a spatial box.

    Parameters
    ----------
    coords : pd.Series
        Series with keys: 'lat' (latitude), 'lon' (longitude),
        'depth' (depth in meters), 'sample_date' (datetime).
    ds_static: xarray.Dataset
        CMEMS static chemical dataset opened by _open_chem
    ds : xarray.Dataset
        CMEMS chemical dataset opened by _open_chem

    Returns
    -------
    list
        [nitrate, oxygen, pH, phosphate, phytoplancton concentration in carbon,
        is_grid_mean] or [nan, nan, nan, nan, nan, nan]
        if data is unavailable.
        Units: [mmol/m^3, mmol/m^3, 1, mmol/m^3 mmol/m^3]
    """
    lat = coords['lat']
    lon = coords['lon']
    date = coords['sample_date']
    depth = coords['depth']

    is_grid_mean = False

    # Ignore entries with missing location or date
    # Depth = 0 is taken for entries with missing depth
    if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(date):
        return [nan, nan, nan, nan, nan, nan]
    if pd.isnull(depth):
        depth = 0

    # determine coordinates of the nearest gridpoint
    lat, lon = _round_to_grid(lat, lon, (1 / 4))

    # Retrieve bathymetry at location. 
    # If terrestrial, bath is nan.
    depths = np.array(ds_static['depth'])
    bath, bath_lev = ds_static \
            .sel(
                latitude = lat,
                longitude = lon,
                method = 'nearest'
            )[['deptho', 'deptho_lev']] \
            .to_array().to_numpy()
    if pd.notna(bath): # grid point is marine
        bath_lev = int(bath_lev) - 1 # one-based to zero-based indexing
        # Check if data available at depth level
        depth_mask, bath_mask = ds_static.sel(
            latitude = lat,
            longitude = lon
        ).sel(
            depth = [depth, depths[bath_lev]],
            method='nearest'
        )['mask'].to_numpy()
        if depth_mask:
            result = ds.sel(
                latitude = lat,
                longitude = lon,
                depth = depth,
                time = date,
                method='nearest'
            )
            no3, o2, ph, po4, phyc = result[[
                'no3', 'o2', 'ph', 'po4', 'phyc'
            ]].to_array().to_numpy()
            return [no3, o2, ph, po4, phyc, is_grid_mean]
        elif (depth < (bath + 0.2*bath)) and bath_mask:
            result = ds.sel(
                latitude = lat,
                longitude = lon,
                depth = depths[bath_lev],
                time = date,
                method='nearest'
            )
            no3, o2, ph, po4, phyc = result[[
                'no3', 'o2', 'ph', 'po4', 'phyc'
            ]].to_array().to_numpy()
            return [no3, o2, ph, po4, phyc, is_grid_mean]

    # If none of the cases above returned data,    
    # check in grid whether data can be retrieved
    grid_mask = ds_static.sel(
        latitude = slice(lat-1/4, lat+1/4),
        longitude = slice(lon-1/4, lon+1/4)
    ).sel(
        depth = depth,
        method = 'nearest'
    )['mask'].values
    if np.any(grid_mask):
        is_grid_mean = True
        result = ds.sel(
            latitude = slice(lat-1/4, lat+1/4),
            longitude = slice(lon-1/4, lon+1/4)
        ).sel(
            depth = depth,
            time = date,
            method = 'nearest'
        )
        no3, o2, ph, po4, phyc = result[[
            'no3', 'o2', 'ph', 'po4', 'phyc'
        ]].mean(skipna=True).to_array().to_numpy()
        return [no3, o2, ph, po4, phyc, is_grid_mean]
    else:
        return [nan, nan, nan, nan, nan, nan]


def get_properties(df: pd.DataFrame, nb_workers: int = 1) -> pd.DataFrame:
    """
    Retrieve CMEMS properties for given locations and dates.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns: lat, lon, depth, sample_date
    nb_workers: int
        The number of workers used to retrieve copernicus properties

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: temperature, salinity, nitrate, oxygen and pH
    """

    # Convert sample_date to date objects (remove timezone if present)
    df["sample_date"] = df['sample_date'].apply(parse_dates)
    
    # Define dataset parameters and read dataset
    dataset_static_phys, dataset_phys = _open_phys()
    dataset_static_chem, dataset_chem = _open_chem()

    results = pd.DataFrame(index=df.index)

    tqdm.pandas()
    logger.info("Retrieve physical properties")
    phys = df.progress_apply(
        lambda row: get_phys(row, dataset_static_phys, dataset_phys),
        axis=1, 
        result_type="expand",
    )
    results[[
        "temperature",
        "salinity",
        "is_grid_mean_phys"
    ]] = phys.iloc[:, :3]
    logger.info("Retrieve biochemical properties")
    chem = df.progress_apply(
        lambda row: get_chem(row, dataset_static_chem, dataset_chem),
        axis=1,
        result_type="expand",
    )
    results[[
        "nitrate", 
        "oxygen", 
        "pH", 
        "phosphate", 
        "phytoplankton",
        "is_grid_mean_chem"
    ]] = chem.iloc[:, :6]

    return results