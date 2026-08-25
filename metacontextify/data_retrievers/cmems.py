"""
Data retrieval functions for CMEMS (Copernicus Marine Environment Monitoring Service) API.
"""

from importlib.resources import files
from numpy import nan, nanmean
from pathlib import Path
from typing import Optional, Iterable
from tqdm import tqdm

import appdirs
import copernicusmarine as cmems
import numpy as np
import os
import pandas as pd
import xarray


from ..utils.logging import get_logger
from ..utils import parsers

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
        raise ValueError("CMEMS authentication failed. Please check your username and password.")


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


def _open_chem() -> tuple[xarray.Dataset, xarray.Dataset, xarray.Dataset]:
    """Open CMEMS biochemical dataset.

    Opens the static global biochemical properties dataset with bathymetry from CMEMS, the
    daily variant with biochemical data, and the monthly version for pH and phytoplankton concentration. The static version
    is available in the data dir of the package. 

    Returns
    -------
    xarray.Dataset
        CMEMS static biochemical dataset containing bathymetry data.
    xarray.Dataset
        CMEMS biochemical dataset containing daily biochemical data.
    xarray.Dataset
        CMEMS biochemical dataset containing monthly biochemical data.
    """
    assert CREDS_FILE.exists(), "You must login first!"
    logger.info("Opening CMEMS chemical datasets")
    # Static dataset
    # --------------
    ds_stat_chem = xarray.open_dataset(STAT_CHEM)

    # Daily dataset with biochemical data
    # -----------------------------------
    ds_chem_daily = cmems.open_dataset(
        dataset_id="cmems_mod_glo_bgc_my_0.25deg_P1D-m", credentials_file=CREDS_FILE
    )

    # Monthly dataset with biochemical data
    # -----------------------------------
    ds_chem_monthly = cmems.open_dataset(
        dataset_id="cmems_mod_glo_bgc_my_0.25deg_P1M-m", credentials_file=CREDS_FILE
    )
    return ds_stat_chem, ds_chem_daily, ds_chem_monthly


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


def _get_properties_from_dataset(
        coords: pd.Series, 
        ds_static: xarray.Dataset, 
        ds: xarray.Dataset,
        resolution: float,
        properties: list[str]
) -> list:
    """Retrieve requested properties from a CMEMS dataset.

    Queries the provided CMEMS dataset for the provided properties at a
    specified location, depth, and date. Handles missing or coastal data
    by attempting averaging over a spatial box.

    Parameters
    ----------
    coords : pd.Series
        Series with keys: 'lat' (latitude), 'lon' (longitude),
        'depth' (depth in meters), 'sample_date' (datetime).
    ds_static: xarray.Dataset
        CMEMS static dataset opened by _open_phys or _open_chem
    ds : xarray.Dataset
        CMEMS dataset opened by _open_phys or _open_chem
    resolution: float
        Resolution of the CMEMS dataset
    properties: set[str]
        A set of properties to be retrieved. Possible values are:
            - physical: temperature, salinity
            - daily chemical: nitrate, phosphate, oxygen
            - monthly chemical: ph, phyc

    Returns
    -------
    list
        desired properties + is_grid_mean + is_depth_approx
        nans if data is unavailable.
    """
    lat = coords['lat']
    lon = coords['lon']
    date = coords['sample_date']
    depth = coords['depth']

    # translate property names to names used by Copernicus
    translate_properties = {
        'temperature': 'thetao',
        'salinity': 'so',
        'nitrate': 'no3',
        'phosphate': 'po4',
        'oxygen': 'o2',
        'ph': 'ph',
        'phytoplankton': 'phyc'
    }
    properties = [translate_properties[property] for property in properties]

    is_grid_mean = False
    is_depth_approx = False

    # When no result can be retrieved, return the right number of nans
    nan_result = [nan] * (len(properties) + 2)

    # Ignore entries with missing location or date
    # Depth = 0 is taken for entries with missing depth
    if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(date):
        return nan_result
    if pd.isnull(depth):
        depth = 0

    # determine coordinates of the nearest gridpoint
    lat, lon = _round_to_grid(lat, lon, resolution)

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
            property_values = result[properties].to_array().to_numpy()
            return list(property_values) + [is_grid_mean, is_depth_approx]
        # If depth 20% deeper than bath, use values at bath
        elif (depth < (bath + 0.2*bath)) and bath_mask:
            is_depth_approx = True
            result = ds.sel(
                latitude = lat,
                longitude = lon,
                depth = depths[bath_lev],
                time = date,
                method='nearest'
            )
            property_values = result[properties].to_array().to_numpy()
            return list(property_values) + [is_grid_mean, is_depth_approx]

    # If none of the cases above returned data,    
    # check in grid whether data can be retrieved
    grid_mask = ds_static.sel(
        latitude = slice(lat-resolution, lat+resolution),
        longitude = slice(lon-resolution, lon+resolution)
    ).sel(
        depth = depth,
        method = 'nearest'
    )['mask'].values
    if np.any(grid_mask):
        is_grid_mean = True
        result = ds.sel(
            latitude = slice(lat-resolution, lat+resolution),
            longitude = slice(lon-resolution, lon+resolution)
        ).sel(
            depth = depth,
            time = date,
            method = 'nearest'
        )
        property_values = result[properties].mean(skipna=True).to_array().to_numpy()
        return list(property_values) + [is_grid_mean, is_depth_approx]
    else:
        return nan_result


def get_properties(df: pd.DataFrame, properties: set[str] = parsers.ALL_PROPERTIES) -> pd.DataFrame:
    """
    Retrieve CMEMS properties for given locations and dates.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns: lat, lon, depth, sample_date
    properties : set[str]
        The properties for which copernicus values should be retrieved.
        Default includes all accepted values: temperature,salinity,
        nitrate,oxygen,ph,phosphate,phytoplankton

    Returns
    -------
    pd.DataFrame
        DataFrame with the requested properties as columns and an 'is_grid_mean'
        and 'is_depth_approx' column for each accessed dataset 
    """

    # Convert sample_date to date objects (remove timezone if present)
    df["sample_date"] = df['sample_date'].apply(parsers.parse_dates)
    
    # Define dataset parameters and read dataset
    dataset_static_phys, dataset_phys = _open_phys()
    dataset_static_chem, dataset_chem_daily, dataset_chem_monthly = _open_chem()

    results = pd.DataFrame(index=df.index)

    tqdm.pandas()

    params = {
        'physical': {
            'property_subset': parsers.PHYSICAL_PROPERTIES,
            'static_dataset': dataset_static_phys,
            'dataset': dataset_phys,
            'resolution': (1 / 12)
        }, 'daily_chemical': {
            'property_subset': parsers.DAILY_CHEMICAL_PROPERTIES,
            'static_dataset': dataset_static_chem,
            'dataset': dataset_chem_daily,
            'resolution': (1 / 4)
        }, 'monthly_chemical': {
            'property_subset': parsers.MONTHLY_CHEMICAL_PROPERTIES,
            'static_dataset': dataset_static_chem,
            'dataset': dataset_chem_monthly,
            'resolution': (1 / 4)
        }
    }

    if df.shape[0] == 0:
        cols = []
        for category, params_susbet in params.items():
            desired_properties = properties.intersection(params_susbet['property_subset'])
            # Put properties in fixed order
            desired_properties = [
                p for p in parsers.PROPERTY_ORDER
                if p in desired_properties
            ]
            if len(desired_properties) > 0:
                cols += desired_properties + ['is_grid_mean_' + category, 'is_depth_approx_' + category]
        return pd.DataFrame([], columns=cols)
            
    
    for category, params_susbet in params.items():
        desired_properties = properties.intersection(params_susbet['property_subset'])
        desired_properties = [
            p for p in parsers.PROPERTY_ORDER
            if p in desired_properties
        ]
        if len(desired_properties) > 0:
            cols = desired_properties + ['is_grid_mean_' + category, 'is_depth_approx_' + category]
            logger.info(f"Retrieve {category} properties")
            results[cols] = df.progress_apply(
                lambda row: _get_properties_from_dataset(
                    row, 
                    params_susbet['static_dataset'], params_susbet['dataset'], params_susbet['resolution'],
                    desired_properties
                ),
                axis=1, 
                result_type="expand",
            )


    return results