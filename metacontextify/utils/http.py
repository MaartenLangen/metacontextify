"""
HTTP utility functions for API requests.
"""

import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .logging import get_logger

logger = get_logger("utils.http")


def http_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
) -> requests.Response:
    """
    Wrapper around requests.get for HTTP GET requests.

    Parameters
    ----------
    url : str
        The URL to request.
    params : dict, optional
        Query parameters to include in the URL.
    headers : dict, optional
        HTTP headers to include in the request.
    timeout : int, optional
        Request timeout in seconds.

    Returns
    -------
    requests.Response
        Response object from the HTTP GET request.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP request fails.
    """
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        handle_http_error(e)
        raise


def retry_request(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    backoff_factor: float = 1.5,
) -> requests.Response:
    """
    Wrapper around http_get that retries with exponential backoff.

    Parameters
    ----------
    url : str
        The URL to request.
    params : dict, optional
        Query parameters to include in the URL.
    headers : dict, optional
        HTTP headers to include in the request.
    max_retries : int, default=3
        Maximum number of retry attempts.
    backoff_factor : float, default=1.5
        Backoff factor for exponential backoff between retries.

    Returns
    -------
    requests.Response
        Response object from the HTTP GET request.

    Raises
    ------
    requests.exceptions.RequestException
        If all retry attempts fail.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        handle_http_error(e)
        raise


def validate_json(response: requests.Response) -> Dict[str, Any]:
    """
    Validate and parse JSON response.

    Parameters
    ----------
    response : requests.Response
        Response object to validate.

    Returns
    -------
    dict
        Parsed JSON content.

    Raises
    ------
    ValueError
        If response is not valid JSON.
    """
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {e}")
        raise ValueError(f"Failed to parse JSON: {e}")


def handle_http_error(error: Exception) -> None:
    """
    Handle and log HTTP errors.

    Parameters
    ----------
    error : Exception
        The exception that occurred.
    """
    if isinstance(error, requests.exceptions.Timeout):
        logger.error(f"Request timeout: {error}")
    elif isinstance(error, requests.exceptions.ConnectionError):
        logger.error(f"Connection error: {error}")
    elif isinstance(error, requests.exceptions.HTTPError):
        logger.error(f"HTTP error: {error}")
    else:
        logger.error(f"Request error: {error}")
