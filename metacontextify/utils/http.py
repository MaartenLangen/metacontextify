"""HTTP utility functions for API requests.

Provides helpers for retrying GET requests, validating JSON responses,
and translating HTTP errors into local exceptions.
"""

import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .logging import get_logger

logger = get_logger("utils.http")

RequestException = requests.exceptions.RequestException


def retry_request(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    backoff_factor: float = 1.5,
) -> requests.Response:
    """Perform a GET request with retries and exponential backoff.

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

    response = session.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response
        


def validate_json(response: requests.Response) -> Dict[str, Any]:
    """Validate and parse a JSON response.

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
