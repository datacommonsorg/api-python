__version__ = "2.1.6"
"""
Data Commons Client Package

This package provides a Python client for interacting with the Data Commons API.
"""

from datacommons_client.client import DataCommonsClient
from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.resolve import ResolveEndpoint
from datacommons_client.endpoints.sdmx import ApiLayout
from datacommons_client.endpoints.sdmx import build_query_params
from datacommons_client.endpoints.sdmx import parse_filters
from datacommons_client.endpoints.sdmx import SdmxClient
from datacommons_client.endpoints.sdmx import SdmxEndpoint
from datacommons_client.utils.context import use_api_key
from datacommons_client.utils.error_handling import SdmxAPIError
from datacommons_client.utils.error_handling import SdmxClientError

__all__ = [
    "DataCommonsClient",
    "API",
    "NodeEndpoint",
    "ObservationEndpoint",
    "ResolveEndpoint",
    "SdmxEndpoint",
    "SdmxClient",
    "ApiLayout",
    "SdmxClientError",
    "SdmxAPIError",
    "parse_filters",
    "build_query_params",
    "use_api_key",
]
