__version__ = "2.0.0b2"
"""
Data Commons Client Package

This package provides a Python client for interacting with the Data Commons API.
"""

from datacommons_client.client import DataCommonsClient
from datacommons_client.endpoints.base import API
from datacommons_client.endpoints.node import NodeEndpoint
from datacommons_client.endpoints.observation import ObservationEndpoint
from datacommons_client.endpoints.resolve import ResolveEndpoint

__all__ = [
    "DataCommonsClient",
    "API",
    "NodeEndpoint",
    "ObservationEndpoint",
    "ResolveEndpoint",
]
