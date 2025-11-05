"""
Data module - Acesso a dados.
"""

from pyaccount.data.client import DataClient
from pyaccount.data.clients import ContabilDBClient, FileDataClient

__all__ = [
    "DataClient",
    "ContabilDBClient",
    "FileDataClient"
]

