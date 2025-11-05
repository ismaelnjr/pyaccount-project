"""
Implementações concretas de DataClient.
"""

from pyaccount.data.clients.odbc import ContabilDBClient
from pyaccount.data.clients.file import FileDataClient

__all__ = [
    "ContabilDBClient",
    "FileDataClient"
]

