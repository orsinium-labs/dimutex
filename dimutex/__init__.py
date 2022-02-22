"""Mutex implementation for distributed systems.
"""
from ._exceptions import AlreadyAcquiredError, AlreadyReleasedError
from ._gcs import GCS


__version__ = '1.3.0'
__all__ = [
    'AlreadyAcquiredError',
    'AlreadyReleasedError',
    'GCS',
]
