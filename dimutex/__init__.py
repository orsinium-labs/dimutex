"""Mutex implementation for distributed systems.
"""
from ._exceptions import AlreadyAcquiredError, AlreadyReleasedError
from ._gcs import GCS


__version__ = '1.0.4'
__all__ = [
    'AlreadyAcquiredError',
    'AlreadyReleasedError',
    'GCS',
]
