"""Mutex implementation for distributed systems.
"""
from ._exceptions import AlreadyAcquiredError
from ._gcs import GCS


__version__ = '0.1.0'
__all__ = ['AlreadyAcquiredError', 'GCS']
