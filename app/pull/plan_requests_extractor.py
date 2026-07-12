"""
Extractor for plan requests CSV data.
"""

from .base_extractor import BaseExtractor
from typing import Union, Iterable


class PlanRequestsExtractor(BaseExtractor):
    """
    Extracts plan requests data from CSV.
    """

    def extract(self, chunksize: int = None):
        """
        Extract plan requests data.

        Args:
            chunksize: If specified, return iterator of DataFrames of given size; else return full DataFrame

        Returns:
            pandas DataFrame or iterator of DataFrames
        """
        return super().extract('plan_requests.csv', chunksize=chunksize)

    @classmethod
    def extract_classmethod(cls, chunksize: int = None):
        """
        Class method shortcut for extraction.

        Args:
            chunksize: If specified, return iterator of DataFrames; else return full DataFrame

        Returns:
            pandas DataFrame or iterator of DataFrames
        """
        extractor = cls()
        return extractor.extract(chunksize=chunksize)


def extract(chunksize: int = None):
    """
    Convenience function that extracts plan requests data using default raw directory.

    Args:
        chunksize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        pandas DataFrame or iterator of DataFrames
    """
    return PlanRequestsExtractor().extract(chunksize=chunksize)