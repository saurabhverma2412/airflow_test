"""This module contains a class for spark dataset which will ignore the empty paths provided in
configuration"""
from kedro.extras.datasets.spark import SparkDataSet
from kedro.io.core import DataSetError

from spaceflights_17_4.utilities.spark_util import get_spark_empty_df


class SparkIgnoreMissingPathDataset(SparkDataSet):
    """Helper class which will ignore the empty paths provided in configuration"""

    def load(self):
        """Helper function to load the datasets"""
        try:
            return super().load()
        except DataSetError as exception:
            if "Path does not exist" in str(exception):
                return get_spark_empty_df()
            else:
                raise exception
