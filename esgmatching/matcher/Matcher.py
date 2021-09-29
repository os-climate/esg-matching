""" This module defines the class Matcher """

# Import python libraries
from abc import ABC, abstractmethod


class Matcher(ABC):
    """
        This base class allows the creation of Matcher objects as to implement different types of matching
        strategies.
    """

    def __init__(self, matching_name, ref_data_source):
        """
            Constructor method.

            Parameters:
                matching_name (string): a descriptive name for the matching to be performed
                ref_data_source (EntityDataSource): data source object used as reference

            Returns:
                Matcher (object)
            Raises:
                No exception is raised.
        """
        # Create the instance attributes
        self._name = matching_name
        self._ref_data_source = ref_data_source
        self._target_data_sources = []
        self._matching_keys = []

    @property
    def matching_keys(self):
        """
            Class property that defines the keys used in the matching

            Parameters:

            Returns:
                _matching_keys (list): a list with the keys used for matching
            Raises:
                No exception is raised.
        """
        return self._matching_keys

    @matching_keys.setter
    def matching_keys(self, matching_keys):
        """
            Setter for the class property that defines a flag used to identify if cleaning methods must be applied
             to column values when reading a file.

            Parameters:
                matching_keys (list): a list with the keys used for matching

            Returns:

            Raises:
                No exception is raised.
        """
        self._matching_keys = matching_keys

    def add_target_data_sources(self, target_data_source):
        """
            Class method that adds target data sources to be matched with the referential data source.

            Parameters:
                target_data_source (EntityDataSource): data source object used as target

            Returns:

            Raises:
                No exception is raised.
        """
        self._target_data_sources.append(target_data_source)

    @abstractmethod
    def execute_matching(self):
        pass