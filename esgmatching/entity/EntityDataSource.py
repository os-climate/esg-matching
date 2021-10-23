""" This module defines the abstract class EntityDataSource """

# Import python libraries
from abc import ABC, abstractmethod
import os.path
import json

# Import internal libraries
from esgmatching.exceptions import MappingFileDoesNotExist, IllFormedMappingFile


class EntityDataSource(ABC):
    """
        This base class allows the creation of Entity objects as to manage the metadata of different Data Sources.
    """

    def __init__(self, *args):
        """
            Constructor method.

            Parameters:
                *args: allows the implementation of multiple construtors on the subclasses. By default,
                    the super class expects to receive one paramter which represents the folder and name of the
                    mapping/metadata file that describes the attributes of the data source.

            Returns:
                EntityDataSource (object)
            Raises:
                AttributeError: if the mapping file does not exists.
        """
        # Create the instance attributes
        self._name = ''
        self._dict_attributes = {}
        self._keys = []
        self._use_cleaner = False
        self._keep_original_data = False
        self._other_keys = None

        # Check if the first parameter is a string and, therefore, it's a metadata file
        if isinstance(args[0], str):
            # If the metadata file exists, try to unpack it into instance attributes
            if os.path.exists(args[0]):
                self._filemap = args[0]
                self.unpack_mapping_file()
            else:
                raise MappingFileDoesNotExist

    @property
    def dict_attributes(self):
        """
            Class property that defines the metadata used as keys to query the data source

            Parameters:

            Returns:
                keys (list): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._dict_attributes

    @dict_attributes.setter
    def dict_attributes(self, dict_attributes):
        """
            Setter for the class property that defines a flag used to identify if cleaning methods must be applied
             to column values when reading a file.

            Parameters:
                dict_attributes (dict): True or False value

            Returns:

            Raises:
                No exception is raised.
        """
        self._dict_attributes = dict_attributes

    @property
    def keys(self):
        """
            Class property that defines the metadata used as keys to query the data source

            Parameters:

            Returns:
                keys (list): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._keys

    @property
    def other_keys(self):
        """
            Class property that defines the metadata used as complementary keys

            Parameters:

            Returns:
                other_keys (list): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._other_keys

    @property
    def use_cleaner(self):
        """
            Class property that defines a flag used to identify if cleaning methods must be applied to columns values
            when reading a file.

            Parameters:

            Returns:
                use_cleaner(boolean): True or False value

            Raises:
                No exception is raised.
        """
        return self._use_cleaner

    @use_cleaner.setter
    def use_cleaner(self, use_cleaner):
        """
            Setter for the class property that defines a flag used to identify if cleaning methods must be applied
             to column values when reading a file.

            Parameters:
                use_cleaner (boolean): True or False value

            Returns:

            Raises:
                No exception is raised.
        """
        self._use_cleaner = use_cleaner

    @property
    def keep_original_data(self):
        """
            Class property that defines a flag used to identify if the original data must be kept as another column
            after performing the cleaning process.

            Parameters:

            Returns:
                keep_original_data(boolean): True or False value

            Raises:
                No exception is raised.
        """
        return self._keep_original_data

    @property
    def name(self):
        """
            Class property that defines the name of the EntityDataSource.

            Parameters:

            Returns:
                _name(string): name of the EntityDataSource

            Raises:
                No exception is raised.
        """
        return self._name

    @keep_original_data.setter
    def keep_original_data(self, keep_original_data):
        """
            Setter for the class property that defines a flag used to identify if the original data must be kept
            as another column after performing the cleaning process.

            Parameters:
                keep_original_data (boolean): True or False value

            Returns:

            Raises:
                No exception is raised.
        """
        self._keep_original_data = keep_original_data

    @abstractmethod
    def get_data(self):
        """
            Abstract method to get the content of the data source.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """
        pass

    def unpack_mapping_file(self):
        """
            Class method that reads the content of the json mapping file and store it as class attributes.

            Parameters:

            Returns:

            Raises:
                AttributeError: if one of the expected json keys (names, keys and attributes) is not found.
        """

        # Reads the json mapping file
        with open(self._filemap) as json_file:
            mapping = json.load(json_file)

        # Initialize the class attributes with the json content
        # These metadata information are mandatory
        if 'name' in mapping and 'matching_keys' in mapping and 'attributes' in mapping:
            self._name = mapping['name']
            self._keys = mapping['matching_keys']
            self._dict_attributes = mapping['attributes']
        else:
            raise IllFormedMappingFile

        # Other keys maybe passed as to feed complementary process, such as indirect matching
        if 'other_keys' in mapping:
            self._other_keys = mapping['other_keys']

        # Check if cleaning attributes were informed
        if 'cleaner' in mapping:
            dict_cleaner = mapping['cleaner']
            if 'useCleaner' in dict_cleaner.keys():
                self._use_cleaner = eval(dict_cleaner['useCleaner'].capitalize())
            if 'keepOriginalData' in dict_cleaner.keys():
                self._keep_original_data = eval(dict_cleaner['keepOriginalData'].capitalize())

    def get_original_attribute_names(self):
        """
            Class method that returns the original attribute names of the data source.

            Parameters:

            Returns:
                a list with the original attribute names of the data source.

            Raises:

        """
        return list(self._dict_attributes.keys())

    def get_renamed_attribute_names(self):
        """
            Class method that returns the standardized attribute names of the data source.
            These are the names used internally to manipulate the data source.

            Parameters:

            Returns:
                a list with the standardized attribute names of the data source.

            Raises:

        """
        return list(self._dict_attributes.values())

    def set_renamed_attribute_names(self, dict_attributes):
        """
            Class method that sets the standardized attribute names of the data source.
            These are the names used internally to manipulate the data source.

            Parameters:

            Returns:
                a list with the standardized attribute names of the data source.

            Raises:

        """
        self._dict_attributes = dict_attributes

    def get_renamed_attribute(self, original_name):
        """
            Class method that searches for a specific attribute in the data source, given its original name,
            and returns its standardized name.

            Parameters:

            Returns:
                a string with the standardized attribute name found in the data source.

            Raises:

        """
        return self._dict_attributes[original_name]

    def get_matching_keys(self, keys_to_check):
        """
            Class method that compares the keys of the data source with a list of keys sent by parameter and
            return the values that are common.

            Parameters:
                keys_to_check (list): a list of keys to be compared.

            Returns:
                a list of keys in common.

            Raises:

        """
        matching_keys = [value for value in self._keys if value in keys_to_check]
        return matching_keys
