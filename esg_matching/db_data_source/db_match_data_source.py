""" Base class allows to create a matching datasource object that represents a matching table in a database """

import pandas as pd

from esg_matching.db_data_source.db_data_source import DbDataSource
from esg_matching.db_engine.engines.connector import DbConnector
from esg_matching.exceptions import exceptions_data_source


class DbMatchDataSource(DbDataSource):
    """
        This class provides a base structure to build a datasource object that represents a table in a database.

        Attributes:
                _std_attributes (dict): a dictionary with the names of standard attributes that must be present as
                    attributes of the datasource so that the matching processes can be performed successfully.
                    The key is a standard name of the attribute and the value is the name of the attribute in the
                    correspondent database table.
                _ds_match_type (str): indicates if the datasource is for a matching table or a no-matching table
                    values can be: 'match' or 'no-match'
    """

    __AUTO_COLUMNS = ['TIMESTAMP', 'MATCHING_ID']
    __NO_MATCHING_STD_ATTRIBUTES = {'TGT_NAME': 'TGT_NAME'}
    __MATCHING_STD_ATTRIBUTES = {'REF_NAME': 'REF_NAME',
                                 'TGT_NAME': 'TGT_NAME',
                                 'MATCHING_TYPE': 'MATCHING_TYPE',
                                 'MATCHING_SCOPE': 'MATCHING_SCOPE',
                                 'MATCHING_RULE': 'MATCHING_RULE'}

    def __init__(self, db_connector: DbConnector, name: str):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): database engines

            Returns:
                DbMatchDataSource (object)

            Raises:
                No exception is raised.
        """
        super().__init__(db_connector, name)
        self._auto_columns = self.__AUTO_COLUMNS
        self._ds_match_type = ''
        self._std_attributes = None
        self._matching_id = ''
        self._map_indirect_matching = None

    @property
    def match_type(self):
        return self._ds_match_type

    @match_type.setter
    def match_type(self, ds_match_type: str):
        if ds_match_type not in ['matching', 'no-matching']:
            raise exceptions_data_source.MatchingDataSourceTypeNotSupported
        self._ds_match_type = ds_match_type
        if ds_match_type == 'matching':
            self._std_attributes = self.__MATCHING_STD_ATTRIBUTES
        else:
            self._std_attributes = self.__NO_MATCHING_STD_ATTRIBUTES

    @property
    def matching_id(self):
        return self._matching_id

    @matching_id.setter
    def matching_id(self, matching_id: str):
        self._matching_id = matching_id

    @property
    def map_indirect_matching(self):
        return self._map_indirect_matching

    @map_indirect_matching.setter
    def map_indirect_matching(self, map_indirect_matching: str):
        self._map_indirect_matching = map_indirect_matching

    def change_std_attribute_name(self, key_std_attribute: str, new_name: str):
        """
            Class method that allows to change the name of an standard attribute in the matching data source.

            Parameters:
                key_std_attribute (str): the standard attribute key, which is also its default name
                new_name (str): a new name for the standard attribute

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._check_match_type()
        self._std_attributes[key_std_attribute] = new_name

    def get_std_attribute_name(self, key_std_attribute):
        """
            Class method that allows to get the current name of an standard attribute in the matching data source.

            Parameters:
                key_std_attribute (str): the standard attribute key.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._check_match_type()
        return self._std_attributes[key_std_attribute]

    def get_std_attribute_names(self):
        """
            Class method that allows to get all the names of the stardard attributes of the matching datasource.

            Parameters:
                No parameters required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._check_match_type()
        return list(self._std_attributes.values())

    def get_std_attribute_keys(self):
        """
            Class method that allows to get all the keys for the stardard attributes of the matching datasource.

            Parameters:
                No parameters required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._check_match_type()
        return list(self._std_attributes.keys())

    def _check_match_type(self):
        if self._ds_match_type == '':
            raise exceptions_data_source.MatchingDataSourceTypeNotDefined

    def _is_std_attributes_in_list(self, list_of_attributes: list):
        """
            Private method that checks if all standard attributes are present in the list of attributes sent as param.

            Parameters:
                list_of_attributes (list): a list of attribute names

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        for name_attribute in self._std_attributes.keys():
            if name_attribute not in list_of_attributes:
                return False
        return True

    def sync_with_db_table(self, set_original_fields=False, set_pks=False):
        """
            Overrides the method of the parent class as to guarantee that the standard attributes of the
             matching datasource are always present in the correspondent (linked) database table.

            Parameters:
                set_original_fields (bool): if the original attribute names should be the same as the attribute names
                    of the reflected database table.
                set_pks (bool): if the primary key names should be the same as the primary key names of the
                    reflected database table.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable when some standard attribute
                    expected for the matching table is not present in the correspondent database table.
        """
        # Calls the parent method
        super().sync_with_db_table(set_original_fields, set_pks)

        # Check if the match type was defined
        self._check_match_type()

        # Checks if the standard attributes can be found in the synchronized database table
        list_of_table_attribute = list(self._table_obj.columns.keys())
        if not self._is_std_attributes_in_list(list_of_table_attribute):
            self._table_obj = None
            raise exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable

    def create_table(self, table_name: str, db_columns: list):
        """
            Overrides the method that creates a table for the datasource given a list of object columns. This method
                verifies if the standard attributes of the matching table are present as a column in the list sent
                as parameter.

            Parameters:
                table_name (str): table name
                db_columns (list): a list of columns (sqlalchemy.sql.schema.Column)

            Returns:
                No return value.

            Raises:
                exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable when some standard attribute
                    expected for the matching table is not present in the correspondent database table.
        """
        # Check if the match type was defined
        self._check_match_type()

        # Because db_columns is a sqlalchemy.sql.schema.Column object, creates a list with only the column names
        column_names = []
        for column in db_columns:
            column_names.append(column.name)
        if not self._is_std_attributes_in_list(column_names):
            raise exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable

        # The verification is fine...calls the parent method
        super().create_table(table_name, db_columns)

    def create_table_from_df(self, table_name: str, df: pd.DataFrame):
        """
            Overrides the method that creates a table for the datasource given a pandas dataframe. This method
                verifies if the standard attributes of the matching table are present as a column in the DataFrame
                sent as parameter.

            Parameters:
                table_name (str): table name
                df (pd.DataFrame): pandas dataframe that contains the structure of the table to be created

            Returns:
                No return value.

            Raises:
                exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable when some standard attribute
                    expected for the matching table is not present in the correspondent database table.
        """
        # Check if the match type was defined
        self._check_match_type()

        column_names = list(df.columns)
        if not self._is_std_attributes_in_list(column_names):
            raise exceptions_data_source.StandardMatchingAttributeMissingInDatabaseTable

        # The verification is fine...calls the parent method
        super().create_table_from_df(table_name, df)
