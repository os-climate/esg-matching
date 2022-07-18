""" Base class allows to create a datasource object that represents a table in a database """

import pandas as pd

from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.engine.sql.dql import DqlManager
from esg_matching.engine.sql.dml import DmlManager
from esg_matching.engine.builders.table_builder import TableBuilder, ColumnBuilder
from esg_matching.exceptions import exceptions_data_source


class DbDataSource:
    """
        This class provides a base structure to build a datasource object that represents a table in a database
        to be used in a matching scenario.
    """

    def __init__(self, db_connector: DbConnector, name: str):
        self._db_connector = db_connector
        self._table_schema = None
        self._table_name = ''
        self._table_obj = None
        self._name = name
        self._primary_keys = None
        self._original_attributes_to_ds = {}
        self._matching_role = 'target'
        self._policy_name = ''
        self._matching_alias = {}
        self._map_to_matching = {}
        self._matching_policies_settings = {}
        self._dml_manager = DmlManager(db_connector)
        self._dql_manager = DqlManager(db_connector)
        self._auto_columns = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name: str):
        self._name = new_name

    @property
    def table_schema(self):
        return self._table_schema

    @table_schema.setter
    def table_schema(self, new_table_schema: str):
        self._table_schema = new_table_schema

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, new_table_name: str):
        self._table_name = new_table_name

    @property
    def table_obj(self):
        return self._table_obj

    @property
    def matching_role(self):
        return self._matching_role

    @matching_role.setter
    def matching_role(self, new_matching_role: str):
        self._matching_role = new_matching_role

    @property
    def policy_name(self):
        return self._policy_name

    @policy_name.setter
    def policy_name(self, new_policy_name: str):
        self._policy_name = new_policy_name

    @property
    def auto_columns(self):
        return self._auto_columns

    @auto_columns.setter
    def auto_columns(self, auto_columns: list):
        self._auto_columns = auto_columns

    def _set_primary_keys(self):
        """
            Class private method that takes the name of all primary keys from the metadata table and store them at the
                class property called _primary_keys (list).
        """
        self._primary_keys = [pk_column.name for pk_column in self._table_obj.primary_key.columns.values()]

    def _set_original_attributes_from_table(self):
        """
            Class private method that sets the original attribute names as the same in the metadata table.
            This method is called when a table is not created, but reflected from the database. Therefore, in order
            to mantain the integrity of the mapping attributes, it's assumed the original attribute names and the
            table attribute names are the same.

        """
        for column_name in self._table_obj.columns:
            self._original_attributes_to_ds[column_name] = column_name

    def is_valid_attribute(self, attribute_name: str):
        """
            Class method that checks if a given name exists as attribute name of the datasource table.

            Parameters:
                attribute_name (str): name to check if it is a datasource attribute in the metadata table.

            Returns:
                True when the name is indeed an attribute name of the metadata table,
                False otherwise.

        """
        for column in self._table_obj.columns:
            if column.name == attribute_name:
                return True
        return False

    def is_valid_aliases(self, aliases: list):
        """
            Class method that checks if some names are mapped as an alias for attribute names of the datasource.

            Parameters:
                aliases (list): names to check if they are mapped as alias for attribute names of the datasource.

            Returns:
                True when all names are mapped as an alias attribute name of the datasource, or
                False if at least one name fails the checking.

        """
        for alias in aliases:
            if alias not in self._matching_alias:
                return False
        return True

    def is_sync_with_db_table(self):
        """
            Class method that checks if the datasource was synchronized with the database table.

            Returns:
                True when the datasource contains the metadata table synchronized with the database, or
                False otherwise.

        """
        if self._table_obj is None:
            return False
        else:
            return True

    def get_original_attribute_names(self):
        """
            Class method that returns the attribute names associated with the original attribute header of a file that
                originated the datasource. In case of the datasource not being created from a file, the original
                attribute names are the same as the attribute names of the reflected database table.

            Returns:
                (list) with all original attribute names.

            Raises:
                exceptions_data_source.AttributesNotDefinedInDataSource when the mapping between original names and
                    attribute names are not defined.
        """
        if len(self._original_attributes_to_ds) == 0:
            raise exceptions_data_source.AttributesNotDefinedInDataSource
        return list(self._original_attributes_to_ds.keys())

    def get_attribute_names(self, remove_auto_cols=False):
        """
            Class method that returns the attribute names associated with the reflected database table.

            Returns:
                (list) with all attribute names of the reflected database table associated to the datasource.

            Raises:
                exceptions_data_source.AttributesNotDefinedInDataSource when the mapping between original names and
                    attribute names are not defined.
        """
        if len(self._original_attributes_to_ds) == 0:
            raise exceptions_data_source.AttributesNotDefinedInDataSource
        col_names = []
        if remove_auto_cols and self._auto_columns is not None:
            all_columns = list(self._original_attributes_to_ds.values())
            for col_name in all_columns:
                if col_name not in self._auto_columns:
                    col_names.append(col_name)
        else:
            col_names = list(self._original_attributes_to_ds.values())
        return col_names

    def get_primary_keys(self):
        """
            Class method that returns the primary key names associated with the reflected database table.

            Returns:
                (list) with all primary key names of the reflected database table associated to the datasource.

            Raises:
                exceptions_data_source.PrimaryKeysNotDefinedInDataSource when the primary keys are not defined.
        """
        if len(self._primary_keys) == 0:
            raise exceptions_data_source.PrimaryKeysNotDefinedInDataSource
        return self._primary_keys

    def get_table_column(self, column_name: str, label: str = ''):
        """
            Class method that returns the column object of the datasource, given its name.

            Parameters:
                column_name (str): the column name
                label (str): the column label. The label has the goal of modifying the column name, which
                    can be usefull whenever SELECT statements are created. If the label is not provided, the column
                    object won´t have a label associated to it.

            Returns:
                Column (sqlalchemy.sql.schema.Column): the column metadata object with or without a label

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        if label == '':
            column_db = self._table_obj.columns[column_name]
        else:
            column_db = self._table_obj.columns[column_name].label(label)

        return column_db

    def get_table_columns(self, column_names: list, column_alias: list = None):
        """
            Class method that returns the column object of the datasource, given an alias name.

            Parameters:
                column_names (str): column names to retrieve
                column_alias (str): alias name for the column

            Returns:
                Column (sqlalchemy.sql.schema.Column): the column metadata object

            Raises:
                exceptions_data_source.AliasNotInDataSource when the given alias is not mapped to any attribute of
                    the datasource.
        """
        columns_db = []
        if column_alias is not None:
            if len(column_names) != len(column_alias):
                raise exceptions_data_source.ColumnNamesAndColumnAliasesMustMatch
            for col_spec in zip(column_names, column_alias):
                col_db = self.get_table_column(col_spec[0]).label(col_spec[1])
                columns_db.append(col_db)
        else:
            for column_name in column_names:
                col_db = self.get_table_column(column_name)
                columns_db.append(col_db)
        return columns_db

    def get_table_column_by_alias(self, alias_name: str):
        """
            Class method that returns the column object of the datasource, given an alias name.

            Parameters:
                alias_name (str): the alias used to describe the equivalent column in the database table.

            Returns:
                Column (sqlalchemy.sql.schema.Column): the column metadata object

            Raises:
                exceptions_data_source.AliasNotInDataSource when the given alias is not mapped to any attribute of
                    the datasource.
        """
        if alias_name in self._matching_alias:
            return self._matching_alias[alias_name]
        else:
            raise exceptions_data_source.AliasNotInDataSource

    def get_matching_attribute_by_alias(self, alias_name: str):
        """
            Class method that returns the name of the attribute in the matching/no-matching table associated to
                an alias used in the datasource.

            Parameters:
                alias_name (str): the alias used to describe the equivalent column in the database table.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.OriginalNameAlreadyMapped when the given original name was already mapped
                    to a datasource attribute.
        """
        column_name = self.get_table_column_by_alias(alias_name)
        for map_attribute, map_column in self._map_to_matching.items():
            if map_column.element.name == column_name.name:
                return map_attribute
        raise exceptions_data_source.AliasNotMappedToMatching

    def map_attribute_names(self, original_name: str, attribute_name: str):
        """
            Class method that maps an original attribute name (from a header file) to an attribute name
                of the reflected database table associated to the datasource.

            Parameters:
                original_name (str): original attribute name (from a header file)
                attribute_name (str): attribute name of the reflected database table associated to the datasource.

            Raises:
                exceptions_data_source.OriginalNameAlreadyMapped when the given original name was already mapped
                    to a datasource attribute.
        """
        if original_name in self._original_attributes_to_ds:
            raise exceptions_data_source.OriginalNameAlreadyMapped
        self._original_attributes_to_ds[original_name] = attribute_name

    def map_alias_to_attribute(self, alias_name: str, attribute_name: str):
        """
            Class method that maps an alias to an attribute name of the reflected database table.

            Parameters:
                alias_name (str): short name that works as an alias for an attribute name of the database table.
                attribute_name (str): the attribute name of the database table.

            Raises:
                exceptions_data_source.AttributeNotInDataSource when the given attribute name is not an attribute of
                    the datasource.
                exceptions_data_source.AliasNameAlreadyMapped when the given alias is already mapped to an attribute
                    of the datasource.
        """
        if not self.is_valid_attribute(attribute_name):
            raise exceptions_data_source.AttributeNotInDataSource

        if alias_name in self._matching_alias:
            raise exceptions_data_source.AliasNameAlreadyMapped

        column_db = self.get_table_column(attribute_name)
        self._matching_alias[alias_name] = column_db

    def map_attribute_to_matching(self, matching_attribute_name: str, attribute_name: str):
        """
            Class method that maps an alias to an attribute name of the reflected database table.

            Parameters:
                attribute_name (str): the attribute name of the database table associate to the datasource.
                matching_attribute_name (str): the attribute name of the matching table

            Raises:
                exceptions_data_source.AttributeNotInDataSource when the given attribute name is not an attribute of
                    the datasource.
        """
        if not self.is_valid_attribute(attribute_name):
            raise exceptions_data_source.AttributeNotInDataSource

        column_db = self.get_table_column(attribute_name, label=matching_attribute_name)
        self._map_to_matching[matching_attribute_name] = column_db

    def add_policy_definition(self, policy_name: str, matching_type: str, rule_name: str, alias_list: list):
        """
            Class method that adds a matching policy rule specification to the datasource.

            Parameters:
                policy_name (str): the policy name
                matching_type (str): the matching type ('dfm', 'drm' or 'irm')
                rule_name (str): a short name for the rule
                alias_list (list): a list with aliases for attribute names

            Raises:
                exceptions_data_source.MatchingTypeInPolicyDefinitionNotSupported when the matching type is
                    different from 'dfm', 'drm' or 'irm'.
                exceptions_data_source.AliasNotInDataSource when some alias from the list is not mapped to any
                    datasource attribute.
        """
        if matching_type not in ['dfm', 'drm', 'irm']:
            raise exceptions_data_source.MatchingTypeInPolicyDefinitionNotSupported

        if not self.is_valid_aliases(alias_list):
            raise exceptions_data_source.AliasNotInDataSource

        if len(self._matching_policies_settings) == 0 or \
                policy_name not in self._matching_policies_settings:
            self._matching_policies_settings[policy_name] = {}

        if len(self._matching_policies_settings[policy_name]) == 0 or \
                matching_type not in self._matching_policies_settings[policy_name]:
            self._matching_policies_settings[policy_name][matching_type] = {}

        self._matching_policies_settings[policy_name][matching_type][rule_name] = alias_list

    def get_policy_definition(self, policy_name: str = ''):
        """
            Class method that retrieves a policy specification from the datasource.

            Parameters:
                policy_name (str): the policy name. If it is not informed, retrieves all specifications.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.MatchingTypeInPolicyDefinitionNotSupported when the matching type is
                    different from 'dfm', 'drm' or 'irm'.
                exceptions_data_source.AliasNotInDataSource when some alias from the list is not mapped to any
                    datasource attribute.
        """
        if policy_name == '':
            return self._matching_policies_settings
        else:
            if policy_name not in self._matching_policies_settings.keys():
                raise exceptions_data_source.PolicyDefinitionNotFound
            else:
                return self._matching_policies_settings[policy_name]

    def get_db_cols_with_same_name(self, common_names: list):
        """
            Class method that returns the metadata columns of the datasource that has the same name as the ones
            in a list sent by parameter. This method also guarantees that the resultant column list is sent in the
            same order that the column appears in the common_names list.

            Parameters:
                common_names (list): a list with column names that must match with the ones in the datasource

            Returns:
                No return value.

            Raises:
                exceptions_data_source.AttributesNotDefinedInDataSource when datasource attributes are not defined.
        """
        if len(self._original_attributes_to_ds) == 0:
            raise exceptions_data_source.AttributesNotDefinedInDataSource
        ds_attributes = list(self._original_attributes_to_ds.values())
        result_cols = []
        for name in common_names:
            if name in ds_attributes:
                result_cols.append(self.get_table_column(name))
        return result_cols

    def get_db_cols_mapped_to_matching(self):
        """
            Class method that returns the database table columns that are mapped to a matching table.

            Parameters:
                No parameters required.

            Returns:
                (list) of sqlalchemy.sql.schema.Column

            Raises:
                No exception is raised.
        """
        return list(self._map_to_matching.values())

    def get_name_cols_mapped_to_matching(self):
        """
            Class method that returns the attribute names mapped to the matching table.

            Parameters:
                No parameters required.

            Returns:
                (list) with all attribute names in common

            Raises:
                No exception is raised.
        """
        return list(self._map_to_matching.keys())

    def get_mapping_to_alias(self):
        """
            Class method that returns the attribute names mapped to alias.

            Parameters:
                No parameters required.

            Returns:
                (dict) of alias and its correspondent attribute name

            Raises:
                No exception is raised.
        """
        return self._matching_alias

    def get_mapping_to_matching(self):
        """
            Class method that returns the attribute names in the matching table mapped to a table attribute.

            Parameters:
                No parameters required.

            Returns:
                (dict) of matching attribute name and its correspondent table attribute

            Raises:
                No exception is raised.
        """
        map_to_matching = {}
        for match_attribute_name, ds_column in self._map_to_matching.items():
            map_to_matching[match_attribute_name] = ds_column.element
        return map_to_matching

    def get_mapping_attribute_name(self):
        """
            Class method that returns the original (file) attribute names mapped to the correspondent database
                table attribute.

            Parameters:
                No parameters required.

            Returns:
                (dict) of atrribute names

            Raises:
                No exception is raised.
        """
        return self._original_attributes_to_ds

    def sync_with_db_table(self, set_original_fields=False, set_pks=False):
        """
            Class method that updates (reflects) the metadata database table associated to the datasource.

            Parameters:
                set_original_fields (bool): if the original attribute names should be the same as the attribute names
                    of the reflected database table.
                set_pks (bool): if the primary key names should be the same as the primary key names of the
                    reflected database table.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.TableNameNotInformed when the table name of the datasource was not provided.
        """
        if self._table_name == '':
            raise exceptions_data_source.TableNameNotInformed

        # Update the metadata table object
        self._table_obj = self._db_connector.get_table_from_metadata(self._table_name, self._table_schema)
        if set_pks:
            self._set_primary_keys()
        if set_original_fields:
            self._set_original_attributes_from_table()

    def create_table(self, db_columns: list):
        """
            Class method that creates a table for the datasource given a list of object columns.

            Parameters:
                db_columns (list): a list of columns (sqlalchemy.sql.schema.Column)
        """
        table_builder = TableBuilder(self._db_connector)
        table_builder = table_builder.create_table(self._table_name, self._table_schema).add_columns(db_columns)
        table_object = table_builder.execute()
        self._table_obj = table_object
        self._set_primary_keys()

    def create_table_from_df(self, table_name: str, df: pd.DataFrame):
        """
            Class method that creates a table for the datasource given a pandas dataframe

            Parameters:
                table_name (str): table name
                df (pd.DataFrame): pandas dataframe that contains the structure of the table to be created

        """
        self._table_name = table_name
        columns_db = []
        column_builder = ColumnBuilder(self._db_connector)
        for column_name in df.columns:
            column_db = column_builder.create_column(column_name).set_type('str').build()
            columns_db.append(column_db)
        self.create_table(table_name, columns_db)
        self.sync_with_db_table(set_original_fields=True, set_pks=True)

    def drop_table(self):
        """
            Class method that drops the table associated to the datasource from the database.

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        self._db_connector.drop_table(self._table_obj)
        self._table_obj = None

    def delete_all_entries(self):
        """
            Class method that deletes all entries from the table associated to the datasource.

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        self._dml_manager.delete_all_entries(self._table_obj)

    def insert_row(self, data_row):
        """
            Class method that inserts a row into the table associated to the datasource.

            Parameters:
                data_row (): a row created as NamedTuple('FileRecord', attributes_row)

        """
        # Create the data set specification as follows:
        # Each element in the list represents a row to be inserted in the table in the following format:
        # [{'col1': 'value', 'col2': 'value'},{'col1': 'value', 'col2': 'value'}]
        row_db = {}
        for field_name in data_row._fields:
            field_idx = data_row._fields.index(field_name)
            field_value = data_row[field_idx]
            db_field_name = self._original_attributes_to_ds[field_name]
            field_value = str(field_value).strip()
            if len(field_value) == 0:
                row_db[db_field_name] = self._db_connector.get_null_value()
            else:
                row_db[db_field_name] = field_value
        print(row_db)
        self._dml_manager.insert_row(self._table_obj, row_db)

    def get_total_entries(self):
        """
            Class method that returns the total entries of the database table associated to the datasource.

            Returns:
                0 : if there is no rows
                number (int): the total rows

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        total_entries = self._dql_manager.get_total_entries(self._table_obj)
        return total_entries

    def get_total_entries_by_column(self, column_name: str, distinct_values=False):
        """
            Class method that returns the total entries of a column in the database table associated to the datasource.

            Parameters:
                No parameters required.

            Returns:
                0 : if there is no rows
                number (int): the total rows

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        column_object = self._table_obj.columns[column_name]
        total_entries = self._dql_manager.get_total_entries_by_column(column_object, distinct_values)
        return total_entries

    def get_data_as_df(self):
        """
            Class method that returns all entries of the database table associated with the datasource in the format
                of a pandas DataFrame.

            Returns:
                df_result (DataFrame): pandas dataframe representing the content of the database table

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        df_result = self._dql_manager.get_df_from_table(self._table_obj)
        return df_result

    def get_data(self):
        """
            Class method that returns all entries of the database table associated with the datasource in the format
                of a list. Each element of the list is equivalent to a row in the database table.

            Returns:
                df_result (list): list representing the content of the database table

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the datasource was not syncronized
                    with its equivalent database table object. See sync_with_db_table() method.
        """
        if self._table_obj is None:
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable

        lst_result = self._dql_manager.query_table(self._table_obj)
        return lst_result
