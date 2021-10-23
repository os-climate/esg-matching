""" This module defines the class EntitySqlDB """

# Import python libraries


# Import internal libraries
from esgmatching.entity.EntityDataSource import EntityDataSource
from esgmatching.dbmanager.SqlEngine import SqlEngine
from esgmatching.dbmanager import SqlUtility
from esgmatching.exceptions import NotConnectedToDatabase, ValueErrorDatabaseEngine, ErrorDB, \
    EntityNotLinkedToDB, IdxColumnIsMissing
from esgmatching.cleaner import company, country, banking_ids


class EntityDB(EntityDataSource):
    """
        Class to manage the extraction, transformation and loading of data from a csv file.
    """

    def __init__(self, *args):
        """
            Constructor method.

            Parameters:
                file_map: complete file path and name of the mapping json file that describes the attributes
                or metadata for the data source.

            Returns:
                EntitySqlDB (object)
            Raises:
                AttributeError (from super-class): if the mapping file does not exists.
        """
        super().__init__(*args)

        # Check if the first parameter is a table object
        if SqlUtility.is_table_object(args[0]):
            self._table_obj = args[0]
            self._table_name = self._table_obj.name
        else:
            self._table_name = self._name
            self._table_obj = None
        self._db_engine = None

    @staticmethod
    def __is_enginedb_valid(sql_engine_obj):
        if type(sql_engine_obj) is SqlEngine:
            if sql_engine_obj.is_connected():
                return True
            else:
                raise NotConnectedToDatabase
        else:
            raise ValueErrorDatabaseEngine

    @property
    def table_name(self):
        """
             Class property that defines the correspondent table name for the data source.

             Parameters:

             Returns:
                 The table name (string) for the data source.
             Raises:
                 No exception is raised.
         """
        return self._table_name

    @property
    def table_obj(self):
        """
             Class property that defines the table object, retrieved from a database, for the data source.

             Parameters:

             Returns:
                 The table object (sqlalchemy.sql.schema.Table) for the data source.
             Raises:
                 No exception is raised.
         """
        return self._table_obj

    @table_obj.setter
    def table_obj(self, table_obj):
        """
            Setter for the class property that defines the table object.

            Parameters:
               table_obj (sqlalchemy.sql.schema.Table): the table metadata object

            Returns:

            Raises:
                No exception is raised.
        """
        self._table_obj = table_obj

    def set_database_engine(self, sql_engine_obj):
        """
            Class method that allows the injection of an object engine, as to perform operations in a database.

            Parameters:
                sql_engine_obj (SqlEngine): the object engine connected to a database.

            Returns:
                No return value.

            Raises:
                ValueErrorDatabaseEngine: if the object engine is not a SqlEngine object or is None.
                NotConnectedToDatabase: if the object engine is not connected to the database.
        """

        if self.__is_enginedb_valid(sql_engine_obj):
            self._db_engine = sql_engine_obj

    def get_table_column(self, column_name):
        """
            Class method that returns the column object of the data source, given a column name.

            Parameters:
                column_name (string): the column name

            Returns:
                Column (sqlalchemy.sql.schema.Column): the column metadata object

            Raises:
                No exception is raised.
        """
        if self._table_obj is not None:
            return self._table_obj.columns[column_name]
        else:
            return None

    def get_table_column_names(self):
        if self._table_obj is not None:
            return self._table_obj.columns.keys()
        else:
            raise EntityNotLinkedToDB

    def get_table_columns_from_renamed_attributes(self, prefix_alias=''):
        """
            Class method that returns a dictionary of column objects of the data source.

            Parameters:
                prefix_alias (string): if the column name needs to be renamed, this parameter is used as to add
                a prefix alias to the original column name.

            Returns:
                Dictionary of Columns (sqlalchemy.sql.schema.Column), in which the key is the column name
                and the value is the Column object.

            Raises:
                No exception is raised.
        """
        if self._table_obj is not None:
            table_columns = {}
            # Get the data source column names
            attributes = self.get_renamed_attribute_names()
            for attribute in attributes:
                # Apply the suffix if necessary
                if prefix_alias == '':
                    attribute_name = attribute
                else:
                    attribute_name = prefix_alias + attribute
                table_columns[attribute_name] = self.get_table_column(attribute)

            return table_columns
        else:
            return None

    def get_table_columns_from_db(self, remove_idx=True, remove_timestamp=True, prefix_alias='',
                                  not_with_prefix='', with_prefix=''):
        if self._table_obj is not None:
            table_columns = {}
            # Get the data source column names
            for column in self._table_obj.columns:
                if remove_idx and column.name == 'idx':
                    continue
                if remove_timestamp and column.name == 'timestamp':
                    continue
                if len(not_with_prefix) > 0:
                    if not_with_prefix in column.name:
                        continue
                if len(with_prefix) > 0:
                    if with_prefix not in column.name:
                        continue
                # Apply the suffix if necessary
                if prefix_alias == '':
                    column_name = column.name
                else:
                    column_name = prefix_alias + column.name
                table_columns[column_name] = column

            return table_columns
        else:
            return None

    def are_column_names_valid(self, column_names):
        """
            Method used to compare the columns names sent by parameter with the attributes of the EntityDB, that
            were read from a mapping file.

            Parameters:
                column_names (list): columns names to be checked against the attribute names in the EntityDB

            Returns:
                True or False to indicate if the column names are valid.

            Raises:
                No exception is raised.
        """

        # Get the name of attributes
        attributes_to_read = self.get_original_attribute_names()
        # Check if these names against the column_names parameter
        for attribute in attributes_to_read:
            if attribute not in column_names:
                return False
        return True

    def create_data_source_in_db(self):
        """
            Private class method that creates a database table, given an entity with its metadata information.

            Parameters:

            Returns: True or False if the operation succeeds or fails.

            Raises:
                No exception is raised.
        """

        # Get the attributes from the metadata in the entity data source
        ds_attributes = self.get_renamed_attribute_names()

        # Get the patterns to recognize the attributes to be validated or cleaned
        pattern_company_name = company.get_pattern_name_to_clean()
        pattern_country_name = country.get_pattern_country_to_clean()
        pattern_ids = banking_ids.get_ids_to_validate()

        # Defines the column names and types for the new table
        table_columns = []
        types_columns = []

        # Get db type object for string and boolean content
        db_type_str = SqlUtility.get_db_type('str')
        db_type_bol = SqlUtility.get_db_type('bol')

        # Add default column that describes the data source
        table_columns.append('data_source')
        types_columns.append(db_type_str)
        for attribute in ds_attributes:
            table_columns.append(attribute)
            types_columns.append(db_type_str)
            # Create specific attributes for the cleaning process
            if self._use_cleaner:
                if self._keep_original_data and attribute in pattern_company_name:
                    table_columns.append('original_' + attribute)
                    types_columns.append(db_type_str)
                elif attribute in pattern_ids:
                    table_columns.append('isvalid_' + attribute)
                    types_columns.append(db_type_bol)
                elif attribute in pattern_country_name:
                    table_columns.append('country_alpha2')
                    types_columns.append(db_type_str)
                    table_columns.append('country_alpha3')
                    types_columns.append(db_type_str)
                    if self._keep_original_data:
                        table_columns.append('original_' + attribute)
                        types_columns.append(db_type_str)

        try:
            # Create the table in the database
            new_table = self._db_engine.create_table(self._table_name, table_columns, types_columns)

            # Injects the database metadata into the entity data source for future uses
            self._table_obj = new_table

        except Exception as e:
            # Customize the error message
            original_msg = getattr(e, 'message', repr(e))
            exception_msg = original_msg + ' - ' + 'Error creating table [{table_name}].'.format(table_name=self._table_name)
            raise ErrorDB(exception_msg)

    def convert_to_db_row(self, row_data, rename_row_data=True):
        """
            Private class method that returns the pairs of columns and values given an attribute name and
            an attribute value read from the csv file. This function transform an attribute from a csv file
            to a database attribute. For this, it performs validation and cleaning when requested/necessary.
            Also, it is important to realize that some attributes in a csv file may generate multiples attributes
            in the database. For instance, country information from csv which generates three database attributes:
            (country name, alpha2 and alpha3 code.). That's why the returning type for this function is a dictionary
            with all the pairs (attribute names and value) ready to be persisted into a database table.

            Parameters:
                row_data (dictionary): data representing a row to be inserted, in which the key is an attribute name
                and the value is the column value.
                rename_row_data(bool): indicates if the attribute names must be changed to a renamed attribute name

            Returns:
                dict_db_rows (dictionary): a dictionary with the pairs - attribute + value ready to be
                persisted into a database table.

            Raises:
                No exception is raised.
        """
        dict_db_rows = {}

        pattern_company_name = company.get_pattern_name_to_clean()
        pattern_country_name = country.get_pattern_country_to_clean()
        pattern_ids = banking_ids.get_ids_to_validate()

        for attribute_name in row_data:
            if rename_row_data:
                name_column = self.get_renamed_attribute(attribute_name)
            else:
                name_column = attribute_name
            if self._use_cleaner:
                if name_column in pattern_company_name:
                    # Adds the company's name and value (cleaned and original)
                    clean_name = company.get_clean_name(row_data[attribute_name])
                    value_db = SqlUtility.get_value_db(clean_name)
                    dict_db_rows[name_column] = value_db
                    if self._keep_original_data:
                        value_db = SqlUtility.get_value_db(row_data[attribute_name])
                        dict_db_rows['original_' + name_column] = value_db
                elif name_column in pattern_ids:
                    value_db = SqlUtility.get_value_db(row_data[attribute_name])
                    dict_db_rows[name_column] = value_db
                    is_valid_id = banking_ids.validate_id(row_data[attribute_name], name_column)
                    if is_valid_id is None:
                        # Force Null value
                        value_db = SqlUtility.get_value_db('')
                    else:
                        # Get the True/False value
                        value_db = is_valid_id
                    dict_db_rows['isvalid_' + name_column] = value_db
                elif name_column in pattern_country_name:
                    dict_country = country.get_country_info(row_data[attribute_name])
                    if dict_country is None:
                        dict_db_rows[name_column] = SqlUtility.get_value_db('')
                        dict_db_rows['country_alpha2'] = SqlUtility.get_value_db('')
                        dict_db_rows['country_alpha3'] = SqlUtility.get_value_db('')
                    else:
                        dict_db_rows[name_column] = dict_country['country_name']
                        dict_db_rows['country_alpha2'] = dict_country['country_alpha2']
                        dict_db_rows['country_alpha3'] = dict_country['country_alpha3']
                    if self._keep_original_data:
                        value_db = SqlUtility.get_value_db(row_data[attribute_name])
                        dict_db_rows['original_' + name_column] = value_db
                else:
                    dict_db_rows[name_column] = SqlUtility.get_value_db(row_data[attribute_name])
            else:
                dict_db_rows[name_column] = SqlUtility.get_value_db(row_data[attribute_name])
        # Add default value to the data source column
        dict_db_rows['data_source'] = self._table_name
        return dict_db_rows

    def get_data_as_df(self):
        if EntityDB.__is_enginedb_valid(self._db_engine):
            df_result = self._db_engine.get_df_from_table(self._table_obj)
            return df_result

    def get_data(self):
        if EntityDB.__is_enginedb_valid(self._db_engine):
            lst_result = self._db_engine.query_table(self._table_obj)
            return lst_result

    def get_total_entries_by_column_name(self, column_name):
        if EntityDB.__is_enginedb_valid(self._db_engine):
            if self._table_obj is not None:
                column_object = self._table_obj.columns[column_name]
                result = self._db_engine.get_count_by_column(column_object)
                return result
            else:
                raise EntityNotLinkedToDB

    def get_total_entries_by_idx(self):
        column_names = self.get_table_column_names()
        if 'idx' not in column_names:
            raise IdxColumnIsMissing
        result = self.get_total_entries_by_column_name(column_name='idx')
        return result
