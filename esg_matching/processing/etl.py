""" Base class allows to create an ETL process object"""

from esg_matching.file_reader.file import File
from esg_matching.file_reader.csv_reader import FileReader
from esg_matching.exceptions import exceptions_file

from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.data_source.db_source import DbDataSource
from esg_matching.data_source.db_match import DbMatchDataSource
from esg_matching.engine.builders.table_builder import ColumnBuilder

from esg_matching.report.report_manager import ReportManager


class EtlProcessing:
    """
        This base class provides the infrastructure needed to create an ETL process object that reads the content
            of a file and store the data in a database table.

        Attributes:
            _db_connector (DbConnector): database engine
            _file (File): a file object to read from
            _file_reader (FileReader): a file reader that knows how to read the given file
            _data_source (DbDataSource): a datasource object created during the ETL process and that represents
                an equivalent database table.
    """

    __SUPPORTED_FILES = ['.csv']

    def __init__(self, db_connector: DbConnector):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): database connectors

            Returns:
                EtlProcessing (object)

            Raises:
                No exception is raised.
        """
        self._db_connector = db_connector
        self._file = None
        self._file_reader = None
        self._data_source = None

    def _update_data_source_mappings(self):
        """
            Private class method that udpdates the datasource mappings to the matching/no-matching tables and
                macthing aliases.

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        # Update the aliases mapping
        map_dict = self._file.settings.matching_alias
        if map_dict is not None:
            for alias_name, attribute_name in map_dict.items():
                self._data_source.map_alias_to_attribute(alias_name, attribute_name)

        # Update the mapping to the matching table
        map_dict = self._file.settings.map_to_matching
        if map_dict is not None:
            for match_attribute_name, ds_attribute_name in map_dict.items():
                self._data_source.map_attribute_to_matching(match_attribute_name, ds_attribute_name)

    def _add_data_source_policies(self):
        """
            Private class method that adds the policies from a json file into the datasource

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        # Policies are only available for target datasources
        if self._data_source.matching_role == 'target':
            for policy_name in self._file.settings.matching_policy:
                for matching_type in self._file.settings.matching_policy[policy_name]:
                    for rule_name in self._file.settings.matching_policy[policy_name][matching_type]:
                        alias_list = self._file.settings.matching_policy[policy_name][matching_type][rule_name]
                        self._data_source.add_policy_definition(policy_name, matching_type, rule_name, alias_list)

    def _create_db_source(self):
        """
            Private class method that creates a datasource object to reflect a database table.

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """

        # Creates a datasource object
        if self._file.settings.matching_role in ['target', 'referential']:
            self._data_source = DbDataSource(self._db_connector, self._file.settings.datasource_name)
        elif self._file.settings.matching_role in ['matching', 'no-matching']:
            self._data_source = DbMatchDataSource(self._db_connector, self._file.settings.datasource_name)
            self._data_source.match_type = self._file.settings.matching_role
            self._data_source.matching_id = self._file.settings.matching_id
            self._data_source.map_indirect_matching = self._file.settings.map_indirect_matching
        self._data_source.table_schema = self._file.settings.datasource_table_schema
        self._data_source.table_name = self._file.settings.datasource_table_name
        self._data_source.matching_role = self._file.settings.matching_role
        self._data_source.policy_name = self._file.settings.policy_name

        # If table exists
        if self._db_connector.table_exists(self._data_source.table_name, self._data_source.table_schema):
            self._data_source.sync_with_db_table()
            # ...drop it
            if self._file.settings.datasource_if_table_exists == 'drop':
                self._data_source.drop_table()
            # ... clean it
            if self._file.settings.datasource_if_table_exists == 'clean':
                self._data_source.delete_all_entries()

        # Create table or get the metadata from an existent table
        if self._file.settings.datasource_create_table:
            self._create_data_source_table()
        else:
            if self._file.settings.datasource_attributes is not None:
                self._data_source.sync_with_db_table(set_original_fields=False, set_pks=True)
                for original_name, items in self._file.settings.datasource_attributes.items():
                    db_column_name = items[0]
                    self._data_source.map_attribute_names(original_name, db_column_name)
            else:
                self._data_source.sync_with_db_table(set_original_fields=True, set_pks=True)

    def _create_data_source_table(self):
        """
            Private class method that creates a database table.

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """

        if self._file.settings.datasource_attributes is None:
            columns_data_source = self._get_standard_columns()
        else:
            columns_data_source = self._get_columns_from_settings()

        self._data_source.create_table(db_columns=columns_data_source)

    def _get_columns_from_settings(self):
        """
            Private class method that creates a database columns from file settings.

            Parameters:
                No parameter required.

            Returns:
                columns_data_source (list): list of sqlalchemy.sql.schema.Column

            Raises:
                No exception is raised.
        """
        column_builder_obj = ColumnBuilder(self._db_connector)
        columns_data_source = []
        for original_name, items in self._file.settings.datasource_attributes.items():
            db_field_settings = tuple(items)
            db_name = ''
            db_type = 'str'
            db_size = 0
            if len(db_field_settings) >= 1:
                db_name = db_field_settings[0]
            if len(db_field_settings) >= 2:
                db_type = db_field_settings[1]
            if len(db_field_settings) >= 3:
                db_size = int(db_field_settings[2])
            column_builder_obj = column_builder_obj.create_column(db_name)
            if db_type == 'auto-timestamp':
                column_builder_obj = column_builder_obj.is_auto_timestamp(True)
            elif db_type == 'auto-id':
                column_builder_obj = column_builder_obj.is_auto_primary_key(True)
            elif self._file.settings.datasource_primary_keys is not None and \
                    db_name in self._file.settings.datasource_primary_keys:
                column_builder_obj = column_builder_obj.set_type(db_type, db_size).is_primary_key(True)
            else:
                column_builder_obj = column_builder_obj.set_type(db_type, db_size)
            column_db = column_builder_obj.build()
            columns_data_source.append(column_db)
            self._data_source.map_attribute_names(original_name, db_name)
        return columns_data_source

    def _get_standard_columns(self):
        """
            Private class method that creates standard database columns from file settings.

            Parameters:
                No parameter required.

            Returns:
                columns_data_source (list): list of sqlalchemy.sql.schema.Column

            Raises:
                No exception is raised.
        """
        columns_data_source = []
        columns_file = self._file_reader.read_file_header_columns(self._file.filename)
        column_builder_obj = ColumnBuilder(self._db_connector)
        for column_name in columns_file:
            column_builder_obj = column_builder_obj.create_column(column_name).set_type('str')
            column_db = column_builder_obj.build()
            columns_data_source.append(column_db)
            self._data_source.map_attribute_names(column_name, column_name)
        return columns_data_source

    def _process_csv_file_bulk_sql(self):
        """
            Private class method that reads the csv file and insert the row to the correspondent database table

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """

        # Set up the encoding and separator
        self._file_reader.encoding = self._file.settings.file_encoding
        self._file_reader.separator = self._file.settings.file_separator

        # Set the fields to read from file
        self._file_reader.attributes_to_read = self._data_source.get_original_attribute_names()

        # Read the file, row by row
        for file_row in self._file_reader.read_file(self._file.filename):
            self._data_source.insert_row(file_row)

    def _process_csv_file_bulk_pd(self):
        """
            Private class method that reads the csv file and insert the row to the correspondent database table

            Parameters:
                No parameter required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """

        # Set up the encoding and separator
        self._file_reader.encoding = self._file.settings.file_encoding
        self._file_reader.separator = self._file.settings.file_separator

        # Set the fields to read from file
        self._file_reader.attributes_to_read = self._data_source.get_original_attribute_names()
        self._file_reader.renamed_attributes = self._data_source.get_attribute_names()

        # Read the file
        self._file_reader.read_file_with_pd(self._file.filename, self._data_source.table_name, self._db_connector)

    def load_file_to_db(self, file: File, file_reader: FileReader):
        """
            Class method that reads the content of a file.

            Parameters:
                file (File): a file object to read from
                file_reader (FileReader): a file reader that knows how to read the given file

            Returns:
                _data_source (DbDataSource): a datasource object created during the ETL process and that represents
                an equivalent database table.

            Raises:
                exceptions_file.FileTypeNotSupported when the file type is not supported
        """
        # Check the file and file_reader
        self._file = file
        self._file_reader = file_reader
        if self._file.extension not in self.__SUPPORTED_FILES:
            raise exceptions_file.FileTypeNotSupportedByETL
        if self._file.extension != self._file_reader.extension_supported:
            raise exceptions_file.FileTypeNotSupportedByReader

        # Create a datasource object from file settings
        self._create_db_source()

        # Process the file, by reading it line by line and performing bulk sql insert for speed
        if self._file.extension == '.csv' and self._file.settings.read_mode == 'bulk-sql':
            self._process_csv_file_bulk_sql()

        # Process the file, by reading it using pandas dataframe in a bulk setup for speed
        if self._file.extension == '.csv' and self._file.settings.read_mode == 'bulk-pd':
            self._process_csv_file_bulk_pd()

        # Update datasource mappings
        self._update_data_source_mappings()

        # Add policies to the datasource
        self._add_data_source_policies()

        return self._data_source

    def create_data_source(self, file: File):
        """
            Class method that creates the data source as a database table, but does not insert data on it.
            This method can be used to create the matching and no-matching tables.

            Parameters:
                file (File): a file object to read from

            Returns:
                _data_source (DbDataSource): a datasource object created during the ETL process and that represents
                an equivalent database table.

            Raises:
                exceptions_file.FileTypeNotSupported when the file type is not supported
        """

        # Set the file object
        self._file = file

        # Create a datasource object from file settings
        self._create_db_source()

        return self._data_source

    def print_report(self):
        """
            Class method that prints out information about the result of the ETL process.

        """

        # Create the report object to keep information about the reading process
        report_name = ' ETL Processing Report '
        report_desc = 'Details of the ETL process performed on [' + self._data_source.name + '] data source.'
        report_obj = ReportManager(report_name, report_desc)
        report_obj.add_content('File Name', self._file.filename)
        report_obj.add_content('Columns in the File', str(self._file_reader.total_attributes))
        report_obj.add_content('Columns read from File', str(self._file_reader.total_attributes_read))
        report_obj.add_content('Lines Extracted from File', str(self._file_reader.total_lines))
        report_obj.print_report()
