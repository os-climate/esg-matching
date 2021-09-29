""" This module defines the class FileReaderCsvToDB """

# Import python libraries
from csv import DictReader

# Import internal libraries
from esgmatching.reader import FileReader
from esgmatching.entity.EntityDB import EntityDB
from esgmatching.dbmanager.SqlEngine import SqlEngine
from esgmatching.report.ReportManager import ReportManager
from esgmatching.exceptions import NoHeaderInFile, ColumnsDifferFromMapping, ErrorDB


class FileReaderCsvToDB(FileReader.FileReader):
    """
        Class to manage the extraction, transformation and loading of data from a csv file.
        This class makes use of the SqlEngine to persit data into a table in a database. The EntityDB object
        is used as a data structure that not only prepares the file content to be persisted, but also maintain
        the metadata information that links the csv file attributes and its corresponding counterpart in a
        table object in a database.

        Attributes:
            _db_engine (SqlEngine): database connection object.
            _use_session (bool): flag that indicates if a session object must be used.
    """

    def __init__(self):
        """
            Constructor method for FileReaderCsvToDB object.

            Parameters:
                No parameters provided.

            Returns:
                FileReaderCsvToDB (object)

            Raises:
                No exception is raised.
        """
        super().__init__()
        self._db_engine = None
        self._use_session = False

    def set_database_engine(self, sql_engine_obj, use_session=False):
        """
            Class method that allows the injection of an object engine, as to perform operations in a database.

            Parameters:
                sql_engine_obj (SqlEngine): database connection object..
                use_session (bool): flag that indicates if a session object must be used.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._db_engine = sql_engine_obj
        self._use_session = use_session

    def read_file(self, file_path, file_map, delimiter='\t', encoding='utf-8', chunk_size=1):
        """
            Class method that reads a csv file and persist its content to a database table.

            Parameters:
                file_path (string): folder and name of the file to read.
                file_map (string): folder and name of a metadata file that describes how to process the file.
                delimiter (string): a character used to separate values in the file.
                chunk_size (int): defines the chunk of data to read at a time. When chunk size is 1, the file is read
                line by line. Otherwise, multiple lines are read and processed at once, which may increases the speed
                and overall performance of the reading process.
                encoding (str): file encoding.

            Returns:
                obj_entity (EntityDB): the metadata representation of the data source in the database.

            Raises:
                NoHeaderInFile (EsgMatchingError): the csv file has no header
                ColumnsDifferFromMapping (EsgMatchingError): columns in the csv file differ from the metadata file

                    - if the database engine was not specified.
                    - if the csv file does not have a column header
                    - if columns in the mapping file are not compatible to the ones found in the csv file
        """

        # Initialize super class attributes
        super().read_file(file_path, file_map, delimiter, chunk_size)

        try:
            # Open and read the csv file
            with open(self._file_path, 'r', encoding=encoding) as csv_file:
                # pass the file object to DictReader() to get the DictReader object
                csv_reader = DictReader(csv_file, delimiter=delimiter)

                # get column names from the csv file
                csv_column_names = list(csv_reader.fieldnames)
                if len(csv_column_names) == 0:
                    raise NoHeaderInFile

                # Create the data source entity
                obj_entity = EntityDB(self._file_map)
                obj_entity.set_database_engine(self._db_engine)

                # Check if all attributes in the mapping file are present in the csv file
                valid_columns = obj_entity.are_column_names_valid(csv_column_names)
                if not valid_columns:
                    raise ColumnsDifferFromMapping

                # Update the total number of attributes to be read from file
                self._total_attributes = len(csv_column_names)

                # Start the session if necessary
                if self._use_session:
                    self._db_engine.create_session()

                # Create a table using the EntityDB object
                obj_entity.create_data_source_in_db()

                # data_chunk is a list of dictionaries that stores the data to be inserted into a table.
                # Each element in the list represents a row to be inserted in the table in the following format:
                # [{'col1': 'value', 'col2': 'value'},{'col1': 'value', 'col2': 'value'}]
                data_chunk = []
                # counting chunk size and lines
                count_chunk = 0
                count_lines = 0
                # Iterate over each line to insert the rows into the database
                for file_line in csv_reader:
                    count_chunk += 1
                    count_lines += 1
                    # Convert the file row into a database row that represents the entitydb being read
                    # The EntityDB translates the lines of the csv file into recognizable column/value objects.
                    db_row = obj_entity.convert_to_db_row(file_line)
                    # Add the row into the data chunk
                    data_chunk.append(db_row)
                    # Check if the chunk must be flushed into the database
                    if count_chunk == chunk_size:
                        # Insert the content of the chunk into the database
                        self._db_engine.insert_row(obj_entity.table_obj, data_chunk)
                        # Start a new data chunk
                        count_chunk = 0
                        data_chunk = []
                # Update the total number of lines
                self._total_lines = count_lines
                # Check if there are still some data to be flushed to the database
                if count_chunk > 0 and len(data_chunk) > 0:
                    self._db_engine.insert_row(obj_entity.table_obj, data_chunk)
            # Commit all changes if using session object
            if self._use_session:
                self._db_engine.commit_changes()
                self._db_engine.close_session()

            # Create the report object to keep information about the reading process
            report_name = ' File Reader Report '
            report_desc = 'Details of the ETL process performed on [' + obj_entity.name + '] data source.'
            self._report_obj = ReportManager(report_name, report_desc)
            self._report_obj.add_content('File Name', self._file_path)
            self._report_obj.add_content('Columns in File', str(self._total_attributes))
            self._report_obj.add_content('Lines in File', str(self._total_lines))

            total_db_columns = len(obj_entity.get_table_column_names())
            total_db_rows = obj_entity.get_total_entries_by_idx()
            total_cols_to_read = len(obj_entity.get_original_attribute_names())
            self._report_obj.add_content('Columns Read from File', str(total_cols_to_read))
            self._report_obj.add_content('Columns in Database', str(total_db_columns))
            self._report_obj.add_content('Rows in Database', total_db_rows)

            return obj_entity

        except Exception as e:
            if self._use_session:
                self._db_engine.rollback_changes()
                self._db_engine.close_session()
            else:
                self._db_engine.drop_table(obj_entity.table_obj)
            original_msg = getattr(e, 'message', repr(e))
            exception_msg = original_msg + ' - ' + 'Error loading data into the database.'
            raise ErrorDB(exception_msg)
