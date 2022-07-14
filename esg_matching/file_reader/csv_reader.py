"""
    The **esg_matching.file_reader.csv_reader** module provides a concrete implementation of a file-reader object
    for excel (.csv) files.
"""

# Import python libraries
from csv import DictReader
from typing import NamedTuple
import pandas as pd
import numpy as np

# Import internal libraries
from esg_matching.file_reader.base_reader import FileReader
from esg_matching.exceptions import exceptions_file


class FileReaderCsv(FileReader):
    """
        Provides the infrastructure needed to create a file reader for csv files.
        This class inherits from esg_matching.file_reader.base_reader.FileReader().

        Attributes:
            _separator (str)
                Separator of attributes in a csv file
            _extension_supported (str)
                Extension supported by the reader, which is by default .csv.
    """

    def __init__(self):
        """
            Constructor method.
        """

        super().__init__()
        self._separator = ','
        self._extension_supported = '.csv'

    @property
    def separator(self):
        return self._separator

    @separator.setter
    def separator(self, new_separator: str):
        self._separator = new_separator

    def _attributes_exist_in_file(self, attribute_names: list):
        """
            Private class method that checks if a list of names exist as attributes to be read from a file.

            Parameters:
                attribute_names (list)
                    List of attribute names to check if exist as attributes to be read from a file.

            Returns:
                (bool)
                    True if all the names are attributes or False, otherwise.

        """

        for item in self._attributes_to_read:
            if item not in attribute_names:
                return False
        return True

    def _create_row_object(self):
        """
            Private class method that creates an object to represent a row of a csv file.

            Returns:
                file_record (NamedTuple)
                    A structure that represents a row of a csv file with all its attributes.

        """

        attributes_row = []
        for item in self._attributes_to_read:
            attributes_row.append((item, str))
        file_record = NamedTuple('FileRecord', attributes_row)
        return file_record

    def _get_selected_attributes(self, file_row):
        new_file_row = {}
        for attribute_name in file_row.keys():
            if attribute_name in self._attributes_to_read:
                new_file_row[attribute_name] = file_row[attribute_name]
        return new_file_row

    def read_file_header_columns(self, file_path):
        """
            Class method that reads only the header of a csv file.

            Parameters:
                file_path (str)
                    Complete path and name of the csv file to read.

            Returns:
                csv_column_names (list)
                    A list with the attribute names in the file header.

            Raises:
                exceptions_file.NoHeaderInFile
                    When the file does not have a header.
        """

        # Open and read the csv file
        with open(file_path, 'r', encoding=self._encoding) as csv_file:
            # pass the file object to DictReader() to get the DictReader object
            csv_reader = DictReader(csv_file, delimiter=self._separator)

            # get column names from the csv file
            csv_column_names = csv_reader.fieldnames
            if len(csv_column_names) == 0:
                raise exceptions_file.NoHeaderInFile

            return csv_column_names

    def read_file(self, file_path):
        """
            Class method that reads the content of a csv file, line by line.

            Parameters:
                file_path (str)
                    Complete path and name of the csv file to read.

            Yields:
                Line by line of the file in the format of a NamedTuple (see _create_row_object() method)

            Raises:
                exceptions_file.NoHeaderInFile
                    When file does not have a header.
                exceptions_file.ColumnsToReadDifferFromFileHeader
                    When the columns in the file differ from the columns expected to be read.
        """

        # Open and read the csv file
        with open(file_path, 'r', encoding=self._encoding) as csv_file:
            # pass the file object to DictReader() to get the DictReader object
            csv_reader = DictReader(csv_file, delimiter=self._separator)

            # get column names from the csv file
            csv_column_names = list(csv_reader.fieldnames)
            if len(csv_column_names) == 0:
                raise exceptions_file.NoHeaderInFile

            # check the file columns
            if not self._attributes_to_read:
                # if the attributes to read are not specified, read all attributes specified in the file header
                self._attributes_to_read = csv_column_names
            else:
                # if attributes to read are specified, then make sure they exist in the file header
                if not self._attributes_exist_in_file(csv_column_names):
                    raise exceptions_file.ColumnsToReadDifferFromFileHeader

            # create a row object as a list of namedtuple with the name of the attribute and string as default type
            # file_record = [('field1',str),('field2',str),('field3',str)]
            file_record = self._create_row_object()

            # Update the total number of attributes
            self._total_attributes_read = len(self._attributes_to_read)
            self._total_attributes = len(csv_column_names)

            # Read each line of the file
            count_lines = 0
            for file_line in csv_reader:
                # Unpack the file line into the file_record named tuple
                # the result of the unpack will be something like this:
                # FileRecord(UNIQUE_ID='1', ISIN='SK1120005824', COMPANY='CENTRAL PERK', COUNTRY='SK')
                count_lines += 1
                line_selected_attributes = self._get_selected_attributes(file_line)
                row_object = file_record(**line_selected_attributes)
                yield row_object
            self._total_lines = count_lines

    def _adjust_dtype(self, df, db_conn):
        cols = df.select_dtypes(include='object')
        column_type_db = db_conn.get_column_type()
        dtypes = {col: column_type_db for col in cols}
        return dtypes

    def read_file_with_pd(self, file_path, table_name, db_conn):
        df = pd.read_csv(file_path, sep=self._separator, dtype=object, low_memory=False)
        columns = self._attributes_to_read
        df = df[columns]
        df.columns = self._renamed_attributes
        for attribute in self._renamed_attributes:
            df[attribute] = df[attribute].astype(str)
            mask = df[attribute] == 'nan'
            df.loc[mask, attribute] = np.nan
        db_types = self._adjust_dtype(df, db_conn)
        df.to_sql(name=table_name, con=db_conn.engine, index=False, if_exists='append', dtype=db_types, chunksize=1000)
