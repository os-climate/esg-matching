""" Base class allows to create a file_reader object that reads data from a csv file"""

# Import python libraries
from csv import DictReader
from typing import NamedTuple

# Import internal libraries
from esg_matching.file_reader.file_reader import FileReader
from esg_matching.exceptions import exceptions_file


class FileReaderCsv(FileReader):
    """
        This base class provides the infrastructure needed to create a file reader

        Attributes:
            _separator (str): separator of attributes in a csv file
    """

    def __init__(self):
        """
            Constructor method.

            Parameters:
                No parameter required.

            Returns:
                FileReaderCsv (object)

            Raises:
                No exception is raised.
        """
        super().__init__()
        self._separator = ','
        self._extension_supported = '.csv'

    def _attributes_exist_in_file(self, attribute_names: list):
        """
            Private class method that checks if a list of names exist as attributes

            Parameters:
                attribute_names (list): list of attribute names

            Returns:
                (bool) True if all the names are attributes or False, otherwise.

            Raises:
                No exception is raised.
        """

        for item in self._attributes_to_read:
            if item not in attribute_names:
                return False
        return True

    def _create_row_object(self):
        """
            Private class method that creates an object to represent a row of a csv file

            Parameters:
                No parameter required.

            Returns:
                file_record (NamedTuple): a structure that represents a row of a csv file with all its attributes

            Raises:
                No exception is raised.
        """

        attributes_row = []
        for item in self._attributes_to_read:
            attributes_row.append((item, str))
        file_record = NamedTuple('FileRecord', attributes_row)
        return file_record

    def read_file_header_columns(self, file_path):
        """
            Class method that reads only the header of a csv file.

            Parameters:
                file_path (str): folder and name of the csv file to read.

            Returns:
                csv_column_names (list): a list with the attribute names in the file header

            Raises:
                exceptions_file.NoHeaderInFile when the file does not have a header
        """

        # Open and read the csv file
        with open(file_path, 'r', encoding=self._encoding) as csv_file:
            # pass the file object to DictReader() to get the DictReader object
            csv_reader = DictReader(csv_file, delimiter=self._separator)

            # get column names from the csv file
            csv_column_names = list(csv_reader.fieldnames)
            if len(csv_column_names) == 0:
                raise exceptions_file.NoHeaderInFile

            return csv_column_names

    def read_file(self, file_path):
        """
            Class method that reads the content of a csv file.

            Parameters:
                file_path (str): folder and name of the csv file to read.

            Returns:
                Return line by line of the file in the format of a NamedTuple (see _create_row_object method)

            Raises:
                exceptions_file.NoHeaderInFile when file does not have a header
                exceptions_file.ColumnsToReadDifferFromFileHeader when the columns in the file differ from the columns
                    expected to be read (columns described in the file settings)
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
                row_object = file_record(**file_line)
                yield row_object
            self._total_lines = count_lines