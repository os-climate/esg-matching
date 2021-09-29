""" This module defines the abstract class FileReader """

# Import python libraries
import os.path
from abc import ABC, abstractmethod

# Import internal libraries
from esgmatching.exceptions import MappingFileDoesNotExist, FileDoesNotExist


class FileReader(ABC):
    """
        This base class allows the creation of FileReader objects as to manage the extraction, transformation and
        loading of data from different sources.

        Attributes:
            _file_path (string): folder and name of the file to read.
            _file_map (string): folder and name of a metadata file that describes how to process the file.
            _total_lines (int): total lines read from file.
            _total_attributes (int): total number of attributes or columns read from file.
    """

    def __init__(self):
        """
            Constructor method for FileReader object.

            Parameters:
                No parameters provided.

            Returns:
                FileReader (object)

            Raises:
                No exception is raised.
        """
        self._file_path = None
        self._file_map = None
        self._total_lines = 0
        self._total_attributes = 0
        self._report_obj = None

    @property
    def total_lines(self):
        """
            Object property that returns the total lines of the latest file read using the read_file() method.

            Parameters:
                 No parameters provided.

            Returns:
                _total_lines(int): total lines read from file.

            Raises:
                No exception is raised.
        """
        return self._total_lines

    @property
    def total_attributes(self):
        """
            Object property that returns the total number of attributes or columns of the latest file
            read using the read_file() method.

            Parameters:
                 No parameters provided.

            Returns:
                _total_attributes(int): total lines read from file.

            Raises:
                No exception is raised.
        """
        return self._total_attributes

    @property
    def report_obj(self):
        """
            Object property that returns a report object, resultant from the reading process.

            Parameters:
                 No parameters provided.

            Returns:
                _report_obj(ReportManager): report object

            Raises:
                No exception is raised.
        """
        return self._report_obj

    def __validate_files(self, file_path, file_map):
        """
            Private class method used to check if the file and its metadata/mapping file exist as
            to setup the correspondent class attributes.

            Parameters:
                file_path (string): folder and name of the file to read.
                file_map (string): folder and name of a metadata file that describes how to process the file.

            Returns:
                No return value.

            Raises:
                FileDoesNotExist (EsgMatchingError): file does not exist in the given folder.
                MappingFileDoesNotExist (EsgMatchingError): mapping file does not exist in the given folder.
        """
        # Check if the file exists and set the corresponding class attributes
        if os.path.exists(file_path):
            self._file_path = file_path
        else:
            raise FileDoesNotExist

        # Check if the metadata file exists and set the corresponding class attributes
        if os.path.exists(file_map):
            self._file_map = file_map
        else:
            raise MappingFileDoesNotExist

    @abstractmethod
    def read_file(self, file_path, file_map, delimiter='\t', chunk_size=1):
        """
            Class method that reads the content of a file.

            Parameters:
                file_path (string): folder and name of the file to read.
                file_map (string): folder and name of a metadata file that describes how to process the file.
                delimiter (string): a character used to separate values in the file.
                chunk_size (int): defines the chunk of data to read at a time. When chunk size is 1, the file is read
                line by line. Otherwise, multiple lines are read and processed at once, which may increases the speed
                and overall performance of the reading process.

            Returns:
                This method is abstract, implemented by sub-classes. Therefore, the return type may differ. It can,
                for instance, return an object to represent the resultant data or it's metadata.

            Raises:
                No exception is raised.
        """
        self.__validate_files(file_path, file_map)