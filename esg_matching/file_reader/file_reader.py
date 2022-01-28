""" Base class allows to create a file_reader object """

from abc import ABC, abstractmethod


class FileReader(ABC):
    """
        This base class provides the infrastructure needed to create a file reader

        Attributes:
            _extension_supported (str): describes the extension of the file supported by the reader
            _encoding (str): file encoding
            _total_lines (int): total of lines in the file
            _total_attributes (int): total of attributes in the file
            _total_attributes_read (int): total of attributes read from the file
            _attributes_to_read (list): list with attributes names to read from the file
    """

    def __init__(self):
        """
            Constructor method.

            Parameters:
                No parameter required.

            Returns:
                FileReader (object)

            Raises:
                No exception is raised.
        """
        self._extension_supported = ''
        self._encoding = 'utf-8'
        self._total_lines = 0
        self._total_attributes = 0
        self._total_attributes_read = 0
        self._attributes_to_read = None

    @property
    def extension_supported(self):
        return self._extension_supported

    @property
    def total_lines(self):
        return self._total_lines

    @property
    def total_attributes(self):
        return self._total_attributes

    @property
    def total_attributes_read(self):
        return self._total_attributes_read

    @property
    def attributes_to_read(self):
        return self._attributes_to_read

    @attributes_to_read.setter
    def attributes_to_read(self, attributes_to_read: list):
        self._attributes_to_read = attributes_to_read

    @abstractmethod
    def read_file(self, file_path: str):
        """
            Class abstract method that reads the content of a file.

            Parameters:
                file_path (str): folder and name of the file to read.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        pass