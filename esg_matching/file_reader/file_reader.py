""" Abstract class to represent a file-reader object """

from abc import ABC, abstractmethod


class FileReader(ABC):
    """
        This base class provides the infrastructure needed to create a file reader

        Attributes:
            _extension_supported (str)
                Describes the extension of the file supported by the reader.
            _encoding (str)
                File encoding.
            _total_lines (int)
                Total of lines in the file.
            _total_attributes (int)
                Total of attributes in the file.
            _total_attributes_read (int)
                Total of attributes read from the file.
            _attributes_to_read (list)
                List of attribute names to read from the file.
    """

    def __init__(self):
        """
            Constructor method.

            Returns:
                FileReader (object)

        """
        self._extension_supported = ''
        self._encoding = 'utf-8'
        self._total_lines = 0
        self._total_attributes = 0
        self._total_attributes_read = 0
        self._attributes_to_read = None

    @property
    def extension_supported(self):
        """Describes the extension of the file supported by the reader."""
        return self._extension_supported

    @property
    def total_lines(self):
        """Total of lines in the file."""
        return self._total_lines

    @property
    def total_attributes(self):
        """Total of attributes in the file."""
        return self._total_attributes

    @property
    def total_attributes_read(self):
        """Total of attributes read from the file."""
        return self._total_attributes_read

    @property
    def attributes_to_read(self):
        """Returns the list of attribute names to read from the file."""
        return self._attributes_to_read

    @attributes_to_read.setter
    def attributes_to_read(self, attributes_to_read: list):
        """Sets the list of attribute names to read from the file."""
        self._attributes_to_read = attributes_to_read

    @abstractmethod
    def read_file(self, file_path: str):
        """
            Class abstract method that reads the content of a file.

            Parameters:
                file_path (str)
                    Path and name of the file to read.

        """
        pass