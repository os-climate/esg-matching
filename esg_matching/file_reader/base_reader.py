"""
    The **esg_matching.file_reader.base_reader** module provides an abstract representation of a file-reader object.
    The FileReader() class should be used to represent any type of input data source file.
    For concrete representation of a specific data source file, check the esg_matching.file_reader.csv_reader module.
"""

from abc import ABC, abstractmethod


class FileReader(ABC):
    """
        Provides the infrastructure needed to create a non-typed file reader object.

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
                List of attribute to read from the file.
    """

    def __init__(self):
        """
            Constructor method.
        """
        self._extension_supported = ''
        self._encoding = 'utf-8'
        self._total_lines = 0
        self._total_attributes = 0
        self._total_attributes_read = 0
        self._attributes_to_read = None
        self._renamed_attributes = None

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

    @property
    def renamed_attributes(self):
        """Returns the list of attribute names to read from the file."""
        return self._renamed_attributes

    @renamed_attributes.setter
    def renamed_attributes(self, renamed_attributes: list):
        """Sets the list of attribute names to read from the file."""
        self._renamed_attributes = renamed_attributes

    @abstractmethod
    def read_file(self, file_path: str):
        """
            Abstract method, implemented by the concrete class, to read the content of a file.

            Parameters:
                file_path (str)
                    Complete path and name of the file to read.
            Yields:
                Line by line of the file. See concrete implementations of this base class.

        """
        pass