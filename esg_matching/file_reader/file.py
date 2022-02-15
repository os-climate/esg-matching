""" Base class allows to create an object to represent a file accompanied with its matching settings. """

from esg_matching.file_reader.file_settings import JsonSettings
from esg_matching.file_reader import file_utils
from esg_matching.exceptions import exceptions_file


class File:
    """
        This base class provides the infrastructure needed to represent a data file and/or its settings

        Attributes:
            _filename (str)
                The complete file path and name.
            _file_extension (str)
                File extension (e.g. .csv)
            _filename_settings (str)
                Path and name of the json file used as the matching settings.
            _obj_settings (esg_matching.file_reader.file_settings.JsonSettings)
                Settings object that translates the properties of a json file into matching parameters and policies.
    """

    def __init__(self, filename_settings):
        # File properties
        self._filename = ''
        self._file_extension = ''

        # Prepare the settings object
        self._filename_settings = filename_settings
        self._obj_settings = JsonSettings(filename_settings)

        if self._obj_settings.file_path != '':
            self._get_file_info()

    @property
    def filename(self):
        """ The complete file path and name. """
        return self._filename

    @property
    def extension(self):
        """ File extension (e.g. .csv). """
        return self._file_extension

    @property
    def filename_settings(self):
        """ Path and name of the json file used as the matching settings. """
        return self._filename_settings

    @property
    def settings(self):
        """ Settings object that translates the properties of a json file into matching parameters and policies. """
        return self._obj_settings

    def _get_file_info(self):
        """
        Private method to retrieve the complete path, name and extension of the file described in the json settings.
        """

        if file_utils.is_file(self._obj_settings.file_path):
            self._filename = self._obj_settings.file_path
            self._file_extension = file_utils.get_extension(self._filename)
        else:
            if not file_utils.is_dir(self._obj_settings.file_path):
                raise exceptions_file.FilePathNotFoundOrUnreachable
            self._filename = file_utils.get_newest_filename(self._obj_settings.file_path,
                                                            self._obj_settings.file_extension_pattern,
                                                            self._obj_settings.filename_pattern)
            if self._filename == '':
                raise exceptions_file.FileNotFoundOrUnreachable
            self._file_extension = file_utils.get_extension(self._filename)