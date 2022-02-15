""" The **esg_matching.file_reader.file_utils** module provides utility functions associated to file management
that are independent of the environment and operational system being used. Some of the functions are basic os.path
function built-in in python and could be easily added directly on the caller code. However, by modularizing them
in this module brings the advantage of reducing module dependencies on common libraries, centralize code mainteinance
and enhance code reusability.

The following fuctions are provided:

    - **read_json_file:** reads a json file and returns its content as a python dictionary.
    - **file_exists:** checks whether the specified file exists or not.
    - **get_extension:** gets the file extension.
    - **is_file:** checks if the filename passed as parameter is a real file.
    - **is_dir:** checks if the directory name passed as parameter is a real folder.
    - **get_newest_filename:** retrieves the complete path and name of the newest file on a given folder.

"""

# Import python libraries
import json
import glob
import os


def read_json_file(file_to_read):
    """
    Reads a json file and returns its content as a python dictionary.

    Parameters:
        file_to_read (str)
            Complete path and name of the json file to read.

    Returns:
        dict_content (dict)
            The content of the json file as a python dictionary. A python dictionary is organized in keys-values which
            are similar to the way a json file is structured in names-values.

    Examples:
        >>> json_filename = '/home/User/Desktop/myjsonfile.json'
        >>> json_content = read_json_file(json_filename)

    """

    # Reads a json file
    with open(file_to_read, encoding="utf-8") as json_file:
        dict_content = json.load(json_file)
    return dict_content


def file_exists(path):
    """
    Checks whether the specified file exists or not.

    Parameters:
        path (str)
            Complete filename (file path + name).

    Returns:
        (bool)
            True if the path exists or False otherwise.

    Examples:
        >>> my_filename = '/home/User/Desktop/myfile.txt'
        >>> result = file_exists(my_filename)

    """

    return os.path.exists(path)


def get_extension(filename):
    """
    Gets the file extension.

    Parameters:
        filename (str)
            Complete path and name of a file

    Returns:
        (str)
            The filename extension (.csv)

    Examples:
        >>> my_filename = '/home/User/Desktop/myfile.txt'
        >>> result = get_extension(my_filename)

    """

    filename_parts = os.path.splitext(filename)
    return filename_parts[1]


def is_file(filename):
    """
    Checks if the filename passed as parameter is a real file.

    Parameters:
        filename (str)
            Complete path and/or name of a file

    Returns:
        (bool)
            True if is a file or False otherwise

    Examples:
        >>> my_filename = '/home/User/Desktop/myfile.txt'
        >>> result = is_file(my_filename)

    """

    return os.path.isfile(filename)


def is_dir(path):
    """
    Checks if the directory name passed as parameter is a real folder.

    Parameters:
        path (str)
            Complete path of a folder

    Returns:
        (bool)
            True if is a folder or False otherwise

    Examples:
        >>> my_folder = '/home/User/Desktop/'
        >>> result = is_dir(my_folder)

    """

    return os.path.isdir(path)


def get_newest_filename(path, extension_pattern='', filename_pattern=''):
    """
    Retrieves the complete path and name of the newest file on a given folder. Patterns for the name and file extension
    can be provided as filters to better select the files of interest.

    Parameters:
        path (str)
            Complete path of a folder
        extension_pattern (str)
            The file extension used to filter files of interest.
        filename_pattern (str)
            A string pattern that describes a file name and serves as a filter to select the files of interest.

    Returns:
        (bool)
            True if is a folder or False otherwise

    Examples:
        >>> my_folder = '/home/User/Desktop/'
        >>> newest_filename = get_newest_filename(my_folder, '*.csv', 'my_settings_')

    """

    # Normalize path and get all files in the directory
    path = os.path.normpath(path)
    if extension_pattern != '':
        path = "{}{}{}".format(path, os.sep, extension_pattern)
    all_files = glob.glob(path)

    # Create a list of files that follows the filename_pattern
    filtered_files = []
    if filename_pattern != '':
        for file in all_files:
            if str(file).find(filename_pattern) != -1:
                filtered_files.append(file)
    else:
        filtered_files = all_files

    if len(filtered_files) == 0:
        return ''

    # Get the newest file
    newest_file = max(filtered_files, key=os.path.getctime)
    return newest_file
