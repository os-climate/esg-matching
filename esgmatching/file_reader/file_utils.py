import json
import glob
import os


def read_json_file(file_to_read):
    """
    Reads a json file and returns its content as a python dictionary.

    Parameters:
        file_to_read (str): complete path and name of a json file
    Returns:
        dict_content (dict): the content of a json file as a python dictionary
    Raises:
        No exception is raised.
    """
    # Reads a json file
    with open(file_to_read, encoding="utf-8") as json_file:
        dict_content = json.load(json_file)
    return dict_content


def file_exists(filename):
    """
    Check if a file exists.

    Parameters:
        filename (str): complete path and name of a file
    Returns:
        True if the file exists or False otherwise.
    Raises:
        No exception is raised.
    """
    return os.path.exists(filename)


def get_extension(filename):
    """
    Get file extension.

    Parameters:
        filename (str): complete path and name of a file
    Returns:
        (str) the filename extension (.csv)
    Raises:
        No exception is raised.
    """
    filename_parts = os.path.splitext(filename)
    return filename_parts[1]


def is_file(filename):
    """
    Check if filename is a real file.

    Parameters:
        filename (str): complete path and/or name of a file
    Returns:
        (bool) True if is a file or False otherwise
    Raises:
        No exception is raised.
    """
    return os.path.isfile(filename)


def is_dir(path):
    """
    Check if filename is a real file.

    Parameters:
        path (str): complete path of a folder
    Returns:
        (bool) True if is a folder or False otherwise
    Raises:
        No exception is raised.
    """
    return os.path.isdir(path)


def get_newest_filename(path, extension_pattern='', filename_pattern=''):
    """
    Retrieves the complete path and name of the newest file on a given folder that follows a pattern in its name.

    Parameters:
        path (str): complete path of a folder
        extension_pattern (str): a pattern that defines the file type (e.g. *.csv)
        filename_pattern (str): a pattern that defines the name of the file to look for
    Returns:
        (bool) True if is a folder or False otherwise
    Raises:
        No exception is raised.
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
            if str(file).find(filename_pattern):
                filtered_files.append(file)
    else:
        filtered_files = all_files

    if len(filtered_files) == 0:
        return ''

    # Get the newest file
    newest_file = max(filtered_files, key=os.path.getctime)
    return newest_file
