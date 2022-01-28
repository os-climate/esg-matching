""" This module defines the base class ReportManager """

# Import python libraries
from datetime import datetime
from abc import ABC


class ReportManager(ABC):
    """
        This base class allows the creation of Report objects to register the result performed during
        the ETL and matching operations as as textual and user friendly manner.

        Attributes:
            _datetime (datetime): data and time of the report.
            _name (string): short report name.
            _description (string): short report description.
            _dict_content (dict): a dictionary that represents the content of the report in a key, value format
    """

    def __init__(self, report_name, report_desc):
        """
            Constructor method for the ReportManager class.

            Parameters:
                report_name (string): a name for the report
                report_desc (string): a description for the report and its content

            Returns:
                ReportManager (object)

            Raises:
                No exception is raised.
        """
        self._datetime = datetime.now()
        self._name = report_name
        self._description = report_desc
        self._dict_content = {}

    def add_content(self, key, value):
        """
            Class method to add content to the report in a [key, value] format.

            Parameters:
                key (string): key of the information being reported.
                value (string): value of the information being reported.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._dict_content[key] = value

    def remove_content(self, key):
        """
            Class method to remove content from the report.

            Parameters:
                key (string): key of the information being removed.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._dict_content.pop(key)

    def clear_content(self):
        """
            Class method to clear all the content from the report.

            Parameters:
                No parameters provided.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        self._dict_content = {}

    def save_report_as_csv(self, file_path):
        """
            Class method to save the content of the report into a csv file.

            Parameters:
                file_path (string): folder and name of the report to be generated.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        with open(file_path, 'w') as f:
            for key in self._dict_content.keys():
                f.write("%s,%s\n" % (key, self._dict_content[key]))

    def print_report(self):
        """
            Class method to print out a textual version of the report.

            Parameters:
                No parameters provided.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        print('{:-^100}'.format(self._name.upper()))
        print('Description: {}'.format(self._description))
        print('Datetime:{:%Y-%m-%d %H:%M:%S}'.format(self._datetime))
        dash = '-' * 100
        print(dash)
        for key, item in self._dict_content.items():
            print('{}: {}'.format(key, item))