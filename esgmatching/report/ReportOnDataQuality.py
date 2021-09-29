""" This module defines the class ReportOnDataQuality """

# Import python libraries
from abc import ABC, abstractmethod


class ReportOnDataQuality(ABC):
    """
        This class allows the creation of Report objects as to register the result performed during
        ETL (data quality reports).
    """

    def __init__(self, report_name, report_desc):
        """
            Constructor method.

            Parameters:
                report_name (string): a name for the report
                report_desc (string): a description for the report and its content

            Returns:
                ReportOnMatching (object)
            Raises:
                No exception is raised.
        """

        self._dict_null_attributes = {}
        self._dict_null_ids = {}
        self._dict_invalid_attributes = {}

    @abstractmethod
    def generate_report(self, file_path):
        """
            Class method that reads the content of a file.

            Parameters:
                file_path (string): complete file path and name of the report to be generated.

            Returns:
                An excel file with the resultant report.

            Raises:
                No exception is raised.
        """
        pass