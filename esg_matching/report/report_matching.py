""" This module defines the class ReportOnMatching """

# Import python libraries
import datetime

# Import internal libraries
from esg_matching.report.report_manager import ReportManager


class ReportOnMatching(ReportManager):
    """
        This class allows the creation of Report objects as to register the result performed during
        matching operations (matching reports).
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
        super().__init__(report_name, report_desc)
        self._report_matching = {'name': report_name, 'description': report_desc}
        self._data = []

    def print_report(self):
        """
            Class method that prints (default output) the content of the report.

            Parameters:

            Returns:
                This method is abstract, implemented by sub-classes.

            Raises:
                No exception is raised.
        """
        print('{:-^70}'.format(' REPORT ON MATCHING '))
        print('Report name: {}'.format(self._name))
        print('Description: {}'.format(self._description))
        print('Datetime:{:%Y-%m-%d %H:%M:%S}'.format(self._datetime))

        dash = '-' * 70
        print(dash)
        print("{:<20} {:<20} {:>10}".format('Referential', 'Target', 'Coverage (%)'))
        print(dash)
        for data in self._data:
            print("{:20} {:<20} {:>10.2f}".format(
                data['referential'],
                data['target'],
                data['coverage']))
        print(dash)

    def add_matching_data(self, ref_name, target_name, coverage):
        dict_data = {'timestamp': datetime.datetime.now(),
                     'referential': ref_name,
                     'target': target_name,
                     'coverage': coverage}
        self._data.append(dict_data)