""" Base class to connect to a SQLLite database """

# Import internal modules
from esgmatching.db_engine.engines.connector import DbConnector


class SqlLiteConnector(DbConnector):
    """
        This base class provides the infrastructure needed to connect to a SqlLite database

        Attributes:
            _path_db (str): complete path of the sqlite database
    """
    __DB_DIALECT = "sqlite"

    def __init__(self):
        """
            Constructor method.

            Parameters:
                No parameters required.

            Returns:
                SqlLiteConnector (object)

            Raises:
                No exception is raised.
        """
        super().__init__()

        self._path_db = ''

    @property
    def path_db(self):
        return self._path_db

    @path_db.setter
    def path_db(self, path_db: str):
        self._path_db = path_db

    def _prepare_string_connection(self):
        """
            Class method that overrides the super class method to prepare a sqlite connection string.
            SQLite connects to file-based databases, using the Python built-in module sqlite3 by default.
            As SQLite connects to local files, the URL format is slightly different.
            The “file” portion of the URL is the filename of the database.
            For a relative file path, this requires three slashes.

            Parameters:
                No parameters required.

            Returns:
                No return values.

            Raises:
                No exception is raised.
        """
        self._string_connection = self.__DB_DIALECT + ':///' + self._path_db

    def _is_ready_to_connect(self):
        """
            Class method that overrides the super class method as to check if the path was provided, being it
            the only parameter required to connect to a sqlite database.

            Parameters:
                No parameters required.

            Returns:
                No return values.

            Raises:
                No exception is raised.
        """
        if self._path_db == '':
            return False
        return True