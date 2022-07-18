""" Base class to connect to Trino """

# Import third-party libraries
import trino
import sqlalchemy as sa
from sqlalchemy.engine import create_engine

# Import internal modules
from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.exceptions import exceptions_db_engine


class TrinoConnector(DbConnector):
    """
        This base class provides the infrastructure needed to connect to Trino

    """

    __HTTP_STD_SCHEME = "https"
    __VERIFY_FLAG = True

    def __init__(self, engine=None):
        super().__init__()

        self._username = None
        self._user_password = None
        self._host_url = None
        self._port_number = 443
        self._catalog = None
        self._http_scheme = self.__HTTP_STD_SCHEME
        self._verify = self.__VERIFY_FLAG

        if not (engine is None):
            self._engine = engine

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, username: str):
        self._username = username

    @property
    def user_password(self):
        return self._user_password

    @user_password.setter
    def user_password(self, user_password: str):
        self._user_password = user_password

    @property
    def host_url(self):
        return self._host_url

    @host_url.setter
    def host_url(self, host_url: str):
        self._host_url = host_url

    @property
    def port_number(self):
        return self._port_number

    @port_number.setter
    def port_number(self, port_number: int):
        self._port_number = port_number

    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, new_catalog: str):
        self._catalog = new_catalog

    @property
    def http_scheme(self):
        return self._http_scheme

    @http_scheme.setter
    def http_scheme(self, new_http_scheme: str):
        self._http_scheme = new_http_scheme

    @property
    def verify(self):
        return self._verify

    @verify.setter
    def verify(self, new_verify: str):
        self._verify = new_verify

    def _prepare_string_connection(self):
        """
            Class method that overrides the super class method to prepare a trino connection string.

        """

        # The basic trino connection string follows the pattern below:
        # trino://username@host_url:port_number
        self._string_connection = 'trino://{0}@{1}:{2}'.format(self._username, self._host_url, self._port_number)
        if self._catalog is not None:
            self._string_connection += f"/{self._catalog}"

    def _is_ready_to_connect(self):
        """
            Class method that overrides the super class method as to check the required connection parameters
            required by the Oracle database.

        """
        if self._username == '' or self._user_password == '' or self._host_url == '' \
                or self._host_url == '' or self._port_number == '':
            return False
        else:
            return True

    def connect(self):
        """
            Class method used to stablish a connection to Trino database.

            Raises:
                ConnectionAttributesNotDefined: when the private method class _is_ready_to_connect() returns False
                ConnectionStringNotDefined: when the private class property _string_connection is empty
        """
        if not self._is_ready_to_connect():
            raise exceptions_db_engine.ConnectionAttributesNotDefined

        # Prepare the string connection
        self._prepare_string_connection()

        # Check if the connection string was created
        if self._string_connection == '':
            raise exceptions_db_engine.ConnectionStringNotDefined

        # Connect to the database
        sqlargs = {
            'auth': trino.auth.JWTAuthentication(self._user_password),
            'http_scheme': self._http_scheme
        }
        self._engine = create_engine(self._string_connection, connect_args=sqlargs, echo=self._show_sql_statement)
        self._engine.connect()

    def get_column_type(self, str_type: str = 'VARCHAR', size: int = 0):
        """
            Class method that overrides the super class method to reflect the data types supported by trino

            Parameters:
                str_type (str): a string that defines a datatype
                size (int): the size of the datatype

            Returns:
                the trino database object types: VARCHAR, DECIMAL, CHAR, DATE

        """
        if str_type == 'str' or str_type == 'VARCHAR':
            if size > 0:
                return sa.String(size)
            else:
                return sa.String

        if str_type == 'INTEGER':
            return sa.BigInteger

        if str_type == 'DECIMAL':
            return sa.DECIMAL

        if str_type == 'REAL' or str_type == 'DOUBLE':
            return sa.Float

        if str_type == 'BOOLEAN':
            return sa.Boolean

        if str_type == 'DATE':
            return sa.Date

        if str_type == 'TIME':
            return sa.Time

        if str_type == 'TIMESTAMP':
            return sa.TIMESTAMP

        if str_type == 'DATE_TIME':
            return sa.DateTime
        else:
            raise exceptions_db_engine.DataTypeNotSupported

    def get_auto_pk_column(self, column_name):
        """
            Class method that creates an autoincrement primary key database column accoding to the connectors dialect.
            This method might be overridden by subclasses because databases usually have differents ways to create
             automatic primary keys columns.

            Parameters:
                No parameters.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the db engine

            Raises:
                No exception is raised.
        """
        column_object = sa.Column(column_name, sa.Integer, server_default=sa.func.row_number())
        return column_object

    def get_auto_timestamp_column(self, column_name):
        """
            Class method that creates an automatic timestamp column accoding to the connectors dialect.
            This method might be overridden by subclasses because databases usually have differents ways to create
             automatic timestamp columns.

            Parameters:
                No parameters.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the db engine

            Raises:
                No exception is raised.
        """
        column_object = sa.Column(column_name, sa.TIMESTAMP, server_default=sa.func.now())
        return column_object