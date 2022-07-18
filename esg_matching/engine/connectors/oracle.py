""" Base class to connect to an Oracle database """

# Import third-party libraries
import cx_Oracle
import sqlalchemy as sa
from sqlalchemy.dialects import oracle

# Import internal modules
from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.exceptions import exceptions_db_engine


class OracleConnector(DbConnector):
    """
        This base class provides the infrastructure needed to connect to an Oracle database.

        Attributes:
            _username (str): username used to connect to the database
            _user_password (str): user's password used to connecto to the database
            _host_url (str): the host URL used to connecto to the database
            _service_name (str): the service or name of the database used in the connection string
            _port_number (int): the port used to connect to the database
            _client_driver_dir (str): the location (folder) of the oracle client library used for connection
    """

    __DB_DIALECT = "oracle"
    __SQL_DRIVER = "cx_oracle"

    def __init__(self):
        super().__init__()

        self._username = ''
        self._user_password = ''
        self._host_url = ''
        self._service_name = 'DATABASE'
        self._port_number = 1521
        self._client_driver_dir = ''

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
    def service_name(self):
        return self._service_name

    @service_name.setter
    def service_name(self, service_name: str):
        self._service_name = service_name

    @property
    def client_driver_dir(self):
        return self._client_driver_dir

    @client_driver_dir.setter
    def client_driver_dir(self, client_driver_lib_dir: str):
        self._client_driver_dir = client_driver_lib_dir

    def _prepare_string_connection(self):
        """
            Class method that overrides the super class method to prepare an oracle connection string.

        """
        if self._client_driver_dir == '':
            raise exceptions_db_engine.OracleClientDriverLibNotDefined

        # Initialize the driver
        cx_Oracle.init_oracle_client(self._client_driver_dir)

        # The basic oracle connection string follows the pattern below:
        # oracle+cx_oracle://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
        oracle_string_conn = '{dialect}+{driver}://{user}:{password}@{host}:{port}/?service_name={service}'.format(
            dialect=self.__DB_DIALECT,
            driver=self.__SQL_DRIVER,
            user=self._username,
            password=self._user_password,
            host=self._host_url,
            port=str(self._port_number),
            service=self._service_name
        )
        self._string_connection = oracle_string_conn

    def _is_ready_to_connect(self):
        """
            Class method that overrides the super class method as to check the required connection parameters
            required by the Oracle database.

        """
        if self._username == '' or self._user_password == '' or self._host_url == '' \
                or self._service_name == '' or self._port_number == '':
            return False
        else:
            return True

    def get_column_type(self, str_type: str = 'VARCHAR', size: int = 0):
        """
            Class method that overrides the super class method to reflect the data types supported by oracle

            Parameters:
                str_type (str): a string that defines a datatype
                size (int): the size of the datatype

            Returns:
                the oracle database object types: VARCHAR, VARCHAR2, NUMBER, CHAR, DATE

        """
        if str_type == 'VARCHAR' or str_type == 'str':
            if size > 0:
                return oracle.VARCHAR(size)
            else:
                return oracle.VARCHAR

        if str_type == 'VARCHAR2':
            if size > 0:
                return oracle.VARCHAR2(size)
            else:
                return oracle.VARCHAR2

        if str_type == 'NUMBER':
            if size > 0:
                return oracle.NUMBER(size)
            else:
                return oracle.NUMBER

        if str_type == 'CHAR':
            if size > 0:
                return oracle.CHAR(size)
            else:
                return oracle.CHAR

        if str_type == 'DATE':
            return oracle.DATE
        else:
            raise exceptions_db_engine.DataTypeNotSupported

    def get_auto_pk_column(self, column_name):
        """
            Class method that overrides the super class method to reflect the creation of autoincrement and primary
             keys supported by oracle database.

            Parameters:
                No parameters.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the db engine

            Raises:
                No exception is raised.
        """
        column_object = sa.Column(column_name, oracle.NUMBER, sa.Identity(start=1), primary_key=True)
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
        column_object = sa.Column(column_name, oracle.TIMESTAMP, server_default=sa.func.now())
        return column_object