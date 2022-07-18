""" Base class to connect to a database """

# Import python libraries
from abc import ABC, abstractmethod

# Import third-party libraries
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

# Import internal modules
from esg_matching.exceptions import exceptions_db_engine


class DbConnector(ABC):
    """
        This base class provides the infrastructure needed to connect to a database.

        Attributes:
            _string_connection (str): the connection string used to connect to the database.
            _engine (sqlalchemy.engine.Engine): the engine representing the connection with the database.
            _current_session (sqlalchemy.orm.Session): a session object with the database.
            _show_sql_statement(bool): a flag to indicate if all the operations performed in the database
                must use a verbose mode...meaning that database messages will be sent to the default output.
    """

    def __init__(self):
        """
            Constructor method.

        """
        self._string_connection = ''
        self._engine = None
        self._current_session = None
        self._show_sql_statement = False

    @property
    def engine(self):
        return self._engine

    @property
    def session(self):
        return self._current_session

    @property
    def string_connection(self):
        return self._string_connection

    @property
    def show_sql_statement(self):
        return self._show_sql_statement

    @show_sql_statement.setter
    def show_sql_statement(self, new_bol_value: bool):
        self._show_sql_statement = new_bol_value

    @abstractmethod
    def _prepare_string_connection(self):
        """
            This method is provided by the subclasses. It creates the connection string according
            to the type of database in use, since each database has its own connection requirements.
            The result of this method should be stored in the private class attribute _string_connection, which
            is subsequently used by the connect() method to perform the connection to the database.

        """
        pass

    @abstractmethod
    def _is_ready_to_connect(self):
        """
            This method is provided by the subclasses to indicate if the connectors has all the information required
            to perform the connection to a database. This method is called by the connect() method before the a
            connection is stablished. Depending on the type of database in use, it is required to perform several
            checkings regarding the connection parameters, for intance: user_name, password, host url, etc...

        """
        pass

    def connect(self):
        """
            Class method used to stablish a connection to the database.

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
        self._engine = create_engine(self._string_connection, echo=self._show_sql_statement)

    def disconnect(self):
        """
            Class method that closes a connection with the database and disposes the database engine object in memory.

        """

        # Close the current session if it is open
        if self.has_session_open():
            self.close_session()

        # Dispose the database engine object
        if self.is_connected():
            self._engine.dispose()
            self._engine = None

        # Cleans up the string connection
        self._string_connection = ''

    def is_connected(self):
        """
            Class method that checks if a database connection object exists and is ready to use.

            Returns:
                True if a connection object exists or False otherwise.

        """
        if self._engine is None:
            return False
        else:
            return True

    def create_session(self, session_type: str = 'normal'):
        """
            Class method that creates a new database session so that all operations are performed in a batch mode.
            As a consequence of using this method, the operations performed are concluded only after a commit or a
            rollback call is performed.
            A normal session differs from a scoped session in the sense that if this method is called multiple times,
            a new session object is created and its states are independent from the previous session.

            Parameters:
                session_type (str): the type of session being created - 'normal' or 'scoped'

            Raises:
                SessionAlreadyExists: if a session already exists for the connectors.
                SessionTypeNotSupported: when session_type parameter is different from 'normal' or 'scoped'
        """

        # Check if a session already exists
        if self.has_session_open():
            raise exceptions_db_engine.SessionAlreadyExists

        # Create a new session if a session does not exist
        session_factory_obj = sessionmaker()
        session_factory_obj.configure(bind=self._engine)
        if session_type == 'normal':
            self._current_session = session_factory_obj()
        elif session_type == 'scoped':
            self._current_session = scoped_session(session_factory_obj)
        else:
            raise exceptions_db_engine.SessionTypeNotSupported

    def close_session(self):
        """
            Class method that closes the current session.

            Raises:
                NoSessionOpen: when there is no session to be closed.
        """
        if not self.has_session_open():
            raise exceptions_db_engine.NoSessionOpen

        self._current_session.close()
        self._current_session = None

    def has_session_open(self):
        """
            Class method that checks if there is a session object open and ready to use.

            Returns:
                True if a session object exists or False otherwise.

        """
        if self._current_session is None:
            return False
        else:
            return True

    def commit_changes(self):
        """
            Class method that commits (confirm) the operations executed during the current session.

            Raises:
                NoSessionOpen: when there is no session to execute the commit operation
        """
        if not self.has_session_open():
            raise exceptions_db_engine.NoSessionOpen

        self._current_session.commit()

    def rollback_changes(self):
        """
            Class method that rollback (cancel out) the operations executed during the current session.

            Raises:
                NoSessionOpen: when there is no session to execute the commit operation
        """
        if not self.has_session_open():
            raise exceptions_db_engine.NoSessionOpen

        self._current_session.rollback()

    def get_column_type(self, str_type: str = 'str', size: int = 0):
        """
            Class method that returns the equivalent database column's datatype object, given a string that
            defines that datatype and it is recognized by this connectors or its subclasses.

            Parameters:
                str_type (str): a string that defines a datatype
                size (int): the size of the datatype

            Returns:
                the database object type (db.String, db.Integer, db.Boolean, db.DateTime)

            Raises:
                DataTypeNotSupported: when the string datatype is not recognizable as a database datatype
        """
        if str_type == 'str':
            if size > 0:
                return sa.String(size)
            else:
                return sa.String

        if str_type == 'int':
            return sa.Integer

        if str_type == 'bool':
            return sa.Boolean

        if str_type == 'datetime':
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
        column_object = sa.Column(column_name, sa.Integer, primary_key=True, autoincrement=True)
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
        column_object = sa.Column(column_name, sa.DateTime, server_default=sa.func.now())
        return column_object

    @staticmethod
    def get_null_value():
        """
            Class method that returns the equivalent database null object

        """
        return sa.sql.null()

    def drop_table(self, table_obj: sa.Table):
        """
            Class method to drop a table from the database.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                ConnectionNotDefined: when there is no connection with the database
        """
        if not self.is_connected():
            raise exceptions_db_engine.ConnectionNotDefined

        table_obj.drop(self._engine)

    def table_exists(self, table_name: str, table_schema: str = None):
        """
            Class method that checks if a table exists in the database.
            This method may need to be overridden by subclasses since some databases may not support direct
            inspection performed by sqlalchemy.

            Parameters:
                table_name (str): name of the table
                table_schema (str): table schema, used to connect to databases such as Trino

            Returns:
                True if the table exists in the database or False otherwise.

            Raises:
                ConnectionNotDefined: when there is no connection with the database
        """
        if not self.is_connected():
            raise exceptions_db_engine.ConnectionNotDefined

        if table_schema is None:
            metadata = sa.MetaData(self._engine)
            result = sa.inspect(metadata.bind).has_table(table_name)
        else:
            inspector = sa.inspect(self._engine)
            tables_in_schema = inspector.get_table_names(schema=table_schema)
            result = table_name in tables_in_schema

        return result

    def get_table_from_metadata(self, table_name: str, schema=None):
        """
            This class method uses reflection to explicitly get the metadata object of a database table.
            The metadata structure is composed by the table name and its attributes (name, type and properties).

            Parameters:
                table_name (str): name of the table from where to retrieve the metadata structure.
                schema (str): schema where the table is located (depend on the database used)

            Returns:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                ConnectionNotDefined: when there is no connection with the database
        """
        if not self.is_connected():
            raise exceptions_db_engine.ConnectionNotDefined

        # Create metaData instance for the engine
        if schema is None:
            metadata = sa.MetaData(self._engine)
        else:
            metadata = sa.MetaData(schema=schema)
            metadata.reflect(self._engine)

        # Get the Table metadata object
        table_obj = sa.Table(table_name, metadata, autoload=True, autoload_with=self._engine)
        return table_obj