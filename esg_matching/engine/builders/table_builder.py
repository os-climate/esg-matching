""" Base class to create database tables given a database connectors """

# Import third-party libraries
import pandas
from sqlalchemy import MetaData, Table

# Import internal modules
from esg_matching.exceptions import exceptions_db_engine, exceptions_builders
from esg_matching.engine.connectors.base import DbConnector
from esg_matching.engine.builders.column_builder import ColumnBuilder


class TableBuilder:
    """
        This base class provides the infrastructure needed to create database tables
        There are two ways of creating a table:
            (1) from a pandas dataframe: from this way, the table attributes are the same as in the dataframe
            (2) manually by defining the columns, one by one
        For the case (1), one needs only to call the methods create_table_from_df() and execute() as below:
            db_my_table = my_table_builder.create_table_from_df('my_table', df).execute(if_exists='drop')
        For the case (2) one needs to add the columns, one by one
            db_my_table = my_table_builder.create_table('my_table').add_auto_primary_key()
                                                                   .add_column('name', 'str', 40)
                                                                   .add_column('desc', 'str', 255)
                                                                   .execute(if_exists='drop')

        Attributes:
            _db_connector (DbConnector): a database engine
            _column_builder (ColumnBuilder): the column builders object that allows creating database columns
            _table_name (str): the table name
            _columns (list): a list with all the columns of the table being created
    """

    def __init__(self, db_connector: DbConnector):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): a database connectors used to create columns according to its dialect

            Returns:
                TableBuilder (object)

            Raises:
                ConnectionNotDefined: when the connectors object is None or there is no database connection active
        """
        # Check if the database connectors is defined
        if db_connector is None:
            raise exceptions_db_engine.ConnectionNotDefined

        # Check if there is a database connection still active
        if not db_connector.is_connected():
            raise exceptions_db_engine.ConnectionNotDefined

        self._db_connector = db_connector
        self._column_builder = ColumnBuilder(db_connector)
        self._table_name = ''
        self._columns = []
        self._df = None

    def reset_builder(self):
        """
            Class method used to clean up the class variables as to create a new table from scratch.

            Parameters:
                No parameters required.

            Returns:
                No return values

            Raises:
                No exception is raised.
        """
        self._table_name = ''
        self._columns = []
        self._df = None

    def create_table(self, table_name: str):
        """
            Sets up the table name used by the builders.

            Parameters:
                table_name (str): the table name.

            Returns:
                No return values

            Raises:
                No exception is raised.
        """
        self.reset_builder()
        self._table_name = table_name
        return self

    def add_columns(self, table_columns: list):
        """
            Sets the columns of the table. The ColumnBuilder class should be used to create columns considering a
                specific database type.

            Parameters:
                table_columns (list): a list of columns object (sqlalchemy.sql.schema.Column) in which each column
                    was created with ColumnBuilder.

            Returns:
                No return values

            Raises:
                No exception is raised.
        """
        self._columns = table_columns
        return self

    def execute(self):
        """
            Creates the table in the database.

            Parameters:
                No parameters required.

            Returns:
                new_table (sqlalchemy.Table): the sqlalchemy metadata of the new database table

            Raises:
                No exception is raised.
        """
        if self._columns is None:
            raise exceptions_builders.TableColumnsNotDefined

        if self._table_name == '':
            raise exceptions_builders.TableNameNotDefine

        # Define the metadata as default
        meta = MetaData(self._db_connector.engine)

        # Creates a new table object with the column names and types provided
        new_table = Table(self._table_name, meta, *self._columns)
        new_table.create()
        return new_table
