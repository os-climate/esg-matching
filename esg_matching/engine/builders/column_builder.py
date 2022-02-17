""" Base class to create database columns object according to the dialect of a given database connectors """

# Import third-party libraries
import sqlalchemy as sa

# Import internal modules
from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.exceptions import exceptions_db_engine, exceptions_builders


class ColumnBuilder:
    """
        This base class provides the infrastructure needed to create database columns object that can be used
        to create tables and/or manipulate database objects.
        The methods of ColumnBuilder must be called in sequence as in the examples bellow:
            db_col1 = my_column_builder.create_column('simple_pk').set_type('str',12).is_primary_key(True).build()
            db_col2 = my_column_builder.create_column('auto_pk').is_auto_primary_key(True).build()
            db_col3 = my_column_builder.create_column('auto_timestamp').is_auto_timestamp(True).build()

        Attributes:
            _db_connector (DbConnector): a database connectors
            _column_name (str): the column name to be created
            _column_db_type (str): the column type to be created
            _column_is_pk (bool): indicates if the column to be created is a primary key column
            _column_is_auto_pk (bool): indicates if the column to be created is auto incremented primary key
            _column_is_auto_timestamp (bool): indicates if the column to be created is an automatic timestamp column
    """

    def __init__(self, db_connector: DbConnector):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): a database connectors used to create columns according to its dialect

            Returns:
                ColumnBuilder (object)

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
        self._column_name = ''
        self._column_db_type = None
        self._column_is_pk = False
        self._column_is_auto_pk = False
        self._column_is_auto_timestamp = False

    def clean(self):
        """
            Class method used to clean up the builders memory as to create a new column from scratch.

            Parameters:
                No parameters required.

            Returns:
                No return values

            Raises:
                No exception is raised.
        """
        self._column_name = ''
        self._column_db_type = None
        self._column_is_pk = False
        self._column_is_auto_pk = False
        self._column_is_auto_timestamp = False

    def create_literal_column(self, literal_value: str, label_name: str):
        """
            Sets up a literal column which is defined by a standard value and a label. For instance:
                'any-value' (STRING) as my_label

            Parameters:
                literal_value (str): the standard value of the column.
                label_name (str): a label for the column

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the connectors

            Raises:
                No exception is raised.
        """
        db_str_data_type = self._db_connector.get_column_type('str')
        literal_value = f"'{literal_value}'"
        column_object = sa.sql.expression.literal_column(literal_value, db_str_data_type).label(label_name)
        return column_object

    def create_column(self, column_name: str):
        """
            Sets up the column name used by builders.

            Parameters:
                column_name (str): the column name.

            Returns:
                No return values

            Raises:
                No exception is raised.
        """
        self.clean()
        self._column_name = column_name
        return self

    def set_type(self, column_type: str, column_size: int = 0):
        """
            Sets up the column type and size used by builders.

            Parameters:
                column_type (str): the column type.
                column_size (int): the column size (by default is not defined = 0)

            Returns:
                No return values

            Raises:
                ColumnNameNotDefine: when the create_column() method was not called before this method.
        """
        if self._column_name == '':
            raise exceptions_builders.ColumnNameNotDefine
        self._column_db_type = self._db_connector.get_column_type(column_type, column_size)
        return self

    def is_primary_key(self, is_pk: bool):
        """
            Indicates if the column is a primary key column.

            Parameters:
                is_pk (bool): True if it is a primary key or False otherwise.

            Returns:
                No return values

            Raises:
                ColumnNameNotDefine: when the create_column() method was not called before this method.
        """
        if self._column_name == '':
            raise exceptions_builders.ColumnNameNotDefine
        self._column_is_pk = is_pk
        return self

    def is_auto_primary_key(self, is_auto_primary_key: bool):
        """
            Indicates if the column is an autoincremental primary key column.

            Parameters:
                is_auto_primary_key (bool): True if it is a auto primary key or False otherwise.

            Returns:
                No return values

            Raises:
                ColumnNameNotDefine: when the create_column() method was not called before this method.
        """
        if self._column_name == '':
            raise exceptions_builders.ColumnNameNotDefine
        self._column_is_auto_pk = is_auto_primary_key
        return self

    def is_auto_timestamp(self, is_auto_timestamp: bool):
        """
            Indicates if the column is an automatic timestamp column.

            Parameters:
                is_auto_timestamp (bool): True if it is a auto timestamp column or False otherwise.

            Returns:
                No return values

            Raises:
                ColumnNameNotDefine: when the create_column() method was not called before this method.
        """
        if self._column_name == '':
            raise exceptions_builders.ColumnNameNotDefine
        self._column_is_auto_timestamp = is_auto_timestamp
        return self

    def build(self):
        """
            Creates the column according to the database dialect of the connectors object.

            Parameters:
                No parameters required.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the connectors

            Raises:
                No exception is raised.
        """
        if self._column_name == '':
            raise exceptions_builders.ColumnNameNotDefine
        if self._column_is_auto_pk:
            column_object = self._create_auto_pk_column()
        elif self._column_is_auto_timestamp:
            column_object = self._create_auto_timestamp_column()
        else:
            column_object = self._create_column()
        return column_object

    def _create_column(self):
        """
            Private method that creates a database column object with the type and size supported by the connectors.

            Parameters:
                No parameters required.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the connectors.

            Raises:
                No exception is raised.
        """
        # If the column type was not specified, then call the set_type() method to create a default database type
        # related to the current connectors
        if self._column_db_type is None:
            self.set_type('str')
        column_object = sa.Column(self._column_name, self._column_db_type, primary_key=self._column_is_pk)
        return column_object

    def _create_auto_pk_column(self):
        """
            Private method that creates a database column object according to the dialect of the current connectors.

            Parameters:
                No parameters.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the connectors.

            Raises:
                No exception is raised.
        """
        column_object = self._db_connector.get_auto_pk_column(self._column_name)
        return column_object

    def _create_auto_timestamp_column(self):
        """
            Private method that creates a database column object according to the dialect of by the connectors.

            Parameters:
                No parameters.

            Returns:
                column_object (sqlalchemy.sql.schema.Column): a database column object supported by the connectors.

            Raises:
                No exception is raised.
        """
        column_object = self._db_connector.get_auto_timestamp_column(self._column_name)
        return column_object
