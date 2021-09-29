""" This module defines the class SqlEngine """

# Import python libraries

# Import third-party libraries
import pandas as pd

import sqlalchemy as db
import sqlalchemy.sql as sql
from sqlalchemy.orm import sessionmaker, scoped_session

# Import internal libraries
import esgmatching.dbmanager.SqlUtility as SqlUtility


class SqlEngine:
    """
        This class allows the creation of a data base engine as to execute operations (create table, insert/update data
        execute query, etc.) in a database.
    """

    def __init__(self, str_connection):
        """
            Constructor method.

            Parameters:
                str_connection (string): the connection string used to connect to the database. The connection string
                is defined by the type of the database being used. Therefore, no pattern is expected or checked here.

            Returns:
                SqlEngine (object)
            Raises:
                No exception is raised.
        """

        self._str_connection = str_connection
        self._engine = None
        self._current_session = None
        self._Session = None

    def connect(self, show_echo=False):
        """
            Class method that creates a connection to the database.

            Parameters:
                show_echo (bool): a boolean flag that indicates if all the operations performed in the database
                must use a verbose mode...meaning that database messages will be flushed to the default output.

            Returns:

            Raises:
                No exception is raised.
        """
        # Connect to the db engine
        self._engine = db.create_engine(self._str_connection, echo=show_echo)

    def disconnect(self):
        """
            Class method that closes a connection with the database.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """

        if self._current_session:
            self.close_session()

        if self._engine:
            self._engine.dispose()
            self._engine = None

    def is_connected(self):
        """
            Class method that checks if a database connection exists.

            Parameters:

            Returns: True or False

            Raises:
                No exception is raised.
        """
        if self._engine:
            return True
        else:
            return False

    def change_db_connection(self, str_connection):
        """
            Class method that changes the connection setting with the database.
            This causes the current session being terminated and the current connection being disposed.

            Parameters:
                str_connection (string): the connection string used to connect to the database. The connection string
                is defined by the type of the database being used. Therefore, no pattern is expected or checked here.

            Returns:

            Raises:
                No exception is raised.
        """
        if self._current_session:
            self.close_session()

        if self._engine:
            self._engine.dispose()

        self._str_connection = str_connection
        self.connect()

    def create_session(self):
        """
            Class method that creates a new database session, so that all operations are performed in a batch mode.
            As a consequence of using this method, the operations performed are concluded only after a commit or a
            rollback call is performed.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """
        # Create a new session only if one doesn't exist already
        if not self._current_session:
            # Prepare the db engine to work with sessions
            # This creates a scoped session object, meaning that all objects
            # created or manipulated during the session lifetime belongs to
            # the same context/scope
            session_factory = sessionmaker(bind=self._engine)
            self._Session = scoped_session(session_factory)
            self._current_session = self._Session()

    def close_session(self):
        """
            Class method that closes the current session.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """
        if self._current_session:
            self._current_session.close()
            self._current_session = None
            self._Session = None

    def commit_changes(self):
        """
            Class method that commits (confirm) the operations executed during a current session.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """
        if self._current_session:
            self._current_session.commit()

    def rollback_changes(self):
        """
            Class method that rollback (cancel out) the operations executed during a current session.

            Parameters:

            Returns:

            Raises:
                No exception is raised.
        """
        if self._current_session:
            self._current_session.rollback()

    def create_table(self, table_name, columns_names, columns_types=None, add_idx=True, add_timestamp=False):
        """
            Class method that creates a table in the database.

            Parameters:
                table_name (string): name of the table being created
                columns_names (list): a list with all the column names for the table being created
                columns_types (list): a list with all the column types for the table being created
                add_idx (bool)
                add_timestamp (bool)

            Returns:
                new_table (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                No exception is raised.
        """
        # Define the metadata as default
        meta = db.MetaData(self._engine)

        # If types are not provided as a column type list, then define String type (VARCHAR) for all columns
        if not columns_types:
            columns_types = [db.String] * len(columns_names)

        # Initializes idx and timestamp column objects
        idx_column_obj = None
        timestamp_column_obj = None

        # Creates an autoincrement idx column object, if requested
        if add_idx:
            idx_column_obj = db.Column('idx', db.Integer, primary_key=True, autoincrement=True)

        # Creates a automatic timestamp column object, if requested
        if add_timestamp:
            timestamp_column_obj = db.Column('timestamp', db.DateTime, server_default=db.func.now())

        # Creates a new table object with the column names and types provided
        if add_idx and not add_timestamp:
            new_table = db.Table(table_name, meta, idx_column_obj,
                                 *(db.Column(column_name, column_type)
                                   for column_name, column_type in zip(columns_names, columns_types)))
        elif not add_idx and add_timestamp:
            new_table = db.Table(table_name, meta, timestamp_column_obj,
                                 *(db.Column(column_name, column_type)
                                   for column_name, column_type in zip(columns_names, columns_types)))
        elif add_idx and add_timestamp:
            new_table = db.Table(table_name, meta, idx_column_obj, timestamp_column_obj,
                                 *(db.Column(column_name, column_type)
                                   for column_name, column_type in zip(columns_names, columns_types)))
        else:
            new_table = db.Table(table_name, meta,
                                 *(db.Column(column_name, column_type)
                                   for column_name, column_type in zip(columns_names, columns_types)))

        new_table.create()
        return new_table

    def get_table_from_metadata(self, table_name):
        """
            Class method that retrieves the metadata object that represents the table in the database.
            This metadata object is used to perform operations using the columns of this table.

            Parameters:
                table_name (string): name of the table

            Returns:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                No exception is raised.
        """
        # Create MetaData instance
        metadata = db.MetaData(self._engine)
        # Get Table
        table_obj = db.Table(table_name, metadata, autoload=True, autoload_with=self._engine)
        return table_obj

    def drop_table(self, table_obj):
        """
            Class method that removes the table from the database.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Returns:

            Raises:
                No exception is raised.
        """
        table_obj.drop(self._engine)

    def table_exists(self, table_name):
        """
            Class method that checks if a table exists in the database.

            Parameters:
                table_name (string): name of the table

            Returns: True or False

            Raises:
                No exception is raised.
        """
        inspector = db.inspect(self._engine)
        return table_name in inspector.get_table_names()

    def query_table(self, table_obj):
        """
            Class method that query the whole content of a table in the database.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Returns:
                result_data (list): a list with the result of the query. Every item of the list is a row of the
                database table structred as a tupple object whose elements are the table columns.
                example - list with 4 rows and 5 columns:
                    [('SKYNET', 'GERMANY', 'DE0005545503', '5299003VKVDCUPSS5X23', '5734672'),
                     ('WAYNE ENTERPRISES', 'ITALY', 'IT0005083180', '549300RV0WBR05UTDI91', ''),
                     ('SKYNET', 'GERMANY', 'DE0005545503', '5299003VKVDCUPSS5X23', '5734672'),
                     ('WAYNE ENTERPRISES', 'ITALY', 'IT0005083180', '549300RV0WBR05UTDI91', '')]

            Raises:
                No exception is raised.
        """
        query_table = db.select([table_obj])
        connection = self._engine.connect()
        result_proxy = connection.execute(query_table)
        result_data = result_proxy.fetchall()
        connection.close()
        return result_data

    def get_count_by_column(self, column_object):
        # Create sql statement
        # SELECT COUNT(DISTINCT Column) FROM Table.Column
        can_close_session = False
        if self._current_session is None:
            self.create_session()
            can_close_session = True
        result_data = self._current_session.query(sql.func.count(sql.distinct(column_object))).first()
        if can_close_session:
            self.close_session()
        if result_data is None:
            return 0
        else:
            return result_data[0]

    def get_count_non_null_by_column(self, column_object):
        # Create sql statement
        # SELECT COUNT(Column) FROM Table.Column WHERE Column IS NOT NULL
        can_close_session = False
        if self._current_session is None:
            self.create_session()
            can_close_session = True
        filter_clause = SqlUtility.get_function_where_clause(column_object, 'not_null')
        result_data = self._current_session.query(sql.func.count(column_object)) \
            .filter_by(filter_clause) \
            .first()
        if can_close_session:
            self.close_session()
        if result_data is None:
            return 0
        else:
            return result_data[0]

    def get_count_by_column_where_literal(self, column_object, where_literal):
        # Create sql statement
        # SELECT COUNT(Column) FROM Table.Column WHERE Column == 'literal'
        can_close_session = False
        if self._current_session is None:
            self.create_session()
            can_close_session = True
        filter_clause = sql.expression.ColumnOperators.__eq__(column_object, db.text(where_literal))
        result_data = self._current_session.query(sql.func.count(column_object)) \
            .where(filter_clause) \
            .first()
        if can_close_session:
            self.close_session()
        if result_data is None:
            return 0
        else:
            return result_data[0]

    def get_df_from_table(self, table_obj):
        """
            Class method that retrieves a data frame object from a database table.
            This method can be used to perform quick validations in jupyter notebooks.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): the table in the database.

            Returns:
                df_table (pandas dataframe): a pandas dataframe representing a full select in the table object

            Raises:
                No exception is raised.
        """
        query_table = db.select([table_obj])
        df_table = pd.read_sql_query(query_table, self._engine)
        return df_table

    def insert_row(self, table_obj, data):
        """
            Class method that insert a row of values to a database table.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database
                data (list): a list with all the values to be inserted in the table

            Returns:

            Raises:
                No exception is raised.
        """
        insert_stmt = table_obj.insert().values(data)
        if self._current_session:
            self._current_session.execute(insert_stmt)
        else:
            self._engine.execute(insert_stmt)

    def update_where_is_null(self, table_obj, column_is_null, value):
        # print(ColumnOperators.__eq__(column_is_null, value))
        # update_stm = table_obj.update().where(column_is_null.is_(None))\
        #                                .values(**{column_is_null: 1})
        update_stm = table_obj.update().where(column_is_null.is_(None)).values({column_is_null.name: value})
        if self._current_session is not None:
            self._current_session.execute(update_stm)
        else:
            self._engine.execute(update_stm)

    def insert_into_from_select(self, table_obj, columns_obj, select_obj):
        """
            Class method that insert several rows to a database table by using a select clause, based on the selection
            of attribute values from other table sources.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database
                columns_obj (list): a list with all the column reference (column object) found in the target table
                that will receive the values retrieved from the select clause.
                select_obj (select_query): a select clause from sqlalchemy

            Returns:

            Raises:
                No exception is raised.
        """
        insert_stm = table_obj.insert().from_select(columns_obj, select_obj)
        if self._current_session is not None:
            self._current_session.execute(insert_stm)
        else:
            self._engine.execute(insert_stm)