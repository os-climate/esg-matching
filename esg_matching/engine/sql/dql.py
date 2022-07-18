""" Base class to perform DQL (Data Query Language) commands, such as SELECT """

# Import third-party libraries
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.sql as sql

# Import internal modules
from esg_matching.engine.connectors.base_connector import DbConnector


class DqlManager:
    """
        This class provides a base structure to execute DQL commands, such as SELECT

        Attributes:
            _db_connector (DbConnector): database connectors
    """

    def __init__(self, db_connector: DbConnector):
        self._db_connector = db_connector

    def get_total_entries(self, table_obj):
        """
            Class method that count unique rows from a database table using a column as key.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Returns:
                0 : if there is no unique rows
                number (int): the total of unique rows

        """
        # Create sql statement
        # SELECT COUNT(*) FROM Table
        sql_stm = sa.select([sql.func.count()]).select_from(table_obj)
        if self._db_connector.session is None:
            result_data = self._db_connector.engine.execute(sql_stm).scalar()
        else:
            result_data = self._db_connector.session.execute(sql_stm).scalar()

        if result_data is None:
            return 0
        else:
            return result_data

    def get_total_entries_by_column(self, db_column_obj: list, distinct_values=False):
        """
            Class method that count unique rows from a database table using a column as key.

            Parameters:
                db_column_obj (sqlalchemy.sql.schema.Column): metadata object that represents a table column
                distinct_values (bool): count only the distinct values

            Returns:
                0 : if there is no unique rows
                number (int): the total of unique rows

        """
        # Create sql statement
        # SELECT COUNT(DISTINCT Column) FROM Table.Column
        can_close_session = False
        if self._db_connector.session is None:
            self._db_connector.create_session()
            can_close_session = True

        if distinct_values:
            result_data = self._db_connector.session.query(sql.func.count(sql.distinct(db_column_obj))).first()
        else:
            result_data = self._db_connector.session.query(sql.func.count(db_column_obj)).first()
        if can_close_session:
            self._db_connector.close_session()
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

        """
        query_table = sa.select([table_obj])
        df_table = pd.read_sql_query(query_table, self._db_connector.engine)
        return df_table

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

        """
        query_table = sa.select([table_obj])
        connection = self._db_connector.engine.connect()
        result_proxy = connection.execute(query_table)
        result_data = result_proxy.fetchall()
        connection.close()
        return result_data

    def query_by_sql_statement(self, sql_stm, as_pandas_df=False):
        """
            Class method that query the whole content of a table in the database.

            Parameters:
                sql_stm: sqlalchemy statement that does not return a value, to be executed in the database.
                as_pandas_df (bool): indicates if the result should be returned as pandas dataframe

            Returns:
                result_data (list): a list with the result of the query. Every item of the list is a row of the
                database table structred as a tupple object whose elements are the table columns.
                example - list with 4 rows and 5 columns:
                    [('SKYNET', 'GERMANY', 'DE0005545503', '5299003VKVDCUPSS5X23', '5734672'),
                     ('WAYNE ENTERPRISES', 'ITALY', 'IT0005083180', '549300RV0WBR05UTDI91', ''),
                     ('SKYNET', 'GERMANY', 'DE0005545503', '5299003VKVDCUPSS5X23', '5734672'),
                     ('WAYNE ENTERPRISES', 'ITALY', 'IT0005083180', '549300RV0WBR05UTDI91', '')]

        """
        connection = self._db_connector.engine.connect()
        if as_pandas_df:
            result_data = pd.read_sql(sql_stm, connection)
        else:
            result_proxy = connection.execute(sql_stm)
            result_data = result_proxy.fetchall()
        connection.close()
        return result_data