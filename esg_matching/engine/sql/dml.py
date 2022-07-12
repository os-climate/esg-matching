""" Base class allows to perform DML (Data Manipulation Language) commands, such as: insert, update and delete """

# Import third-party libraries
import sqlalchemy as sa

# Import internal modules
from esg_matching.engine.connectors.base_connector import DbConnector


class DmlManager:
    """
        This class provides a base structure to execute DML commands, such as: insert, update and delete.

        Attributes:
            _db_connector (DbConnector): database connectors
    """

    def __init__(self, db_connector: DbConnector):
        self._db_connector = db_connector

    def delete_all_entries(self, table_obj: sa.Table, must_commit: bool = True):
        """
            Class method that removes all records in a table. Similar to the following SQL statement:
            DELETE * FROM table

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database
                must_commit (bool): indicates to commit the changes immediately

        """
        can_close_session = False
        if self._db_connector.session is None:
            self._db_connector.create_session()
            can_close_session = True
        num_rows_deleted = self._db_connector.session.query(table_obj).delete()
        if must_commit:
            self._db_connector.commit_changes()
        if can_close_session:
            self._db_connector.close_session()
        if num_rows_deleted is None:
            return 0
        else:
            return num_rows_deleted

    def insert_row(self, table_obj, data):
        """
            Class method that insert a row of values to a database table.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database
                data (dict): a list with all the values to be inserted in the table as follows:
                    [{'col1': 'value', 'col2': 'value'},{'col1': 'value', 'col2': 'value'}]

        """
        insert_stmt = table_obj.insert().values(data)
        if self._db_connector.session is None:
            self._db_connector.engine.execute(insert_stmt)
        else:
            self._db_connector.session.execute(insert_stmt)

    def insert_into_from_select(self, table_obj, columns_obj, select_obj):
        """
            Class method that insert several rows to a database table by using a select clause, based on the selection
            of attribute values from other table sources.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database
                columns_obj (list): a list with all the column reference (column object) found in the target table
                that will receive the values retrieved from the select clause.
                select_obj (select_query): a select clause from sqlalchemy

        """
        insert_stm = table_obj.insert().from_select(columns_obj, select_obj)
        if self._db_connector.session is None:
            self._db_connector.engine.execute(insert_stm)
        else:
            self._db_connector.session.execute(insert_stm)

    def delete_data(self, delete_stm):
        if self._db_connector.session is None:
            self._db_connector.engine.execute(delete_stm)
        else:
            self._db_connector.session.execute(delete_stm)

    def execute_sql_statement(self, sql_stm):
        """
            Class method that executes a sqlalchemy statement. It can be used to execute complex delete, update or
                insert statement that is step by step built with the use of the builders.

            Parameters:
                sql_stm: sqlalchemy statement that does not return a value, to be executed in the database.

        """
        if self._db_connector.session is None:
            self._db_connector.engine.execute(sql_stm)
        else:
            self._db_connector.session.execute(sql_stm)