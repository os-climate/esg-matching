""" Base class allows to build a DELETE statement step by step """

import sqlalchemy.sql as sql


class DeleteBuilder:
    """
        This class provides a base structure to build a DELETE statement step by step.

        Attributes:
            _from_table_obj (sqlalchemy.sql.schema.Table): the table in the database to delete from
            _where_in_subquery (sqlalchemy.sql.expression.Delete.where): a select where clause to apply IN operator
            _column_where_in (sqlalchemy.sql.schema.Column): the database column to apply IN operator
            _where_condition (sqlalchemy.sql.expression.Delete.where): a simple where clause
    """

    def __init__(self):
        """
            Constructor method.

            Parameters:
                No parameters required.

            Returns:
                DeleteBuilder (object)

            Raises:
                No exception is raised.
        """
        self._from_table_obj = None
        self._where_in_subquery = None
        self._column_where_in = None
        self._where_condition = None
        self._delete_stm = None

    def create_delete(self):
        """
            Starts the creation of a new delete statement.

            Parameters:
                No parameters required.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self.clean()
        return self

    def from_table(self, table_obj):
        """
            Defines the table to delete from.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): the table in the database to delete from

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._from_table_obj = table_obj
        return self

    def where_condition(self, where_condition):
        """
            Defines the where condition of the delete statement.

            Parameters:
                where_condition (sqlalchemy.sql.expression.Delete.where): a simple where clause

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._where_condition = where_condition
        return self

    def where_in_condition(self, column_obj,  where_in_select):
        """
            Defines a where condition that makes use of the IN operator, such as:
                column_obj IN (subquery)

            Parameters:
                 column_obj (sqlalchemy.sql.schema.Column): the database column to apply the IN operator
                 where_in_select (sqlalchemy.sql.expression.Delete.where): the subquery to apply the IN operator

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._column_where_in = column_obj
        self._where_in_subquery = where_in_select
        return self

    def build_statement(self):
        """
            Build the whole DELETE statement which can be of two types:
                - DELETE FROM table_obj WHERE where_condition
                - DELETE FROM table_obj WHERE column_where_in IN (SELECT column_select....)

            Parameters:
                 No parameters required.

            Returns:
                delete_stm (sqlalchemy.sql.expression.Delete): the DELETE statement to be executed

            Raises:
                No exception is raised.
        """
        delete_stm = sql.delete(self._from_table_obj)
        if self._where_in_subquery is not None:
            subquery_stm = self._column_where_in.in_(self._where_in_subquery)
            delete_stm = delete_stm.where(subquery_stm)
        elif self._where_condition is not None:
            delete_stm = delete_stm.where(self._where_condition)

        self._delete_stm = delete_stm
        return delete_stm

    def get_statement(self):
        """
            Returns the current built in statement.

            Parameters:
                 No parameters required.

            Returns:
                delete_stm (sqlalchemy.sql.expression.Delete): the current DELETE statement to be executed

            Raises:
                No exception is raised.
        """
        return self._delete_stm

    def clean(self):
        """
            Reset the class attribute as to prepare to build a new DELETE statement.

            Parameters:
                 No parameters required.

            Returns:
                No return values.

            Raises:
                No exception is raised.
        """
        self._delete_stm = None
        self._from_table_obj = None
        self._where_in_subquery = None
        self._column_where_in = None
        self._where_condition = None