""" Base class allows to build a SELECT statement step by step """


import sqlalchemy.sql as sql


class SelectBuilder:
    """
        This class provides a base structure to build a SELECT statement step by step.
        The SelectBuilder() allows the creation of SELECT statements that make use of JOIN or simple WHERE clauses:
            - SELECT col1, col2,... FROM table1 WHERE condition1 AND condition2
            - SELECT col1, col2,... FROM table1 JOIN table2 ON condition1 AND condition2

        Attributes:
            _columns (list): a list of sqlalchemy.sql.schema.Column objects
            _from_table_obj (sqlalchemy.sql.schema.Table): the table to select from
            _join_table_obj (sqlalchemy.sql.schema.Table): the table to join to
            _type_join (str): the type of join ('join', 'left' or 'right')
            _join_condition (sqlalchemy.sql.expression): JOIN condition created with SqlConditionBuilder()
            _where_condition (sqlalchemy.sql.expression): WHERE condition created with SqlConditionBuilder()
    """

    def __init__(self):
        """
            Constructor method.

            Parameters:
                No parameters required.

            Returns:
                SelectBuilder (object)

            Raises:
                No exception is raised.
        """
        self._columns = []
        self._from_table_obj = None
        self._join_table_obj = None
        self._type_join = 'join'
        self._join_condition = None
        self._where_condition = None

    def create_select(self, columns_select: list):
        """
            Starts the creation of a new select statement.

            Parameters:
                No parameters required.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._columns = columns_select
        return self

    def from_table(self, table_obj):
        """
            Defines the table to select from.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): the table in the database to select from

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._from_table_obj = table_obj
        return self

    def join_table(self, table_obj):
        """
            Defines the table to join on.

            Parameters:
                table_obj (sqlalchemy.sql.schema.Table): the table in the database to join on

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._join_table_obj = table_obj
        return self

    def join_on(self, join_condition, type_join='join'):
        """
            Defines the join clause condition of the select statement.

            Parameters:
                join_condition (sqlalchemy.sql.expression): JOIN condition created with SqlConditionBuilder()
                type_join (str): the type of join ('join', 'left' or 'right')

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._join_condition = join_condition
        self._type_join = type_join
        return self

    def where_condition(self, where_condition):
        """
            Defines the where clause condition of the select statement.

            Parameters:
                where_condition (sqlalchemy.sql.expression): WHERE condition created with SqlConditionBuilder()

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
        """
        self._where_condition = where_condition
        return self

    def build_statement(self):
        """
            Build the whole SELECT statement, for instance:
            - SELECT col1, col2,... FROM table1 WHERE condition1 AND condition2
            - SELECT col1, col2,... FROM table1 JOIN table2 ON condition1 AND condition2

            Parameters:
                 No parameters required.

            Returns:
                select_stm (sqlalchemy.sql.expression.Delete): the SELECT statement to be executed

            Raises:
                No exception is raised.
        """
        if self._join_condition is not None:
            if self._type_join == 'left':
                var_isouter = True
            else:
                var_isouter = False
            join_stm = self._from_table_obj.join(self._join_table_obj, self._join_condition, isouter=var_isouter)
            select_stm = sql.select(self._columns).select_from(join_stm)
        else:
            select_stm = sql.select(self._columns).select_from(self._from_table_obj)

        if self._where_condition is not None:
            select_stm = select_stm.where(self._where_condition)

        return select_stm

    def clean(self):
        """
            Reset the class attributes as to prepare to build a new SELECT statement.

            Parameters:
                 No parameters required.

            Returns:
                No return values.

            Raises:
                No exception is raised.
        """
        self._columns = []
        self._from_table_obj = None
        self._join_table_obj = None
        self._type_join = 'join'
        self._join_condition = None
        self._where_condition = None