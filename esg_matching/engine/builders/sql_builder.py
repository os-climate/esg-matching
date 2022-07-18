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
        self._from_table_obj = None
        self._where_in_subquery = None
        self._column_where_in = None
        self._where_condition = None
        self._delete_stm = None

    def create_delete(self):
        """
            Starts the creation of a new delete statement.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade
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

            Returns:
                delete_stm (sqlalchemy.sql.expression.Delete): the current DELETE statement to be executed

        """
        return self._delete_stm

    def clean(self):
        """
            Reset the class attribute as to prepare to build a new DELETE statement.

        """
        self._delete_stm = None
        self._from_table_obj = None
        self._where_in_subquery = None
        self._column_where_in = None
        self._where_condition = None


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

        """
        self._columns = []
        self._from_table_obj = None
        self._join_table_obj = None
        self._type_join = 'join'
        self._join_condition = None
        self._where_condition = None


class SqlConditionBuilder:
    """
        This class provides a base structure to build a SQL condition statement step by step. The condition can be
            used in a JOIN or WHERE clauses and can be of the following types:
            - COLUMN EQUALITY: column_a = column_b
            - COLUMN DIFFERENCE: column_a != column_b
            - VALUE EQUALITY: column_a = value
            - VALUE DIFFERENCE: column_a != value
            - IS NULL: column_a IS NULL
            - IS NOT NULL: column_a IS NOT NULL
        The sql condition can be composed of multiple conditions by using the condition operators AND/OR, such as:
            column_a = column_b AND column_c = column_d
            column_a = column_b OR column_c = column_d

        Attributes:
            self._sql_condition (sqlalchemy.sql.expression): a sql expression/condition

    """

    def __init__(self):
        self._sql_condition = None

    def create_condition(self):
        """
            Starts the creation of a new conditional statement.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self.clean()
        return self

    def equal_cols(self, left_column, right_column):
        """
            Creates a 'COLUMN EQUALITY' condition: column_a = column_b

            Parameters:
                left_column (sqlalchemy.sql.schema.Column): the condition column on the left
                right_column (sqlalchemy.sql.schema.Column): the condition column on the right

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = sql.expression.ColumnOperators.__eq__(left_column, right_column)
        return self

    def not_equal_cols(self, left_column, right_column):
        """
            Creates a 'COLUMN DIFFERENCE' codition: column_a != column_b

            Parameters:
                left_column (sqlalchemy.sql.schema.Column): the condition column on the left
                right_column (sqlalchemy.sql.schema.Column): the condition column on the right

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = sql.expression.ColumnOperators.__ne__(left_column, right_column)
        return self

    def equal_value(self, column_obj, value):
        """
            Creates a 'VALUE EQUALITY' codition: column_a = value

            Parameters:
                column_obj (sqlalchemy.sql.schema.Column): the condition column
                value (str): the condition value

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        value_db = value
        self._sql_condition = sql.expression.ColumnOperators.__eq__(column_obj, value_db)
        return self

    def not_equal_value(self, column_obj, value):
        """
            Creates a 'VALUE DIFFERENCE' codition: column_a != value

            Parameters:
                column_obj (sqlalchemy.sql.schema.Column): the condition column
                value (str): the condition value

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        value_db = value
        self._sql_condition = sql.expression.ColumnOperators.__ne__(column_obj, value_db)
        return self

    def is_null(self, column_obj):
        """
            Creates a 'IS NULL' condition: column_a IS NULL

            Parameters:
                column_obj (sqlalchemy.sql.schema.Column): the condition column

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = column_obj.is_(None)
        return self

    def is_not_null(self, column_obj):
        """
            Creates a 'IS NOT NULL' condition: column_a IS NOT NULL

            Parameters:
                column_obj (sqlalchemy.sql.schema.Column): the condition column

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = column_obj.isnot(None)
        return self

    def and_condition(self, previous_condition):
        """
            Concatenates two conditions by using the operator AND.

            Parameters:
                previous_condition (sqlalchemy.sql.expression): a previous condition to be concatenated to the actual
                    one by using the AND operator.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = sql.and_(previous_condition, self._sql_condition)
        return self

    def or_condition(self, previous_condition):
        """
            Concatenates two conditions by using the operator OR.

            Parameters:
                previous_condition (sqlalchemy.sql.expression): a previous condition to be concatenated to the actual
                    one by using the OR operator.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

        """
        self._sql_condition = sql.or_(previous_condition, self._sql_condition)
        return self

    def get_condition(self):
        """
            Returns the current conditional statement.

            Parameters:
                 No parameters required.

            Returns:
                self._sql_condition (sqlalchemy.sql.expression): the current conditional statement

        """
        return self._sql_condition

    def clean(self):
        """
            Reset the class attribute as to prepare to build a new conditional statement.

        """
        self._sql_condition = None