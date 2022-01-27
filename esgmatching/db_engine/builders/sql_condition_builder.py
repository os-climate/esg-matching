""" Base class allows to build a SQL condition statement step by step """

import sqlalchemy.sql as sql


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
        """
            Constructor method.

            Parameters:
                No parameters required.

            Returns:
                SqlConditionBuilder (object)

            Raises:
                No exception is raised.
        """
        self._sql_condition = None

    def create_condition(self):
        """
            Starts the creation of a new conditional statement.

            Parameters:
                No parameters required.

            Returns:
                (self): return the reference to itself as to allow to execute commands in cascade

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
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

            Raises:
                No exception is raised.
        """
        return self._sql_condition

    def clean(self):
        """
            Reset the class attribute as to prepare to build a new conditional statement.

            Parameters:
                 No parameters required.

            Returns:
                No return values.

            Raises:
                No exception is raised.
        """
        self._sql_condition = None