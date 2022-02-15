class DbEngineBuildersError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class ColumnNameNotDefine(DbEngineBuildersError):
    """The column name was not defined. Use create_column() method for that."""

    message = "The column name was not defined. Use create_column() method for that."


class TableNameNotDefine(DbEngineBuildersError):
    """The table name is not defined. Use create_table() method for that."""

    message = "The table name is not defined. Use create_table() method for that."


class TableColumnsNotDefined(DbEngineBuildersError):
    """The table columns are not defined. Use add_column() method or create the table from a dataframe"""

    message = "The table columns are not defined. Use add_column() method or create the table from a dataframe."


class MethodNotSupportedWhenCreateTableWithDataFrame(DbEngineBuildersError):
    """This method is not supported when creating a table from a dataframe."""

    message = "This method is not supported when creating a table from a dataframe."

class DataFrameIsRequired(DbEngineBuildersError):
    """This method is not supported when creating a table from a dataframe."""

    message = "This method is not supported when creating a table from a dataframe."