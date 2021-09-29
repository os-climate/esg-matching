""" Custom exceptions of the esgmatching.reader.
The methods available in the library must raise one of the exceptions below if the code fails.
"""


class EsgMatchingError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class MappingFileDoesNotExist(EsgMatchingError):
    """ The mapping file does not exist in the given folder."""

    message = 'The mapping file does not exist in the given folder.'


class FileDoesNotExist(EsgMatchingError):
    """ The reading file does not exist in the given folder."""

    message = 'The reading file does not exist in the given folder.'


class IllFormedMappingFile(EsgMatchingError):
    """ The mapping file does not exist in the given folder."""

    message = 'The mapping file is ill formed or does not contain all information needed.'


class NotConnectedToDatabase(EsgMatchingError):
    """ The database engine is not connected to the database."""

    message = 'The database engine is not connected to the database.'


class ValueErrorDatabaseEngine(EsgMatchingError):
    """ The database engine was not specified or it is from a wrong type"""

    message = 'Database engine was not specified or it is from a wrong type'


class NoHeaderInFile(EsgMatchingError):
    """ The file has no header columns."""

    message = 'The file has no header columns.'


class ColumnsDifferFromMapping(EsgMatchingError):
    """ The columns in the file differ from those in the mapping file."""

    message = 'The columns in the file differ from those in the mapping file.'


class ErrorDB(EsgMatchingError):
    """ Errors returned from sqlaclchemy are  """
    pass


class EntityNotLinkedToDB(EsgMatchingError):
    """ The EntityDB is not connected to a table in the database. """
    message = 'The EntityDB is not connected to a table in the database.'


class IdxColumnIsMissing(EsgMatchingError):
    """ The EntityDB does not have an automatic idx column. Use the get_total_entries_by_column_name()
    to retrieve the total entries by a different column name. """
    message = 'The EntityDB does not have an automatic idx column. Use get_total_entries_by_column_name() ' + \
              'to retrieve the total entries by a different column name.'


class MatchingTableAlreadyExists(EsgMatchingError):
    """ The matching table already exists in the database. """
    message = 'The matching table already exists in the database.'


class IndirectMatchingMustBeExecutedAfterExactMatching(EsgMatchingError):
    """ The indirect matching must be performed only after an exact macthing. """
    message = 'The indirect matching must be performed only after an exact macthing.'