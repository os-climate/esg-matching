
class DataSourceError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class TableNameNotInformed(DataSourceError):
    """Table name was not informed"""

    message = "Table name was not informed"


class TableDoesNotExistInDatabase(DataSourceError):
    """Table does not exist in the database"""

    message = "Table does not exist in the database"


class DataSourceNotSynchronizedWithDbTable(DataSourceError):
    """Datasource is not synchronized with a database table"""

    message = "Datasource is not synchronized with a database table."


class AttributeNotInDataSource(DataSourceError):
    """Attribute not found in datasource. Check if the data source is synchronized with a table."""

    message = "Attribute not found in datasource. Check if the data source is synchronized with a table."


class AliasNotInDataSource(DataSourceError):
    """Alias not found in datasource. Check the list of alias available."""

    message = "Alias not found in datasource. Check the list of alias available."


class AttributesNotDefinedInDataSource(DataSourceError):
    """Attributes not defined. Check if the data source is synchronized with a table."""

    message = "Attributes not defined. Check if the data source is synchronized with a table."


class PrimaryKeysNotDefinedInDataSource(DataSourceError):
    """Primary keys not defined. Check if the data source is synchronized with a table."""

    message = "Primary keys not defined. Check if the data source is synchronized with a table."


class OriginalNameAlreadyMapped(DataSourceError):
    """The given original name was already mapped to a datasource attribute"""

    message = "The given original name was already mapped to a datasource attribute"


class AliasNameAlreadyMapped(DataSourceError):
    """The given alias was already mapped to a datasource attribute"""

    message = "The given alias was already mapped to a datasource attribute"


class StandardMatchingAttributeMissingInDatabaseTable(DataSourceError):
    """Some standard matching attributes are missing in the database table."""

    message = "Some standard matching attributes are missing in the database table"


class MatchingDataSourceTypeNotSupported(DataSourceError):
    """The matching data source type is not supported"""

    message = "The matching data source type is not supported"


class MatchingDataSourceTypeNotDefined(DataSourceError):
    """The matching data source type was not defined. Use the match_type property for that."""

    message = "The matching data source type was not defined. Use the match_type property for that."


class MatchingTypeInPolicyDefinitionNotSupported(DataSourceError):
    """The matching type in policy definition is not supported."""

    message = "The matching type in policy definition is not supported."


class PolicyDefinitionNotFound(DataSourceError):
    """The policy definition cannot be found in the datasource."""

    message = "The policy definition cannot be found in the datasource."


class AliasNotMappedToMatching(DataSourceError):
    """Some alias is not associated to the matching/no-matching table."""

    message = "Some alias is not associated to the matching/no-matching table."


class ColumnNamesAndColumnAliasesMustMatch(DataSourceError):
    """The lists of column names and aliases must match."""

    message = "The lists of column names and aliases must match."
