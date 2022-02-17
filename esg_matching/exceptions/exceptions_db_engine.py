class DbEngineError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class ConnectionAttributesNotDefined(DbEngineError):
    """The connection attributes are incomplete or not defined."""

    message = "The connection attributes are incomplete or not defined."


class ConnectionStringNotDefined(DbEngineError):
    """ The connection string is not defined. Check the connection parameters for the database in use."""

    message = "The connection string is not defined. Check the connection parameters for the database in use."


class ConnectionNotDefined(DbEngineError):
    """ There is no connection stablished with the database. Check the connect() method for your connectors. """

    message = "There is no connection stablished with the database. Check the connect() method for your connectors."


class SessionAlreadyExists(DbEngineError):
    """ A session already exists for this connectors. """

    message = "A session already exists for this connectors."


class SessionTypeNotSupported(DbEngineError):
    """ The session type is not supported. """

    message = "The session type is not supported."


class NoSessionOpen(DbEngineError):
    """ There is no session available. """

    message = "There is no session available."


class DataTypeNotSupported(DbEngineError):
    """The data type is not supported."""

    message = "The data type is not supported."


class OracleClientDriverLibNotDefined(DbEngineError):
    """The oracle client driver directory was not defined."""

    message = "The oracle client driver directory was not defined."