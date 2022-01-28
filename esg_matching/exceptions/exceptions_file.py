class FileError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class NoHeaderInFile(FileError):
    """ The file has no header columns."""

    message = 'The file has no header columns.'


class ColumnsToReadDifferFromFileHeader(FileError):
    """ The columns to read differ from those in the file header."""

    message = 'The columns to read differ from those in the file header.'


class FileTypeNotSupportedByETL(FileError):
    """ The file type is not supported by the ETL process."""

    message = 'The file type is not supported by the ETL process.'


class FileTypeNotSupportedByReader(FileError):
    """ The file type is not supported by the reader given as parameter."""

    message = 'The file type is not supported by the reader given as parameter.'


class FilePathNotFoundOrUnreachable(FileError):
    """ The file path was not found or is unreachable."""

    message = 'The file path was not found or is unreachable.'


class FileNotFoundOrUnreachable(FileError):
    """ The file was not found or is unreachable."""

    message = 'The file was not found or is unreachable.'
