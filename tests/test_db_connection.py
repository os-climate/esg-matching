from unittest import TestCase, TestSuite, TextTestRunner

# Import the module for connection to a sqllite database
from esg_matching.engine.connectors.sql_lite import SqlLiteConnector


# Localization of the database to be created during the test
_DB_SQL_LITE = './data/sqlite/test_esg_matching.db'

# The database connector is represented by the class SqlLiteConnector
db_conn = SqlLiteConnector()


class TestDbConnection(TestCase):
    """
    This is the TestCase class to test database connection
    """

    # Class level setup function, executed once and before any tests function
    @classmethod
    def setUpClass(cls):
        global db_conn

        # The connect() method of the SqlLiteConnector is used to stablish a connection with the database if it exists,
        # or to create a new one. The property path_db defines the location and name of the database.
        db_conn.path_db = _DB_SQL_LITE
        db_conn.show_sql_statement = False
        db_conn.connect()

    # Test sql lite connection
    def test_sql_lite_connection(self):
        # Check if the connection was stablished
        self.assertTrue(db_conn.is_connected())


def build_test_suite():
    # Create a pool of tests
    test_suite = TestSuite()
    test_suite.addTest(TestDbConnection("test_sql_lite_connection"))
    return test_suite


def build_text_report():
    # Generate a tests report
    test_suite = build_test_suite()
    test_runner = TextTestRunner()
    test_runner.run(test_suite)


if __name__ == "__main__":
    build_text_report()
