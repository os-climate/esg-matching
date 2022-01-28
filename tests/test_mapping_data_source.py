from unittest import TestCase, TestSuite, TextTestRunner

# Import the module for connection to a sqllite database
from esgmatching.db_engine.engines.connector_sql_lite import SqlLiteConnector

# Import the modules for file management
from esgmatching.file_reader.file import File
from esgmatching.file_reader.file_reader_csv import FileReaderCsv

# Import the modules for the etl processing: reading, transformation and loading data to a database
from esgmatching.processing.etl_processing import EtlProcessing

# Import query manager
from esgmatching.db_engine.executor.dql_manager import DqlManager


# Localization of the database to be created during the test
_DB_SQL_LITE = './data/sqlite/test_esg_matching.db'

_REF_JSON_FILE = './data/sqlite/test_referential1_sqlite.json'
_TGT1_JSON_FILE = './data/sqlite/test_target1_sqlite.json'
_TGT2_JSON_FILE = './data/sqlite/test_target2_sqlite.json'

# The database connector is represented by the class SqlLiteConnector
db_conn = SqlLiteConnector()


class TestDataSourceMapping(TestCase):
    """
    This is the TestCase class to verify the mapping of datasources
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

    def test_mapping_referential(self):
        # Create an ETL process object
        etl_proc_obj = EtlProcessing(db_conn)

        # Crete a file reader object for csv files
        csv_reader_obj = FileReaderCsv()

        # Load data from REFERENCIAL
        ref_obj = File(_REF_JSON_FILE)
        db_ref = etl_proc_obj.load_file_to_db(ref_obj, csv_reader_obj)

        # Retrieve the original attribute names (read from the csv file)
        expected_attributes = ['UNIQUE_ID', 'ISIN', 'COMPANY', 'COUNTRY']
        attributes = db_ref.get_attribute_names()
        self.assertListEqual(expected_attributes, attributes)

        # Retrieve the attribute names of the primary keys in the database table
        expected_pks = ['UNIQUE_ID']
        pks = db_ref.get_primary_keys()
        self.assertListEqual(expected_pks, pks)

    def test_mapping_target1(self):
        # Create an ETL process object
        etl_proc_obj = EtlProcessing(db_conn)

        # Crete a file reader object for csv files
        csv_reader_obj = FileReaderCsv()

        # Load data from TARGET 1
        tgt1_obj = File(_TGT1_JSON_FILE)
        db_tgt1 = etl_proc_obj.load_file_to_db(tgt1_obj, csv_reader_obj)

        # Retrieve the original attribute names (read from the csv file)
        expected_attributes = ['UNIQUE_ID', 'ISIN', 'LEI', 'COMPANY', 'COUNTRY']
        attributes = db_tgt1.get_attribute_names()
        self.assertListEqual(expected_attributes, attributes)

        # Retrieve the attribute names of the primary keys in the database table
        expected_pks = ['UNIQUE_ID']
        pks = db_tgt1.get_primary_keys()
        self.assertListEqual(expected_pks, pks)


def build_test_suite():
    # Create a pool of tests
    test_suite = TestSuite()
    test_suite.addTest(TestDataSourceMapping("test_sql_lite_connection"))
    test_suite.addTest(TestDataSourceMapping("test_mapping_referential"))
    test_suite.addTest(TestDataSourceMapping("test_mapping_target1"))

    return test_suite


def build_text_report():
    # Generate a tests report
    test_suite = build_test_suite()
    test_runner = TextTestRunner()
    test_runner.run(test_suite)


if __name__ == "__main__":
    build_text_report()
