from unittest import TestCase, TestSuite, TextTestRunner

# Import the module for connection to a sqllite database
from esgmatching.db_engine.engines.connector_sql_lite import SqlLiteConnector

# Import the modules for file management
from esgmatching.file_reader.file import File
from esgmatching.file_reader.file_reader_csv import FileReaderCsv

# Import the modules for the etl processing: reading, transformation and loading data to a database
from esgmatching.processing.etl_processing import EtlProcessing

# Import policy module
from esgmatching.db_matcher.matching_policy import MatchingPolicy

# Import DFM, DRM and IFM modules
from esgmatching.db_matcher.db_matcher_dfm import DbMatcherDfm
from esgmatching.db_matcher.db_matcher_drm import DbMatcherDrm
from esgmatching.db_matcher.db_matcher_ifm import DbMatcherIfm

# Import query manager
from esgmatching.db_engine.executor.dql_manager import DqlManager


# Localization of the database to be created during the test
_DB_SQL_LITE = './data/sqlite/test_esg_matching.db'

_REF_JSON_FILE = './data/sqlite/test_referential1_sqlite.json'
_TGT1_JSON_FILE = './data/sqlite/test_target1_sqlite.json'
_TGT2_JSON_FILE = './data/sqlite/test_target2_sqlite.json'

_MATCH_JSON_FILE = './data/sqlite/test_matching_sqlite.json'
_NO_MATCH_JSON_FILE = './data/sqlite/test_no_matching_sqlite.json'

# The database connector is represented by the class SqlLiteConnector
db_conn = SqlLiteConnector()

# Datasource objects
db_ref = None
db_tgt1 = None
db_tgt2 = None
db_matching = None
db_no_matching = None
policy_match_tgt1 = None
policy_match_tgt2 = None


class TestDbExactMatching(TestCase):
    """
    This is the TestCase class for exact matching (direct, residual and indirect matching)
    """

    # Class level setup function, executed once and before any tests function
    @classmethod
    def setUpClass(cls):
        global db_conn
        global db_ref
        global db_tgt1
        global db_tgt2
        global db_matching
        global db_no_matching
        global policy_match_tgt1
        global policy_match_tgt2

        # The connect() method of the SqlLiteConnector is used to stablish a connection with the database if it exists,
        # or to create a new one. The property path_db defines the location and name of the database.
        db_conn.path_db = _DB_SQL_LITE
        db_conn.show_sql_statement = False
        db_conn.connect()

        # Create an ETL process object
        etl_proc_obj = EtlProcessing(db_conn)

        # Crete a file reader object for csv files
        csv_reader_obj = FileReaderCsv()

        # Load data from REFERENCIAL
        ref_obj = File(_REF_JSON_FILE)
        db_ref = etl_proc_obj.load_file_to_db(ref_obj, csv_reader_obj)

        # Load data from TARGET 1
        tgt1_obj = File(_TGT1_JSON_FILE)
        db_tgt1 = etl_proc_obj.load_file_to_db(tgt1_obj, csv_reader_obj)

        # Load data from TARGET 2
        tgt2_obj = File(_TGT2_JSON_FILE)
        db_tgt2 = etl_proc_obj.load_file_to_db(tgt2_obj, csv_reader_obj)

        # Create match and no-match
        file_match = File(_MATCH_JSON_FILE)
        file_no_match = File(_NO_MATCH_JSON_FILE)
        db_matching = etl_proc_obj.create_data_source(file_match)
        db_no_matching = etl_proc_obj.create_data_source(file_no_match)

        # Create macthing policy object for target1 and target2
        policy_match_tgt1 = MatchingPolicy(db_tgt1, 'matching_with_ref1')
        policy_match_tgt2 = MatchingPolicy(db_tgt2, 'matching_with_ref1')

        # Set the referential and matching/no-matching sources
        policy_match_tgt1.set_referential_source(db_ref)
        policy_match_tgt1.set_matching_source(db_matching)
        policy_match_tgt1.set_no_matching_source(db_no_matching)

        # Set the referential and matching/no-matching sources
        policy_match_tgt2.set_referential_source(db_ref)
        policy_match_tgt2.set_matching_source(db_matching)
        policy_match_tgt2.set_no_matching_source(db_no_matching)

    # Test sql lite connection
    def test_sql_lite_connection(self):
        # Check if the connection was stablished
        self.assertTrue(db_conn.is_connected())

    def test_dfm_matching(self):
        # Create a matcher object for DFM
        dfm_matcher_obj = DbMatcherDfm(db_conn)

        # Perform DFM on target 1 and 2
        dfm_matcher_obj.set_policy(policy_match_tgt1)
        dfm_matcher_obj.execute_matching()

        dfm_matcher_obj.set_policy(policy_match_tgt2)
        dfm_matcher_obj.execute_matching()

        # Checking the results on matching and no-matching tables
        db_query_manager = DqlManager(db_conn)
        total_entries_match = db_query_manager.get_total_entries(db_matching.table_obj)
        total_entries_no_match = db_query_manager.get_total_entries(db_no_matching.table_obj)
        self.assertEqual(total_entries_match, 12)
        self.assertEqual(total_entries_no_match, 7)

    def test_drm_matching(self):
        # Create a matcher object for DRM
        drm_matcher_obj = DbMatcherDrm(db_conn)

        # Perform DRM on target 1 and 2
        drm_matcher_obj.set_policy(policy_match_tgt1)
        drm_matcher_obj.execute_matching()

        drm_matcher_obj.set_policy(policy_match_tgt2)
        drm_matcher_obj.execute_matching()

        # Checking the results on matching and no-matching tables
        db_query_manager = DqlManager(db_conn)
        total_entries_match = db_query_manager.get_total_entries(db_matching.table_obj)
        total_entries_no_match = db_query_manager.get_total_entries(db_no_matching.table_obj)
        self.assertEqual(total_entries_match, 14)
        self.assertEqual(total_entries_no_match, 5)

    def test_ifm_matching(self):
        # Create a matcher object for IFM
        ifm_matcher_obj = DbMatcherIfm(db_conn)

        # Perform IFM on target 1 and 2
        ifm_matcher_obj.set_policy(policy_match_tgt1)
        ifm_matcher_obj.execute_matching()

        ifm_matcher_obj.set_policy(policy_match_tgt2)
        ifm_matcher_obj.execute_matching()

        # Checking the results on matching and no-matching tables
        db_query_manager = DqlManager(db_conn)
        total_entries_match = db_query_manager.get_total_entries(db_matching.table_obj)
        total_entries_no_match = db_query_manager.get_total_entries(db_no_matching.table_obj)
        self.assertEqual(total_entries_match, 15)
        self.assertEqual(total_entries_no_match, 4)


def build_test_suite():
    # Create a pool of tests
    test_suite = TestSuite()
    test_suite.addTest(TestDbExactMatching("test_sql_lite_connection"))
    test_suite.addTest(TestDbExactMatching("test_dfm_matching"))
    test_suite.addTest(TestDbExactMatching("test_drm_matching"))
    test_suite.addTest(TestDbExactMatching("test_ifm_matching"))
    return test_suite


def build_text_report():
    # Generate a tests report
    test_suite = build_test_suite()
    test_runner = TextTestRunner()
    test_runner.run(test_suite)


if __name__ == "__main__":
    build_text_report()
