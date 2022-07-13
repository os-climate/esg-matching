""" Base class allows to create a matcher that performs direct full matching on a database """

from esg_matching.matcher.db_matcher import DbMatcher
from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.matcher.policy import MatchingPolicy
from esg_matching.exceptions import exceptions_matching_policy


class DbMatcherDfm(DbMatcher):
    """
        This base class provides the infrastructure needed to perform direct full matching in a database

        Attributes: (see DbMatcher)
            _matching_type (str): indicates the type of matching to be performed. It can be:
                'direct_full_matching': (dfm) matching between a target datasource and a referential datasource
                                        by using join conditions as matching rules.
    """

    def __init__(self, db_connector: DbConnector):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): database connectors

            Returns:
                DbMatcherDfm (object)

        """
        super().__init__(db_connector)
        self._matching_type = 'dfm'
        self._literal_matching_type = 'direct'
        self._literal_matching_scope = 'full'

    def _can_matcher_be_used(self, policy: MatchingPolicy):
        """
            Private class method used to check if the policy can be applied to the instanciated matcher.
            In the case of DFM, it checks if the policy has at least one rule for 'direct_full_matching'.

            Parameters:
                policy (MatchingPolicy) : the matching policy contains the description of the matching rules.

            Returns:
                True if the matcher is described as a matching process in the given policy, or
                False, otherwise.

        """
        # Check if the policy contains rules to perform the direct full matching
        if not policy.has_rule_type(self._matching_type):
            raise exceptions_matching_policy.DirectFullMatchingNotInPolicy

    def _get_conditions(self, attribute_rules):
        """
            Private class method that builds the join and where conditions that will be used to form the matching and
                the no-matching sql statements. For DFM, the join clause is built between the target and referential.
                The alias_attributes is mapped to the target and referential datasource as build the equality condition
                for the join. The where condition is used only to build the no-matching table.

            Parameters:
                attribute_rules (list) : a list of attribute aliases that represent the names of table attributes
                    of the datasources participating on a matching/nno-matching process.

            Returns:
                join_condition (sqlalchemy.sql.expression): the join condition
                where_condition (sqlalchemy.sql.expression): the where condition

        """
        # Build the condition for the join and where clauses
        join_condition = None
        where_condition = None
        for alias_attribute in attribute_rules:
            left_db_col = self._tgt_source.get_table_column_by_alias(alias_attribute)
            right_db_col = self._ref_source.get_table_column_by_alias(alias_attribute)
            self._join_condition_builder = self._join_condition_builder.create_condition()
            self._join_condition_builder = self._join_condition_builder.equal_cols(left_db_col, right_db_col)
            if join_condition is None:
                join_condition = self._join_condition_builder.get_condition()
            else:
                self._join_condition_builder = self._join_condition_builder.and_condition(join_condition)
                join_condition = self._join_condition_builder.get_condition()
            self._where_condition_builder = self._where_condition_builder.create_condition()
            self._where_condition_builder = self._where_condition_builder.is_null(right_db_col)
            if where_condition is None:
                where_condition = self._where_condition_builder.get_condition()
            else:
                self._where_condition_builder = self._where_condition_builder.and_condition(where_condition)
                where_condition = self._where_condition_builder.get_condition()
        return join_condition, where_condition

    def execute_matching(self):
        """
            Class method that executes the direct full matching (DFM), a join between referential and target tables.
            It also builds the no-matching, a left join between referential and target tables where the referencial
            is null because it did not match.

        """
        # Open a session if one does not exist already so that the matching and no-matching results are
        # added  and commited to their respective database tables during that session.
        # if not self._db_connector.has_session_open():
        #    self._db_connector.create_session()
        for rule_key in self._matching_rules:
            # ------ MATCHING PROCESS ------
            # Process positive matchings: this is basically an INSERT into the MATCHING_TABLE taken values
            # from a SELECT JOIN beetween the TARGET_TABLE and REFERENTIAL_TABLE
            # Get the columns of the SELECT...JOIN that computes the positive results (matchings)
            select_db_cols, select_name_cols = self._get_cols_match(rule_key)

            # Get the attribute names that will form the join condition (for matching) and the where_condition
            # used in no-matching. The attributes are aliases mapped to the real attribute names in the target
            # and the referencial datasources.
            join_condition, where_condition = self._get_conditions(self._matching_rules[rule_key])

            # Build the select statement
            self._select_builder = self._select_builder.create_select(select_db_cols).from_table(self._tgt_table)
            self._select_builder = self._select_builder.join_table(self._ref_table).join_on(join_condition)
            select_stm = self._select_builder.build_statement()

            # Get the metadata columns of the matching table
            # The columns of the matching table must be the same, in the same order as the ones in the
            # select statement. Therefore, the column names of the select are sent by paramenter to guarantee that.
            matching_db_cols = self._matching_source.get_db_cols_with_same_name(select_name_cols)
            self._dml_manager.insert_into_from_select(self._matching_table, matching_db_cols, select_stm)

            # ------ NO-MATCHING PROCESS ------
            # NEGATIVE MATCHINGS: INSERT INTO NO-MATCHING TABLE
            # Get the columns of the INSERT...SELECT that computes the negative results (no-matchings)
            select_db_cols, select_name_cols = self._get_cols_no_match()
            # Build the select statement
            self._select_builder = self._select_builder.create_select(select_db_cols).from_table(self._tgt_table)
            self._select_builder = self._select_builder.join_table(self._ref_table).join_on(join_condition, 'left')
            self._select_builder = self._select_builder.where_condition(where_condition)
            select_stm = self._select_builder.build_statement()
            no_matching_db_cols = self._no_matching_source.get_db_cols_with_same_name(select_name_cols)
            self._dml_manager.insert_into_from_select(self._no_matching_table, no_matching_db_cols, select_stm)

            # Commit changes
            # self._db_connector.commit_changes()
        # Close the session
        # self._db_connector.close_session()