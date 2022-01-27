""" Base class allows to create a matcher that performs indirect matching on a database """

from esgmatching.db_matcher.db_matcher import DbMatcher
from esgmatching.db_engine.engines.connector import DbConnector
from esgmatching.db_matcher.matching_policy import MatchingPolicy
from esgmatching.exceptions import exceptions_matching_policy


class DbMatcherIfm(DbMatcher):
    """
        This base class provides the infrastructure needed to perform direct residual matching in a database.

        Attributes: (see DbMatcher)
            'indirect_matching': (inm) performs a matching between a main target and other targets, but only on the
                                    entries by which the other targets were previously and successfully matched to
                                    a referential. In other words, the indirect matching applies the join condition
                                    rules on the no-matching (residual records) and the matching table, so that
                                    it's possible to capture positive matchings between a main target and other
                                    targets that had already being matched with a referential.
    """

    def __init__(self, db_connector: DbConnector):
        """
            Constructor method.

            Parameters:
                db_connector (DbConnector): database engines

            Returns:
                DbMatcherDrm (object)

            Raises:
                No exception is raised.
        """
        super().__init__(db_connector)
        self._matching_type = 'ifm'
        self._literal_matching_type = 'indirect'
        self._literal_matching_scope = 'full'

    def _can_matcher_be_used(self, policy: MatchingPolicy):
        """
            Private class method used to check if the policy can be applied to the instanciated matcher.
            In the case of DFM, it checks if the policy has at least one rule for 'direct_residual_matching'.

            Parameters:
                policy (MatchingPolicy) : the matching policy contains the description of the matching rules.

            Returns:
                True if the matcher is described as a matching process in the given policy, or
                False, otherwise.

            Raises:
                No exception is raised.
        """
        # Check if the policy contains rules to perform the direct residual matching
        if not policy.has_rule_type(self._matching_type):
            raise exceptions_matching_policy.IndirectMatchingNotInPolicy

    def _get_conditions(self, attribute_rules):
        """
            Private class method that builds the join and where conditions that will be used to form the matching and
                the no-matching sql statements. For DRM, the join clause is built between the target and no-matching.
                The alias_attributes is mapped to the target and no-matching datasource as build the equality condition
                for the join. The where condition is used only to build the no-matching table.

            Parameters:
                attribute_rules (list) : a list of attribute aliases that represent the names of table attributes
                    of the datasources participating on a matching/nno-matching process.

            Returns:
                join_condition (sqlalchemy.sql.expression): the join condition
                where_condition (sqlalchemy.sql.expression): the where condition

            Raises:
                No exception is raised.
        """
        # Build the condition for the join and where clauses
        join_condition = None
        where_condition = None
        for alias_attribute in attribute_rules:
            tgt_col_name = self._tgt_source.get_matching_attribute_by_alias(alias_attribute)
            left_db_col = self._no_matching_source.get_table_column(tgt_col_name)
            right_db_col = self._matching_source.get_table_column(tgt_col_name)
            self._join_condition_builder = self._join_condition_builder.create_condition()\
                .equal_cols(left_db_col, right_db_col)
            if join_condition is None:
                join_condition = self._join_condition_builder.get_condition()
            else:
                self._join_condition_builder = self._join_condition_builder.and_condition(join_condition)
                join_condition = self._join_condition_builder.get_condition()
        tgt_name = self._tgt_source.name
        tgt_db_name = self._no_matching_source.get_table_column('TGT_NAME')
        self._where_condition_builder = self._where_condition_builder.create_condition()
        self._where_condition_builder = self._where_condition_builder.equal_value(tgt_db_name, tgt_name)
        where_condition = self._where_condition_builder.get_condition()

        tgt_db_name = self._matching_source.get_table_column('TGT_NAME')
        self._where_condition_builder = self._where_condition_builder.create_condition()
        self._where_condition_builder = self._where_condition_builder.not_equal_value(tgt_db_name, tgt_name)
        self._where_condition_builder = self._where_condition_builder.and_condition(where_condition)
        return join_condition, where_condition

    def _get_cols_match(self, rule_name):
        """
            Private class method that builds a list with the columns of the select statement that computes positive
            matches. These columns are a combination of the columns in the target and referential data sources
            that are mapped to the matching database table. Also, literal columns are added as to include columns
            that describe each row of the positive matching.

            Parameters:
                rule_name (str) : the name of the matching rule is passed as parameter because this method calls
                    _get_literal_cols_match() method.

            Returns:
                select_db_cols (list): list of sqlalchemy.sql.schema.Column objects with all the columns used in the
                    select statement that computes positive matches. Overall, select_db_cols is the sum of literal
                     columns, columns from the target datasource and columns from the referential datasource.
                select_name_cols (list): list with the names of all select_db_cols.

            Raises:
                No exception is raised.
        """
        literal_db_cols, literal_name_cols = self._get_literal_cols_match(rule_name)
        no_match_name_cols = self._tgt_source.get_name_cols_mapped_to_matching()
        no_match_db_cols = self._no_matching_source.get_db_cols_with_same_name(no_match_name_cols)
        ref_db_cols = self._ref_source.get_db_cols_mapped_to_matching()
        ref_name_cols = self._ref_source.get_name_cols_mapped_to_matching()
        select_db_cols = literal_db_cols + no_match_db_cols + ref_db_cols
        select_name_cols = literal_name_cols + no_match_name_cols + ref_name_cols
        return select_db_cols, select_name_cols

    def _get_cols_no_match(self):
        literal_db_cols, literal_name_cols = self._get_literal_cols_no_match()
        tgt_db_cols = self._tgt_source.get_db_cols_mapped_to_no_matching()
        tgt_name_cols = self._tgt_source.get_name_cols_mapped_to_no_matching()
        select_db_cols = literal_db_cols + tgt_db_cols
        select_name_cols = literal_name_cols + tgt_name_cols
        return select_db_cols, select_name_cols

    def execute_matching(self):
        """
            Class method that executes the direct full matching (DFM), a join between referential and target tables.

            Parameters:
                No parameters required.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        # Open a session if one does not exist already
        if not self._db_connector.has_session_open():
            self._db_connector.create_session()
        for rule_key in self._matching_rules:
            # ------ MATCHING PROCESS ------
            # Process positive matchings: this is basically an INSERT into the MATCHING_TABLE taken values
            # from a SELECT JOIN beetween the TARGET_TABLE and REFERENTIAL_TABLE
            # Get the columns of the INSERT...SELECT that computes the positive results (matchings)
            select_db_cols, select_name_cols = self._get_cols_match(rule_key)

            # Get the attribute names that will form the join condition (for matching) and the where_condition
            # used in the no-matching. The attributes are alias mapped to the real attribute names in the target
            # and the referencial datasources.
            join_condition, where_condition = self._get_conditions(self._matching_rules[rule_key])

            # Build the select statement
            self._select_builder = self._select_builder.create_select(select_db_cols)
            self._select_builder = self._select_builder.from_table(self._no_matching_table)
            self._select_builder = self._select_builder.join_table(self._ref_table).join_on(join_condition)
            self._select_builder = self._select_builder.where_condition(where_condition)
            select_stm = self._select_builder.build_statement()

            # Get the metadata columns of the matching table
            # The columns of the matching table must be the same, in the same order as the ones in the
            # select statement. Therefore, the column names of the select is sent by paramenter.
            matching_db_cols = self._matching_source.get_db_cols_with_same_name(select_name_cols)
            self._dml_manager.insert_into_from_select(self._matching_table, matching_db_cols, select_stm)

            # ------ NO-MATCHING PROCESS ------
            # DELETE MATCHING RESULTS FROM NO-MATCHING TABLE
            # Build the select statement
            # self._select_builder = self._select_builder.create_select(select_db_cols).from_table(self._tgt_table)
            # self._select_builder = self._select_builder.join_table(self._ref_table).join_on(join_condition, 'left')
            # self._select_builder = self._select_builder.where_condition(where_condition)
            # select_stm = self._select_builder.build_statement()
            # self._delete_builder = self._delete_builder.create_delete().from_table(self._no_matching_table)
            # self._delete_builder = self._delete_builder.where_condition(select_stm)
            # no_matching_db_cols = self._no_matching_source.get_db_cols_with_same_name(select_name_cols)
            # self._dml_manager.insert_into_from_select(self._no_matching_table, no_matching_db_cols, select_stm)

            # Commit changes
            self._db_connector.commit_changes()
        # Close the session
        self._db_connector.close_session()