""" Base class allows to create a matcher that performs direct and indirect matching on a database """

# Import python libraries
from abc import ABC, abstractmethod

# Database connection and sql stamente sql
from esg_matching.engine.connectors.base_connector import DbConnector
from esg_matching.engine.sql.dml import DmlManager

# Matching policy
from esg_matching.matcher.policy import MatchingPolicy

# Builders
from esg_matching.engine.builders.table_builder import ColumnBuilder
from esg_matching.engine.builders.sql_builder import SelectBuilder
from esg_matching.engine.builders.sql_builder import DeleteBuilder
from esg_matching.engine.builders.sql_builder import SqlConditionBuilder


class DbMatcher(ABC):
    """
        This class provides a base structure to perform direct and indirect matching on a database

        Attributes:
            _db_connector (DbConnector): database connectors
            _matching_type (str): indicates the type of matching to be performed. It can be:
                'direct_full_matching': (dfm) matching between a target datasource and a referential datasource
                                        by using join conditions as matching rules.
                'direct_residual_matching': (drm) performs matching between a referential datasource and the records
                                        in the no-matching table. It is called 'residual' because it is performed
                                        on the entries that were not matched after a dfm was executed. It  also
                                        operates on join condition rules. But, this time the joins are between a
                                        referential datasource and the no-matching entries.
                'indirect_matching': (inm) performs a matching between a main target and other targets, but only on the
                                        entries by which the other targets were previously and successfully matched to
                                        a referential. In other words, the indirect matching applies the join condition
                                        rules on the no-matching (residual records) and the matching table, so that
                                        it's possible to capture positive matchings between a main target and other
                                        targets that had already being matched with a referential.
            _join_condition_builder (SqlConditionBuilder): object used to build join conditions
            _where_condition_builder (SqlConditionBuilder): object used to build where conditions
            _column_builder (ColumnBuilder): object used to build literal columns (that contains only a fixed value).
                                        These columns are associated to the standard attribute columns of the
                                        matching and no-matching tables as to identify the type, scope, rules and
                                        the names of the referencial/target on a matching/no-matching record.
            _tgt_source (DbDataSource): the target datasource object extracted from the policy object.
            _ref_source (DbDataSource): the referential datasource object extracted from the policy object.
            _matching_source (DbDataSource): the matching datasource object extracted from the policy object.
            _no_matching_source (DbDataSource): the no-matching datasource object extracted from the policy object.
            _tgt_table (sqlalchemy.sql.schema.Table): the target table object extracted from policy.
            _ref_table (sqlalchemy.sql.schema.Table): the referential table object extracted from policy.
            _matching_table (sqlalchemy.sql.schema.Table): the matching table object extracted from policy.
            _no_matching_table (sqlalchemy.sql.schema.Table): the no-matching table object extracted from policy.
            _matching_rules (dict): describes the rules/attributes used to build the join conditions.
            _dml_manager (DmlManager): object used to execute sql insert and delete for the matching/no-matching.
            _select_builder (SelectBuilder): helps to build sql select's to be used in insert into statements.
            _delete_builder (DeleteBuilder): helps to build sql delete statements .
    """

    def __init__(self, db_connector: DbConnector):
        self._db_connector = db_connector

        # Elements extracted from the matching policy for the matcher type
        self._matching_type = 'unkown'
        self._matching_rules = None
        self._tgt_source = None
        self._ref_source = None
        self._matching_source = None
        self._no_matching_source = None
        self._tgt_table = None
        self._ref_table = None
        self._matching_table = None
        self._no_matching_table = None

        # Helpers used to build sql conditions, sql statements and literal (fixed database columns)
        self._join_condition_builder = SqlConditionBuilder()
        self._where_condition_builder = SqlConditionBuilder()
        self._column_builder = ColumnBuilder(self._db_connector)
        self._select_builder = SelectBuilder()
        self._delete_builder = DeleteBuilder()

        # Literal values
        self._literal_matching_type = ''
        self._literal_matching_scope = ''

        # The sql statement executer
        self._dml_manager = DmlManager(self._db_connector)

    def set_policy(self, policy: MatchingPolicy):
        """
            Class method that unpacks the matching policy to be used by a particular matcher.
            Currently, there are three types of matchers: dfm, drm and inm. One target datasource can be matched
                using the these three matchers. Therefore, for each matcher type, there must be a set of matching
                rules and its correspondent attributes that will take part on the join condition rules.

            Parameters:
                policy (MatchingPolicy) : the matching policy contains the description of the join rules as well as
                                        the mappings of attributes beetween target, referential and matching/
                                        no-matching tables.

            Raises:
                exceptions_data_source.DirectFullMatchingNotInPolicy when the mapping between original names and
                    attribute names are not defined.
        """
        # Check if the policy is supported by the matcher
        self._can_matcher_be_used(policy)
        # If no exception is triggered by the previous statement,
        # ...unpack the policy elements that applies to the matcher type
        self._unpack_policy_elements(policy)

    def _unpack_policy_elements(self, policy: MatchingPolicy):
        """
            Private class method used to unpack all the information that a matcher needs to perform matching on
            a database. This information is encapsulated in a MatchingPolicy object for a particular datasource.
            Because a datasource can apply different types of matchings and the policy reflects this, it is needed
            to unpack the rules that only apply to a particular matcher instance. This is what this method does!

            Parameters:
                policy (MatchingPolicy) : the matching policy contains the description of the matching rules.

            Returns:
                No return value.

            Raises:
                No exception is raised.
        """
        # Get the sources needed for the matching
        self._tgt_source = policy.tgt_source
        self._ref_source = policy.ref_source
        self._matching_source = policy.matching_source
        self._no_matching_source = policy.no_matching_source

        # Get the database tables for the matching
        self._tgt_table = policy.tgt_source.table_obj
        self._ref_table = policy.ref_source.table_obj
        self._matching_table = policy.matching_source.table_obj
        self._no_matching_table = policy.no_matching_source.table_obj

        # Get the macthing rules for the matcher type being instanciated.
        # A matching policy can have many rules, for instance it can macth for an ID first and then, NAME.
        # Therefore, the _matching_rules is a dictionary, in which the key is the name of the matching rule and
        # the value is a list with all the attributes that will compose the join conditions.
        self._matching_rules = policy.get_matching_rules(self._matching_type)

        # Cleans up the builders
        self._join_condition_builder.clean()
        self._where_condition_builder.clean()
        self._select_builder.clean()
        self._delete_builder.clean()

    @abstractmethod
    def _can_matcher_be_used(self, policy: MatchingPolicy):
        """
            Private class method used to check if the policy can be applied to the instanciated matcher.
            Every matcher type must implement this abstract method.

            Parameters:
                policy (MatchingPolicy) : the matching policy contains the description of the matching rules.

            Returns:
                True if the matcher is described as a matching process in the given policy, or
                False, otherwise.

        """
        pass

    @abstractmethod
    def _get_conditions(self, attribute_rules: list):
        """
            Private class method that builds the join and where conditions that will be used to form the matching and
                the no-matching sql statements. Every matcher type must implement this abstract method.

            Parameters:
                attribute_rules (list) : a list of attribute aliases that represent the names of table attributes
                    of the datasources participating on a matching/nno-matching process.

            Returns:
                join_condition (sqlalchemy.sql.expression): the join condition
                where_condition (sqlalchemy.sql.expression): the where condition

        """
        pass

    def _add_literal_cols_type_match(self, rule_name, list_to_add_to: list):
        """
            Private class method that helps to build the literal columns of the matching table. This method
                complements the _get_literal_cols_match() method as to add the additional matching columns that
                describe the type of matching perfomed.

            Parameters:
                rule_name (str) : the name of the matching rule is passed as parameter because it is itself a fixed
                    value for one of the standard attribute 'MATCHING_RULE'.

            Returns:
                literal_db_cols (list): list of sqlalchemy.sql.schema.Column objects with fixed values.

        """
        name_std_col = self._matching_source.get_std_attribute_name('matching_type')
        list_to_add_to.append(self._column_builder.create_literal_column(self._literal_matching_type, name_std_col))

        name_std_col = self._matching_source.get_std_attribute_name('matching_scope')
        list_to_add_to.append(self._column_builder.create_literal_column(self._literal_matching_scope, name_std_col))

        name_std_col = self._matching_source.get_std_attribute_name('matching_rule')
        list_to_add_to.append(self._column_builder.create_literal_column(rule_name, name_std_col))
        return list_to_add_to

    def _get_literal_cols_match(self, rule_name):
        """
            Private class method that builds the literal columns of the matching table. Literal columns are the ones
                holding fixed values and defined in the matching datasource as standard attributes. See the
                DbMatchDataSource object for more details on the pre-defined standard attributes.

            Parameters:
                rule_name (str) : the name of the matching rule is passed as parameter because it is itself a fixed
                    value for one of the standard attribute 'MATCHING_RULE'.

            Returns:
                literal_db_cols (list): list of sqlalchemy.sql.schema.Column objects with fixed values.
                literal_name_cols (list): list with the names of all literal columns created.

        """
        literal_db_cols = []
        tgt_name = self._tgt_source.name
        ref_name = self._ref_source.name

        name_std_col = self._matching_source.get_std_attribute_name('ref_name')
        literal_db_cols.append(self._column_builder.create_literal_column(ref_name, name_std_col))

        name_std_col = self._matching_source.get_std_attribute_name('tgt_name')
        literal_db_cols.append(self._column_builder.create_literal_column(tgt_name, name_std_col))

        literal_db_cols = self._add_literal_cols_type_match(rule_name, literal_db_cols)

        literal_name_cols = self._matching_source.get_std_attribute_names()
        return literal_db_cols, literal_name_cols

    def _get_literal_cols_no_match(self):
        """
            Private class method that builds the literal columns of the no-matching table. Literal columns are the ones
                holding fixed values and defined in the no-matching datasource as standard attributes. See the
                DbMatchDataSource object for more details on the pre-defined standard attributes.

            Parameters:
                No parameters required.

            Returns:
                literal_db_cols (list): list of sqlalchemy.sql.schema.Column objects with fixed values.
                literal_name_cols (list): list with the names of all literal columns created.

        """
        literal_db_cols = []
        tgt_name = self._tgt_source.name
        literal_db_cols.append(self._column_builder.create_literal_column(tgt_name, 'tgt_name'))
        literal_name_cols = self._no_matching_source.get_std_attribute_names()
        return literal_db_cols, literal_name_cols

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

        """
        literal_db_cols, literal_name_cols = self._get_literal_cols_match(rule_name)
        tgt_db_cols = self._tgt_source.get_db_cols_mapped_to_matching()
        tgt_name_cols = self._tgt_source.get_name_cols_mapped_to_matching()
        ref_db_cols = self._ref_source.get_db_cols_mapped_to_matching()
        ref_name_cols = self._ref_source.get_name_cols_mapped_to_matching()
        select_db_cols = literal_db_cols + tgt_db_cols + ref_db_cols
        select_name_cols = literal_name_cols + tgt_name_cols + ref_name_cols
        return select_db_cols, select_name_cols

    def _get_cols_no_match(self):
        """
            Private class method that builds a list with the columns of the select statement that computes negative
            matches. These columns are a combination of the columns in the target and referential data sources
            that are mapped to the no-matching database table. Also, literal columns are added as to include columns
            that describe each row of the negative matching.

            Parameters:
                No parameters required.

            Returns:
                select_db_cols (list): list of sqlalchemy.sql.schema.Column objects with all the columns used in the
                    select statement that computes negative matches. Overall, select_db_cols is the sum of literal
                     columns, columns from the target datasource and columns from the referential datasource.
                select_name_cols (list): list with the names of all select_db_cols.

        """
        literal_db_cols, literal_name_cols = self._get_literal_cols_no_match()
        tgt_db_cols = self._tgt_source.get_db_cols_mapped_to_matching()
        tgt_name_cols = self._tgt_source.get_name_cols_mapped_to_matching()
        select_db_cols = literal_db_cols + tgt_db_cols
        select_name_cols = literal_name_cols + tgt_name_cols
        return select_db_cols, select_name_cols

    @abstractmethod
    def execute_matching(self):
        """
            Class method that executes the matching.

        """
        pass