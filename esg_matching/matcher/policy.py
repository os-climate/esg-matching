""" Base class allows to create policies (rules) to perform matchings on database """

# Import internal modules
from esg_matching.data_source.db_source import DbDataSource
from esg_matching.exceptions import exceptions_data_source
from esg_matching.exceptions import exceptions_matching_policy


def _add_alias_to_list(all_aliases: list, rules_dict: dict):
    for rule in rules_dict:
        aliases = rules_dict[rule]
        for alias in aliases:
            if alias not in all_aliases:
                all_aliases.append(alias)
    return all_aliases


class MatchingPolicy:
    """
        This class provides a base structure to create a policy or rules associated to a datasource, as to perform
        matchings of that datasource with other source based on database statements.

        Attributes:
            _target_ds (DbDataSource): the target datasource to be matched with a referential source
            _policy_name (str): policy name. For instance: 'matching_with_dexo'
            _referencial_ds (DbDataSource): the referencial datasource to matched with
            _matching_ds (DbDataSource): matching datasource that stores the positive matchings
            _no_matching_ds (DbDataSource): matching datasource that stores the negative matchings
            _rules_direct_full_matching (dict): the rules to perform direct full matching (see DbMatcherDfm)
            _rules_direct_residual_matching (dict): the rules to perform direct residual matching (see DbMatcherDrm)
            _rules_indirect_matching (dict): the rules to perform indirect full matching (see DbMatcherIfm)
            _all_aliases (list): all aliases names used in policy rules for the datasource specification
    """

    def __init__(self, data_source: DbDataSource, policy_name: str):
        """
            Constructor method.

            Parameters:
                data_source (DbDataSource): the target datasource to be matched with another source

            Returns:
                MatchingPolicy (object)

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the target datasource is not
                    synchronized with its correspondent database table.
        """
        if not data_source.is_sync_with_db_table():
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable
        self._target_ds = data_source
        self._policy_name = policy_name
        self._referencial_ds = None
        self._matching_ds = None
        self._no_matching_ds = None
        self._rules_direct_full_matching = {}
        self._rules_direct_residual_matching = {}
        self._rules_indirect_matching = {}
        self._set_policy_rules()
        self._all_aliases = self._get_all_aliases()

    @property
    def tgt_source(self):
        return self._target_ds

    @property
    def ref_source(self):
        return self._referencial_ds

    @property
    def matching_source(self):
        return self._matching_ds

    @property
    def no_matching_source(self):
        return self._no_matching_ds

    def _set_policy_rules(self):
        """
            Private class method that sets up the policy rules according to the specification in the datasource.

            Parameters:
                No parameters required.

            Returns:
                No return value.

            Raises:
                No exception is raised
        """
        self._policy_definition = self._target_ds.get_policy_definition(self._policy_name)
        if 'dfm' in self._policy_definition:
            self._rules_direct_full_matching = self._policy_definition['dfm']
        if 'drm' in self._policy_definition:
            self._rules_direct_residual_matching = self._policy_definition['drm']
        if 'irm' in self._policy_definition:
            self._rules_indirect_matching = self._policy_definition['irm']

    def _get_all_aliases(self):
        """
            Private class method that consolidates all aliases in a list so to make verifications if those
                aliases are correctly mapped in the referential data source.

            Parameters:
                No parameters required.

            Returns:
                all_aliases (list): list with all aliases consolidated.

            Raises:
                No exception is raised
        """
        all_aliases = []
        all_aliases = _add_alias_to_list(all_aliases, self._rules_direct_full_matching)
        all_aliases = _add_alias_to_list(all_aliases, self._rules_direct_residual_matching)
        all_aliases = _add_alias_to_list(all_aliases, self._rules_indirect_matching)
        return all_aliases

    def set_referential_source(self, ref_source: DbDataSource):
        """
            Class method that allows the injection of the matching datasource that stores positive matchings and
                it is associated to the matching policy.

            Parameters:
                ref_source (DbDataSource): the referential datasource linked to a database table

            Returns:
                No return value.

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the matching datasource is not
                    synchronized with its correspondent database table.
        """
        if not ref_source.is_sync_with_db_table():
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable
        self._referencial_ds = ref_source

    def set_matching_source(self, matching_source: DbDataSource):
        """
            Class method that allows the injection of the matching datasource that stores positive matchings and
                it is associated to the matching policy.

            Parameters:
                matching_source (DbDataSource): the matching datasource linked to a database table that stores
                positive matching results.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the matching datasource is not
                    synchronized with its correspondent database table.
        """
        if not matching_source.is_sync_with_db_table():
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable
        self._matching_ds = matching_source

    def set_no_matching_source(self, no_matching_source: DbDataSource):
        """
            Class method that allows the injection of the no-matching datasource that stores negative matchings and
                it is associated to the matching policy.

            Parameters:
                no_matching_source (DbDataSource): the no-matching datasource linked to a database table that stores
                negative matching results.

            Returns:
                No return value.

            Raises:
                exceptions_data_source.DataSourceNotSynchronizedWithDbTable when the no-matching datasource is not
                    synchronized with its correspondent database table.
        """
        if not no_matching_source.is_sync_with_db_table():
            raise exceptions_data_source.DataSourceNotSynchronizedWithDbTable
        self._no_matching_ds = no_matching_source

    def has_rule_type(self, rule_type: str):
        """
            Class method that checks if a rule_type is defined in the policy.

            Parameters:
                rule_type (str): rule type, that should be 'dfm', 'drm' or 'ifm'

            Returns:
                True if the rule_type is defined in the policy or
                False otherwise.

            Raises:
                exceptions_matching_policy.MatchingRuleNotSupported when the rule_type is different of 'dfm',
                    'drm' or 'irm'.
        """
        if rule_type == 'dfm':
            total_rules = len(self._rules_direct_full_matching)
        elif rule_type == 'drm':
            total_rules = len(self._rules_direct_residual_matching)
        elif rule_type == 'irm':
            total_rules = len(self._rules_indirect_matching)
        else:
            raise exceptions_matching_policy.MatchingRuleNotSupported
        if total_rules == 0:
            return False
        else:
            return True

    def get_matching_rules(self, rule_type: str):
        """
            Class method that return the rules defined in the policy for a given rule_type.

            Parameters:
                rule_type (str): rule type, that should be 'dfm', 'drm' or 'ifm'

            Returns:
                (dict) the dictionary of rules for the given rule_type

            Raises:
                exceptions_matching_policy.MatchingRuleNotSupported when the rule_type is different of 'dfm',
                    'drm' or 'irm'.
        """
        if rule_type == 'dfm':
            return self._rules_direct_full_matching
        elif rule_type == 'drm':
            return self._rules_direct_residual_matching
        elif rule_type == 'irm':
            return self._rules_indirect_matching
        else:
            raise exceptions_matching_policy.MatchingRuleNotSupported
