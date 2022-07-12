
class FileSettingsError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class FileSourceDoesNotExist(FileSettingsError):
    """File source does not exist"""

    message = "File source does not exist"


class FileSettingsDoesNotExist(FileSettingsError):
    """File settings does not exist"""

    message = "File settings does not exist"


class FileSettingsIsNotAJsonFile(FileSettingsError):
    """File settings is not a json file"""

    message = "File settings is not a json file"


class FileProcessingSettingsNotDefined(FileSettingsError):
    """File processing settings is missing in the json file"""

    message = "File processing settings or one of its elements is missing in the json file"


class DataSourceSettingsNotDefined(FileSettingsError):
    """Data source settings is missing in the json file"""

    message = "Data source settings or one of its elements is missing in the json file"


class MatchingSettingsNotDefined(FileSettingsError):
    """Matching settings is missing in the json file"""

    message = "Matching settings or one of its elements is missing in the json file"


class IfTableExistsNotRecognized(FileSettingsError):
    """Wrong value provided in json parameter [if_table_exists]"""

    message = "Wrong value provided in json parameter [if_table_exists]"


class MatchingRoleNotRecognized(FileSettingsError):
    """Wrong value provided in json parameter [matching_role]"""

    message = "Wrong value provided in json parameter [matching_role]"


class AliasMatchingNotDefined(FileSettingsError):
    """The [matching_alias] parameter is missing from file settings"""

    message = "The [matching_alias] parameter is missing from file settings"


class FieldsMappingToMatchingTableNotDefined(FileSettingsError):
    """The [map_to_matching_table] parameter is missing from file settings"""

    message = "The [map_to_matching_table] parameter is missing from file settings"


class FieldsMappingToNoMatchingTableNotDefined(FileSettingsError):
    """The [map_to_no_matching_table] parameter is missing from file settings"""

    message = "The [map_to_no_matching_table] parameter is missing from file settings"


class MatchingPolicyEmpty(FileSettingsError):
    """The [matching_policy] parameter is defined, but is empty"""

    message = "The [matching_policy] parameter is defined, but is empty"


class ReferentialIsMissingInMatchingPolicy(FileSettingsError):
    """The [referential_source] parameter is not defined for a policy from file settings"""

    message = "The [referential_source] parameter is not defined for a policy in file settings"


class MatchingSourceIsMissingInMatchingPolicy(FileSettingsError):
    """The [matching_source] parameter is not defined for a policy from file settings"""

    message = "The [matching_source] parameter is not defined for a policy in file settings"


class NoMatchingSourceIsMissingInMatchingPolicy(FileSettingsError):
    """The [no_matching_source] parameter is not defined for a policy from file settings"""

    message = "The [no_matching_source] parameter is not defined for a policy in file settings"


class DfmRulesEmptyInMatchingPolicy(FileSettingsError):
    """The [dfm_rules] parameter is empty for a policy from file settings"""

    message = "The [dfm_rules] parameter is empty for a policy in file settings"


class DrmRulesEmptyInMatchingPolicy(FileSettingsError):
    """The [drm_rules] parameter is empty for a policy from file settings"""

    message = "The [drm_rules] parameter is empty for a policy in file settings"


class IrmRulesEmptyInMatchingPolicy(FileSettingsError):
    """The [irm_rules] parameter is empty for a policy from file settings"""

    message = "The [irm_rules] parameter is empty for a policy in file settings"
