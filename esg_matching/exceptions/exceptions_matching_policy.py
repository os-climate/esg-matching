class MatchingPolicyError(Exception):
    """Top-level error type of the library. This exception must not be raised.
    Instead, it is expected to use one of its subclasses. """

    def __str__(self):
        """Return the exception message."""
        return ''.join(self.args[:1]) or getattr(self, 'message', '')


class MatchingRuleNotSupported(MatchingPolicyError):
    """The matching rule informed is not supported."""

    message = "The matching rule informed is not supported."


class MatchingAliasNotInDataSource(MatchingPolicyError):
    """Matching alias not found in datasource. Check the list of alias available."""

    message = "Matching alias not found in datasource. Check the list of alias available."


class NoMatchingAliasNotInDataSource(MatchingPolicyError):
    """No-matching alias not found in datasource. Check the list of alias available."""

    message = "No-matching alias not found in datasource. Check the list of alias available."


class DirectFullMatchingNotInPolicy(MatchingPolicyError):
    """The given policy does not contain rules to perform direct full matching."""

    message = "The given policy does not contain rules to perform direct full matching."


class DirectResidualMatchingNotInPolicy(MatchingPolicyError):
    """The given policy does not contain rules to perform direct residual matching."""

    message = "The given policy does not contain rules to perform direct residual matching."


class IndirectMatchingNotInPolicy(MatchingPolicyError):
    """The given policy does not contain rules to perform indirect matching."""

    message = "The given policy does not contain rules to perform indirect matching."