"""
Shared base class for all load test series across platforms.

Both Snowflake and Databricks series modules inherit from BaseSeries,
which defines the lifecycle hooks and query callback interface.
"""

from abc import ABC, abstractmethod


class BaseSeries(ABC):
    """
    Abstract base class for experiment series.

    Provides hooks for setup/teardown at series and experiment levels,
    and defines the interface for query execution callbacks used by
    Locust workers.
    """

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        """
        Called once before the first experiment in a series.

        Args:
            session_or_config: Platform-specific session/config object.
                - Snowflake: snowflake.snowpark.Session
                - Databricks: dict with host, token, catalog, schema, etc.
            params: Dictionary of series parameters
        """
        pass

    def teardown_series(self, session_or_config, params):
        """
        Called once after the last experiment in a series.

        Args:
            session_or_config: Platform-specific session/config object
            params: Dictionary of series parameters
        """
        pass

    def setup_experiment(self, session_or_config, params):
        """
        Called before each individual experiment.

        Args:
            session_or_config: Platform-specific session/config object
            params: Dictionary of experiment parameters
        """
        pass

    def teardown_experiment(self, session_or_config, params):
        """
        Called after each individual experiment.

        Args:
            session_or_config: Platform-specific session/config object
            params: Dictionary of experiment parameters
        """
        pass

    def init_worker_session(self, session):
        """
        Optional: initialize per-worker state (e.g., HTTP session reference).
        Called by Locust workers on start.
        """
        pass

    @abstractmethod
    def get_query_callback(self, session_or_config, params):
        """
        Returns a callable that executes a single query/request.

        The returned function takes no arguments and performs one unit of work
        (e.g., one HTTP POST or one SQL SELECT). Locust measures its latency.

        Args:
            session_or_config: Platform-specific session or HTTP session
            params: Dictionary of experiment parameters

        Returns:
            Callable that executes a query, or None to use the locustfile's
            built-in query logic (Snowflake REST/SQL mode).
        """
        pass
