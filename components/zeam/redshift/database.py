"""Redshift database connection and query utilities."""

from typing import Any, Dict, List, Optional

import redshift_connector
from zeam.redshift.config import settings


class RedshiftConnection:
    """Manages connections to Redshift database."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """Initialize Redshift connection with settings or explicit params.

        Args:
            host: Redshift host (default: settings.REDSHIFT_HOST)
            port: Redshift port (default: settings.REDSHIFT_PORT)
            database: Database name (default: settings.REDSHIFT_DB)
            user: Database user (default: settings.REDSHIFT_USER)
            password: Database password (default: settings.REDSHIFT_PASSWORD)
        """
        self.host = host or settings.REDSHIFT_HOST
        self.port = int(port or settings.REDSHIFT_PORT)
        self.database = database or settings.REDSHIFT_DB
        self.user = user or settings.REDSHIFT_USER
        self.password = password or settings.REDSHIFT_PASSWORD
        self._connection = None

        if not all([self.host, self.database, self.user, self.password]):
            # It's possible some might be None if not set in env or passed
            pass

    def _is_connection_closed(self) -> bool:
        """Check if the connection is closed.
        
        Returns:
            True if connection is None or closed, False otherwise
        """
        if self._connection is None:
            return True
        
        # Check if the connection has a 'closed' attribute
        if hasattr(self._connection, 'closed'):
            return self._connection.closed
        
        # If no 'closed' attribute, try to check connection validity
        try:
            # Try to access a property that would fail if connection is closed
            _ = self._connection.autocommit
            return False
        except Exception:
            return True

    def connect(self):
        """Establish connection to Redshift."""
        if self._connection is None or self._is_connection_closed():
            if not all([self.host, self.database, self.user, self.password]):
                 raise ValueError(
                    "Missing required connection parameters. "
                    "Ensure REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, and REDSHIFT_PASSWORD "
                    "are set in environment variables or settings."
                )

            self._connection = redshift_connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        return self._connection

    def close(self):
        """Close the database connection."""
        if self._connection and not self._is_connection_closed():
            self._connection.close()
            self._connection = None

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries.
        Supports multiple statements separated by semicolons by executing them sequentially.
        If multiple statements are provided, returns the results of the last statement.

        Args:
            query: SQL query (or queries) to execute
            params: Optional query parameters for parameterized queries (only applied to the last statement)

        Returns:
            List of dictionaries representing query results from the last statement
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Split query into individual statements
            # This is a basic split that handles multiple statements separated by semicolons
            # It might fail if semicolons are inside strings, but for standard usage it's usually fine
            statements = [s.strip() for s in query.split(';') if s.strip()]
            
            if not statements:
                return []

            results = []
            for i, stmt in enumerate(statements):
                is_last = (i == len(statements) - 1)
                
                # Use params only for the last statement if it's the intended target
                # This is a bit of a heuristic, but usually params are meant for the main query
                if is_last and params:
                    cursor.execute(stmt, params)
                else:
                    cursor.execute(stmt)
                
                # Only fetch results for the last statement if it has a result set
                if is_last:
                    if cursor.description is not None:
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        results = [dict(zip(columns, row)) for row in rows]
                    else:
                        results = []
            
            return results
        except Exception as e:
            raise
        finally:
            cursor.close()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a query and return results as list of dictionaries.
    
    Args:
        query: SQL query to execute
        params: Optional query parameters
        
    Returns:
        List of dictionaries representing query results
    """
    with RedshiftConnection() as conn:
        return conn.execute_query(query, params)


def execute_command(command: str, params: Optional[tuple] = None) -> None:
    """Execute a command (UPDATE, INSERT, DELETE, etc).
    
    Args:
        command: SQL command to execute
        params: Optional command parameters
    """
    with RedshiftConnection() as conn:
        conn.execute_query(command, params)


def get_db():
    """Dependency for getting a database connection.
    Yields a RedshiftConnection.
    """
    conn = RedshiftConnection()
    try:
        conn.connect()
        yield conn
    finally:
        conn.close()
