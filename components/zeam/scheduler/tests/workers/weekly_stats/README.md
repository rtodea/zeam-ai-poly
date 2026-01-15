# Weekly Stats Worker Tests

## Overview

Tests for `weekly_stats_worker` focusing on multi-statement SQL execution, date handling, and result aggregation.

## Testing Strategy

### 1. Minimal Data Approach
Instead of testing with full weekly data, we use **1-2 rows per query**:
- Each SQL statement returns minimal data
- Focus on logic, not data volume
- Fast execution (< 1 second)

### 2. Complexity Considerations

This worker is more complex because:
- Multiple SQL statements (split by `;`)
- Date parameter substitution (`{start_date}`, `{end_date}`)
- Temporary tables (CREATE TEMP TABLE)
- Multiple result sets with titles
- Comment filtering logic

### 3. Synthetic Data Structure

```python
# Example for "Live Summarized" query
[
    {
        "friendly_call_sign": "WABC",
        "platform": "iOS",
        "sitename": "Zeam",
        "sitetype": "Zeam",
        "marketstate": "In",
        "dma_name": "New York",
        "viewers": 100,
        "sessions": 50,
        "minutes": 1500.0,
        "avg": 30.0,
    }
]
```

Matches SQL output schema for each query result.

### 4. Edge Cases Covered

- ✅ Multi-statement SQL execution
- ✅ Date calculation (week start/end)
- ✅ Date formatting for SQL
- ✅ Comment-only statements (skipped)
- ✅ Empty results from queries
- ✅ Redis key generation
- ✅ Database errors
- ✅ Redis errors
- ✅ Results aggregation by title

### 5. What We Test

1. **Date Logic** - Week calculation (Monday to Sunday)
2. **SQL Splitting** - Multiple statements from single file
3. **Parameter Substitution** - `{start_date}` and `{end_date}`
4. **Comment Filtering** - Skip comment-only statements
5. **Result Aggregation** - Multiple query results into single Redis key
6. **Redis Storage** - Correct key, structure, TTL
7. **Error Handling** - Database and Redis failures

## Running Tests

```bash
# Run weekly_stats tests
pytest tests/scheduler/workers/weekly_stats/ -v

# With coverage
pytest tests/scheduler/workers/weekly_stats/ --cov=scheduler.workers.weekly_stats_worker

# Specific test
pytest tests/scheduler/workers/weekly_stats/test_weekly_stats_worker.py::TestWeeklyStatsWorker::test_get_current_week_dates -v
```

## Fast Execution

Tests run in **< 1 second** per test:
- Minimal rows (1-2 per query result)
- Mocked database (no real queries)
- Mocked Redis (in-memory)
- No actual date-based filtering

## Redis Data Verification

```python
# Key format
"popularity:weekly_stats:{start_date}"
# Example: "popularity:weekly_stats:2024-12-16"

# Value structure (JSON dict with multiple sections)
{
    "Live Summarized": [
        {"friendly_call_sign": "WABC", "viewers": 100, ...}
    ],
    "Other Section": [
        {...}
    ]
}

# TTL: 691200 seconds (8 days)
```

## Fixtures

### `mock_redis`
Tracks all stored data with TTL.

### `minimal_query_results`
Iterator returning minimal results for each SQL statement.

## Key Test Cases

### Test Date Calculation
```python
def test_get_current_week_dates():
    # Verifies week starts on Monday at 00:00:00
    # Verifies week ends on Sunday
    # Checks date span is correct
```

### Test SQL Splitting
```python
def test_sql_statements_splitting():
    # Verifies SQL file splits into multiple statements
    # Checks date placeholder substitution
    # Validates non-empty statements exist
```

### Test Comment Filtering
```python
def test_comment_only_statements_skipped():
    # Counts actual DB execute calls
    # Verifies comment-only statements don't execute
```

### Test Multi-Statement Execution
```python
def test_process_with_minimal_data():
    # Mocks different results for different statements
    # Verifies aggregation into single results_map
    # Checks Redis storage
```

## Mocking Strategy

### Simple Mock (Single Result)
```python
mock_conn_instance.execute_query = Mock(return_value=[{"test": "data"}])
```

### Advanced Mock (Different Results per Statement)
```python
def execute_query_side_effect(statement):
    if "CREATE TEMP TABLE" in statement.upper():
        return []
    elif "[SITENAME]" in statement.upper() or "SITENAME" in statement.upper():
        return [{"viewers": 100, ...}]
    else:
        return []

mock_conn_instance.execute_query = Mock(side_effect=execute_query_side_effect)
```

## Benefits

1. ✅ **Fast** - Sub-second execution
2. ✅ **Focused** - Tests logic, not data
3. ✅ **Maintainable** - Clear mocking strategy
4. ✅ **Comprehensive** - Covers multi-statement complexity
5. ✅ **Isolated** - No external dependencies

## Challenges & Solutions

### Challenge: Multiple SQL Statements
**Solution**: Mock `execute_query` with `side_effect` to return different data per statement.

### Challenge: Date-Dependent Queries
**Solution**: Mock date calculation or use fixed dates in assertions.

### Challenge: Temporary Tables
**Solution**: Mock to return `[]` for CREATE statements, data for SELECT statements.

### Challenge: Comment Filtering
**Solution**: Test that comment-only statements are skipped (count execute calls).

## Adding New Tests

```python
def test_custom_query_result(mock_redis):
    worker = WeeklyStatsWorker()
    worker.redis_client = mock_redis

    def custom_execute(statement):
        if "YOUR QUERY TITLE" in statement.upper():
            return [{"custom_field": "value"}]
        return []

    with patch('scheduler.workers.weekly_stats_worker.RedshiftConnection') as mock_conn:
        mock_conn_instance = MagicMock()
        mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
        mock_conn_instance.__exit__ = Mock(return_value=None)
        mock_conn_instance.execute_query = Mock(side_effect=custom_execute)
        mock_conn.return_value = mock_conn_instance

        result = worker.process()

    # Your assertions
    assert result["sections_count"] >= 0
```

## Integration Testing

For full SQL validation with real database:
- Use test database with minimal schema
- Load 10-20 sample rows per table
- Run actual queries
- Verify results match expectations
- Keep in separate `tests/integration/` directory
