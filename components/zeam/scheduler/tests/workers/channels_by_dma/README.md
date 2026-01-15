# Channels by DMA Worker Tests

## Overview

Tests for `channels_by_dma_worker` using minimal synthetic data to validate SQL query results and Redis storage.

## Testing Strategy

### 1. Minimal Data Approach
Instead of testing with thousands of rows, we use **3 minimal rows** that cover the essential cases:
- Multiple DMAs
- Different channel counts
- Proper grouping and ordering

### 2. Synthetic Data Structure

```python
[
    {"dma_name": "New York", "dma_id": 501, "channelcount": 10},
    {"dma_name": "Los Angeles", "dma_id": 803, "channelcount": 8},
    {"dma_name": "Chicago", "dma_id": 602, "channelcount": 5},
]
```

Matches SQL output:
```sql
dma.dma_name, dma.dma_id, count(*) as channelCount
```

### 3. Edge Cases Covered

- ✅ Minimal valid data (3 rows)
- ✅ Single result
- ✅ Zero channel count
- ✅ Empty results from database
- ✅ Database connection errors
- ✅ Redis storage errors

### 4. What We Test

1. **Worker Initialization** - Query path and Redis client
2. **Data Processing** - Correct transformation and counting
3. **Redis Storage** - Correct keys, structure, and TTL
4. **Error Handling** - Database and Redis errors
5. **SQL Query** - File loads and has expected structure

## Running Tests

```bash
# Run only channels_by_dma tests
pytest tests/scheduler/workers/channels_by_dma/ -v

# Run with coverage
pytest tests/scheduler/workers/channels_by_dma/ --cov=scheduler.workers.channels_by_dma_worker

# Run specific test
pytest tests/scheduler/workers/channels_by_dma/test_channels_by_dma_worker.py::TestChannelsByDMAWorker::test_process_with_minimal_data -v
```

## Fast Execution

Tests run in **< 1 second** because:
- Only 3 data rows per test
- Mocked database connections (no real DB)
- Mocked Redis connections (no real Redis)
- No network I/O
- Focused assertions

## Redis Data Verification

Tests verify:
```python
# Key format
"channels:dma:{dma_id}"

# Value structure (JSON)
{
    "dma_name": "New York",
    "dma_id": 501,
    "channelcount": 10
}

# TTL: 90000 seconds (25 hours)
```

## Fixtures

### `mock_redis`
Mocked Redis client that tracks all stored data in memory.

### `minimal_db_results`
3 rows representing typical query results.

### `edge_case_db_results`
1 row with edge case (zero channel count).

## Adding New Tests

```python
def test_new_case(mock_redis):
    worker = ChannelsByDMAWorker()
    worker.redis_client = mock_redis

    # Create synthetic data
    test_data = [{"dma_name": "Test", "dma_id": 1, "channelcount": 5}]

    # Mock database
    with patch('scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
        mock_conn_instance = MagicMock()
        mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
        mock_conn_instance.__exit__ = Mock(return_value=None)
        mock_conn_instance.execute_query = Mock(return_value=test_data)
        mock_conn.return_value = mock_conn_instance

        result = worker.process()

    # Assertions
    assert result["dma_count"] == 1
```

## Benefits

1. ✅ **Fast** - Runs in milliseconds
2. ✅ **Focused** - Tests logic, not data volume
3. ✅ **Reliable** - No external dependencies
4. ✅ **Maintainable** - Clear synthetic data
5. ✅ **Comprehensive** - Covers all edge cases

## Integration Tests

For full integration testing with real database:
- See `tests/integration/` (if/when needed)
- Use Docker Compose with test database
- Run separately from unit tests
