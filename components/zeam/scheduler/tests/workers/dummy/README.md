# Dummy Worker Tests

## Overview

Tests for `dummy_worker` using minimal synthetic data to validate content grouping by DMA and content type.

## Testing Strategy

### 1. Minimal Data Approach
Uses **3 rows** covering essential cases:
- Different content types (channel, show, vod)
- Items with DMA (grouped by DMA)
- Items without DMA (grouped as global)

### 2. Synthetic Data Structure

```python
[
    {"content_type": "channel", "content_id": 123, "title": "Channel 1", "dma_id": 501},
    {"content_type": "show", "content_id": 456, "title": "Show 1", "dma_id": 501},
    {"content_type": "vod", "content_id": 789, "title": "Movie 1", "dma_id": None},
]
```

Matches SQL output:
```sql
content_type, content_id, title, dma_id
```

### 3. Edge Cases Covered

- ✅ Mixed DMA and global items
- ✅ All items global (no DMA)
- ✅ Multiple items in single DMA
- ✅ Empty results
- ✅ Database errors
- ✅ Redis errors
- ✅ ContentItem structure validation

### 4. What We Test

1. **Grouping Logic** - DMA vs global separation
2. **Data Structure** - ContentItem format (id, title, type)
3. **Redis Storage** - Correct keys, TTL, JSON structure
4. **Content Types** - channel, show, vod handling
5. **Error Handling** - Database and Redis failures

## Running Tests

```bash
# Run only dummy worker tests
pytest tests/scheduler/workers/dummy/ -v

# With coverage
pytest tests/scheduler/workers/dummy/ --cov=scheduler.workers.dummy_worker

# Specific test
pytest tests/scheduler/workers/dummy/test_dummy_worker.py::TestDummyWorker::test_dma_grouping -v
```

## Fast Execution

Tests run in **< 1 second**:
- Only 3 data rows
- All mocked (no real DB/Redis)
- No network I/O

## Redis Data Verification

### DMA-specific key:
```python
# Key
"dummy:popularity:dma:{dma_id}"

# Value (JSON array of ContentItems)
[
    {"id": "123", "title": "Channel 1", "type": "channel"},
    {"id": "456", "title": "Show 1", "type": "show"}
]

# TTL: 90000 seconds (25 hours)
```

### Global key:
```python
# Key
"dummy:popularity:global"

# Value (JSON array)
[
    {"id": "789", "title": "Movie 1", "type": "vod"}
]
```

## Fixtures

### `mock_redis`
Tracks all stored data in memory.

### `minimal_db_results`
3 rows with mixed DMA/global, different content types.

### `edge_case_all_global`
2 rows, all without DMA.

### `edge_case_single_dma`
2 rows, same DMA.

## Key Test Cases

### Test DMA Grouping
```python
def test_dma_grouping(mock_redis, minimal_db_results):
    # Verifies items with same dma_id are grouped together
    # Checks JSON structure and TTL
```

### Test Global Grouping
```python
def test_global_grouping(mock_redis, minimal_db_results):
    # Verifies items with dma_id=None go to global key
    # Validates separate storage
```

### Test ContentItem Structure
```python
def test_content_item_structure(mock_redis, minimal_db_results):
    # Validates id, title, type fields
    # Ensures correct data types
```

## Benefits

1. ✅ **Fast** - Millisecond execution
2. ✅ **Comprehensive** - Covers all edge cases
3. ✅ **Maintainable** - Clear synthetic data
4. ✅ **Isolated** - No external dependencies
5. ✅ **Reliable** - Mocked connections

## Adding New Tests

```python
def test_custom_case(mock_redis):
    test_data = [
        {"content_type": "custom", "content_id": 999, "title": "Test", "dma_id": 123}
    ]

    worker = DummyWorker()
    worker.redis_client = mock_redis

    with patch('scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
        mock_conn_instance = MagicMock()
        mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
        mock_conn_instance.__exit__ = Mock(return_value=None)
        mock_conn_instance.execute_query = Mock(return_value=test_data)
        mock_conn.return_value = mock_conn_instance

        result = worker.process()

    # Your assertions
    assert result["keys_updated"] == 1
```
