# SQL Queries

This directory contains SQL queries used by workers to fetch data from the source database.

## Available Queries

### Production Queries

1. **`dummy.sql`** - Used by `dummy_worker.py`
   - Fetches popularity data for content items
   - Groups by DMA and content type

2. **`weekly_stats.sql`** - Used by `weekly_stats_worker.py`
   - Calculates weekly statistics for the current week
   - Creates temporary tables and multiple result sets
   - Uses date parameters: `{start_date}` and `{end_date}`

3. **`channels_by_dma.sql`** - Used by `channels_by_dma_worker.py`
   - Fetches channel counts by DMA
   - Joins media_channel, site_channel, and dma tables

### Example Queries

4. **`top_streams.sql`** - Example query template
   - Template for fetching top live streams
   - Not used in production (example only)

## Adding New Queries

1. Create a new `.sql` file with a descriptive name (e.g., `top_channels_by_category.sql`)
2. Write your SQL query
3. Reference it in your worker using `self.load_sql_query("your_file.sql")`

## Best Practices

- Use parameterized queries when possible
- Add comments explaining complex logic
- Keep queries focused on a single purpose
- Test queries against your database schema before deploying
