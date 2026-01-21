# SQL Queries

This directory contains SQL queries used by workers to fetch data from the source database.

## Available Queries

### Production Queries





## Adding New Queries

1. Create a new `.sql` file with a descriptive name (e.g., `top_channels_by_category.sql`)
2. Write your SQL query
3. Reference it in your worker using `self.load_sql_query("your_file.sql")`

## Best Practices

- Use parameterized queries when possible
- Add comments explaining complex logic
- Keep queries focused on a single purpose
- Test queries against your database schema before deploying
