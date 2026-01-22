import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from zeam.redshift import execute_query

logger = logging.getLogger(__name__)

def get_curated_content_sql(start_date: str, end_date: str, dma_id: Optional[int] = None, limit: int = 10) -> str:
    """
    Generates the SQL query for curated content popularity.
    """
    query_path = Path(__file__).parent / "sql" / "curated_content_popularity.sql"
    
    try:
        query_content = query_path.read_text()
    except FileNotFoundError:
        logger.error(f"Query file not found at {query_path}")
        raise

    dma_filter = ""
    if dma_id:
        dma_filter = f"AND log.dmaid = {dma_id}"

    formatted_query = query_content.format(
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        dma_filter=dma_filter,
    )
    
    return formatted_query


def get_results(start_date: str, end_date: str, dma_id: Optional[int] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Executes the curated content popularity query and returns the results.
    
    Args:
        start_date: Start date string
        end_date: End date string
        dma_id: Optional DMA ID
        limit: Number of items limit
        
    Returns:
        List of result rows.
    """
    query = get_curated_content_sql(start_date, end_date, dma_id, limit)
    
    return execute_query(query)
