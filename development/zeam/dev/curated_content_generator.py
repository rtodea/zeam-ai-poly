"""
Development script for generating and retrieving curated content popularity data.

This script provides functions to:
1. Generate curated content popularity for all DMAs (including global)
2. Generate curated content popularity by platform for all platforms (including global)
3. Retrieve results from Redis for all DMAs
4. Retrieve results from Redis for all platforms

Usage:
    PYTHONPATH=bases:components uv run --project development/zeam/dev python development/zeam/dev/curated_content_generator.py
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional

from zeam.redshift import execute_query
from zeam.redis_client import get_json
from zeam.worker_registry.curated_content import (
    run_curated_content_task,
    get_curated_content_redis_key,
)
from zeam.worker_registry.curated_content_by_platform import (
    run_curated_content_by_platform_task,
    get_curated_content_by_platform_redis_key,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default date range
DEFAULT_START_DATE = "2026-01-08"
DEFAULT_END_DATE = "2026-01-22"


# =============================================================================
# DMA and Platform Lookup Functions
# =============================================================================

def get_all_dmas() -> List[Dict[str, Any]]:
    """
    Fetches all DMAs from the database.
    
    Returns:
        List of dicts with 'dma_id' and 'dma_name' keys.
    """
    query = "SELECT dma_id, dma_name FROM prod.dma"
    results = execute_query(query)
    logger.info(f"Found {len(results)} DMAs")
    return results


def get_all_platforms() -> List[Dict[str, Any]]:
    """
    Fetches all platforms from the database.
    
    Returns:
        List of dicts with 'platformos_id' and 'name' keys.
    """
    query = "SELECT platformos_id, name FROM prod.platform_os"
    results = execute_query(query)
    logger.info(f"Found {len(results)} platforms")
    return results


# =============================================================================
# Generation Functions - DMA-based
# =============================================================================

def generate_curated_content_for_dma(
    dma_id: Optional[int],
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    item_count: int = 10
) -> Dict[str, Any]:
    """
    Generates curated content popularity for a specific DMA (or global if dma_id is None).
    
    Args:
        dma_id: DMA ID or None for global
        start_date: Start date string
        end_date: End date string
        item_count: Number of items to fetch
        
    Returns:
        Result dict from the worker task.
    """
    dma_label = dma_id if dma_id else "global"
    logger.info(f"Generating curated content for DMA: {dma_label}")
    
    result = run_curated_content_task(
        start_date=start_date,
        end_date=end_date,
        dma_id=dma_id,
        item_count=item_count,
        run_id=f"dev-script-dma-{dma_label}"
    )
    
    logger.info(f"Generated {result['rows_count']} rows for DMA {dma_label}, key: {result['redis_key']}")
    return result


def generate_curated_content_for_all_dmas(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    item_count: int = 10
) -> List[Dict[str, Any]]:
    """
    Generates curated content popularity for all DMAs including global.
    
    Args:
        start_date: Start date string
        end_date: End date string
        item_count: Number of items to fetch per DMA
        
    Returns:
        List of result dicts from worker tasks.
    """
    results = []
    
    # Generate global first
    logger.info("=== Generating Global Curated Content ===")
    global_result = generate_curated_content_for_dma(None, start_date, end_date, item_count)
    results.append(global_result)
    
    # Get all DMAs and generate for each
    dmas = get_all_dmas()
    logger.info(f"=== Generating Curated Content for {len(dmas)} DMAs ===")
    
    for dma in dmas:
        dma_id = dma.get('dma_id')
        dma_name = dma.get('dma_name', 'Unknown')
        logger.info(f"Processing DMA: {dma_name} (ID: {dma_id})")
        
        result = generate_curated_content_for_dma(dma_id, start_date, end_date, item_count)
        results.append(result)
    
    logger.info(f"=== Completed generating curated content for {len(results)} entries (1 global + {len(dmas)} DMAs) ===")
    return results


# =============================================================================
# Generation Functions - Platform-based
# =============================================================================

def generate_curated_content_for_platform(
    platformos_id: Optional[int],
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    item_count: int = 10
) -> Dict[str, Any]:
    """
    Generates curated content popularity for a specific platform (or global if platformos_id is None).
    
    Args:
        platformos_id: Platform OS ID or None for global
        start_date: Start date string
        end_date: End date string
        item_count: Number of items to fetch
        
    Returns:
        Result dict from the worker task.
    """
    platform_label = platformos_id if platformos_id else "global"
    logger.info(f"Generating curated content for Platform: {platform_label}")
    
    result = run_curated_content_by_platform_task(
        start_date=start_date,
        end_date=end_date,
        platformos_id=platformos_id,
        item_count=item_count,
        run_id=f"dev-script-platform-{platform_label}"
    )
    
    logger.info(f"Generated {result['rows_count']} rows for Platform {platform_label}, key: {result['redis_key']}")
    return result


def generate_curated_content_for_all_platforms(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    item_count: int = 10
) -> List[Dict[str, Any]]:
    """
    Generates curated content popularity for all platforms including global.
    
    Args:
        start_date: Start date string
        end_date: End date string
        item_count: Number of items to fetch per platform
        
    Returns:
        List of result dicts from worker tasks.
    """
    results = []
    
    # Generate global first
    logger.info("=== Generating Global Curated Content by Platform ===")
    global_result = generate_curated_content_for_platform(None, start_date, end_date, item_count)
    results.append(global_result)
    
    # Get all platforms and generate for each
    platforms = get_all_platforms()
    logger.info(f"=== Generating Curated Content for {len(platforms)} Platforms ===")
    
    for platform in platforms:
        platformos_id = platform.get('platformos_id')
        platform_name = platform.get('name', 'Unknown')
        logger.info(f"Processing Platform: {platform_name} (ID: {platformos_id})")
        
        result = generate_curated_content_for_platform(platformos_id, start_date, end_date, item_count)
        results.append(result)
    
    logger.info(f"=== Completed generating curated content for {len(results)} entries (1 global + {len(platforms)} platforms) ===")
    return results


# =============================================================================
# Redis Retrieval Functions - DMA-based
# =============================================================================

async def get_curated_content_from_redis_for_dma(
    dma_id: Optional[int],
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE
) -> Dict[str, Any]:
    """
    Retrieves curated content from Redis for a specific DMA.
    
    Args:
        dma_id: DMA ID or None for global
        start_date: Start date string
        end_date: End date string
        
    Returns:
        Dict with 'dma_id', 'redis_key', and 'data' keys.
    """
    redis_key = get_curated_content_redis_key(start_date, end_date, dma_id)
    data = await get_json(redis_key)
    
    return {
        "dma_id": dma_id,
        "redis_key": redis_key,
        "data": data,
        "count": len(data) if data else 0
    }


async def get_curated_content_from_redis_for_all_dmas(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE
) -> List[Dict[str, Any]]:
    """
    Retrieves curated content from Redis for all DMAs including global.
    
    Args:
        start_date: Start date string
        end_date: End date string
        
    Returns:
        List of dicts with DMA data from Redis.
    """
    results = []
    
    # Get global first
    logger.info("=== Retrieving Global Curated Content from Redis ===")
    global_data = await get_curated_content_from_redis_for_dma(None, start_date, end_date)
    results.append(global_data)
    logger.info(f"Global: {global_data['count']} items")
    
    # Get all DMAs
    dmas = get_all_dmas()
    logger.info(f"=== Retrieving Curated Content from Redis for {len(dmas)} DMAs ===")
    
    for dma in dmas:
        dma_id = dma.get('dma_id')
        dma_name = dma.get('dma_name', 'Unknown')
        
        data = await get_curated_content_from_redis_for_dma(dma_id, start_date, end_date)
        data['dma_name'] = dma_name
        results.append(data)
        logger.info(f"DMA {dma_name} (ID: {dma_id}): {data['count']} items")
    
    logger.info(f"=== Retrieved data for {len(results)} entries ===")
    return results


# =============================================================================
# Redis Retrieval Functions - Platform-based
# =============================================================================

async def get_curated_content_from_redis_for_platform(
    platformos_id: Optional[int],
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE
) -> Dict[str, Any]:
    """
    Retrieves curated content from Redis for a specific platform.
    
    Args:
        platformos_id: Platform OS ID or None for global
        start_date: Start date string
        end_date: End date string
        
    Returns:
        Dict with 'platformos_id', 'redis_key', and 'data' keys.
    """
    redis_key = get_curated_content_by_platform_redis_key(start_date, end_date, platformos_id)
    data = await get_json(redis_key)
    
    return {
        "platformos_id": platformos_id,
        "redis_key": redis_key,
        "data": data,
        "count": len(data) if data else 0
    }


async def get_curated_content_from_redis_for_all_platforms(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE
) -> List[Dict[str, Any]]:
    """
    Retrieves curated content from Redis for all platforms including global.
    
    Args:
        start_date: Start date string
        end_date: End date string
        
    Returns:
        List of dicts with platform data from Redis.
    """
    results = []
    
    # Get global first
    logger.info("=== Retrieving Global Curated Content by Platform from Redis ===")
    global_data = await get_curated_content_from_redis_for_platform(None, start_date, end_date)
    results.append(global_data)
    logger.info(f"Global: {global_data['count']} items")
    
    # Get all platforms
    platforms = get_all_platforms()
    logger.info(f"=== Retrieving Curated Content from Redis for {len(platforms)} Platforms ===")
    
    for platform in platforms:
        platformos_id = platform.get('platformos_id')
        platform_name = platform.get('name', 'Unknown')
        
        data = await get_curated_content_from_redis_for_platform(platformos_id, start_date, end_date)
        data['platform_name'] = platform_name
        results.append(data)
        logger.info(f"Platform {platform_name} (ID: {platformos_id}): {data['count']} items")
    
    logger.info(f"=== Retrieved data for {len(results)} entries ===")
    return results


# =============================================================================
# Main Entry Point
# =============================================================================

async def run_all():
    """
    Runs all generation and retrieval tasks.
    """
    start_date = DEFAULT_START_DATE
    end_date = DEFAULT_END_DATE
    
    print("\n" + "="*80)
    print("CURATED CONTENT GENERATOR - DEV SCRIPT")
    print(f"Period: {start_date} to {end_date}")
    print("="*80 + "\n")
    
    # 1. Generate curated content for all DMAs
    print("\n>>> STEP 1: Generating curated content for all DMAs...")
    dma_generation_results = generate_curated_content_for_all_dmas(start_date, end_date)
    print(f"Generated data for {len(dma_generation_results)} DMA entries\n")
    
    # 2. Generate curated content for all platforms
    print("\n>>> STEP 2: Generating curated content for all platforms...")
    platform_generation_results = generate_curated_content_for_all_platforms(start_date, end_date)
    print(f"Generated data for {len(platform_generation_results)} platform entries\n")
    
    # 3. Retrieve from Redis for all DMAs
    print("\n>>> STEP 3: Retrieving curated content from Redis for all DMAs...")
    dma_redis_results = await get_curated_content_from_redis_for_all_dmas(start_date, end_date)
    print(f"Retrieved data for {len(dma_redis_results)} DMA entries\n")
    
    # 4. Retrieve from Redis for all platforms
    print("\n>>> STEP 4: Retrieving curated content from Redis for all platforms...")
    platform_redis_results = await get_curated_content_from_redis_for_all_platforms(start_date, end_date)
    print(f"Retrieved data for {len(platform_redis_results)} platform entries\n")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"DMA Generation Results: {len(dma_generation_results)} entries")
    print(f"Platform Generation Results: {len(platform_generation_results)} entries")
    print(f"DMA Redis Retrieval: {len(dma_redis_results)} entries")
    print(f"Platform Redis Retrieval: {len(platform_redis_results)} entries")
    print("="*80 + "\n")
    
    return {
        "dma_generation": dma_generation_results,
        "platform_generation": platform_generation_results,
        "dma_redis": dma_redis_results,
        "platform_redis": platform_redis_results,
    }


if __name__ == "__main__":
    asyncio.run(run_all())
