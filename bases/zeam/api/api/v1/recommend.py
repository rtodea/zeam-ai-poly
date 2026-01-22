import json
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException
from zeam.api.schemas import (
    RecommendationRequest,
    RecommendationResponse,
    CuratedRecommendationResponse,
    ContentItem,
    ContentType,
    CuratedRecommendationRequest
)
from zeam.redis_client.client import get_redis_client
from zeam.worker_registry.curated_content import get_curated_content_redis_key
from redis.asyncio import Redis

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/recommend", response_model=RecommendationResponse)
async def get_recommendations(
    request: RecommendationRequest,
    redis: Redis = Depends(get_redis_client)
):
    """
    Get popular content recommendations based on user context.
    """
    # Logic: 
    # 1. Try to find specific popularity list for DMA
    # 2. Fallback to global popularity
    
    keys_to_try = []
    
    if request.dmaid:
        keys_to_try.append(f"zeam-recommender:popularity:dma:{request.dmaid}")
    
    keys_to_try.append("zeam-recommender:popularity:global")
    
    items_data = None
    used_key = None
    
    for key in keys_to_try:
        data = await redis.get(key)
        if data:
            items_data = json.loads(data)
            used_key = key
            break
            
    if not items_data:
        # Fallback to empty response if nothing found (or global missing)
        logger.warning("No popularity data found in Redis")
        return RecommendationResponse()
    
    # items_data is a list of dicts (ContentItem)
    # We need to segregate them into the response buckets
    
    response = RecommendationResponse()
    
    try:
        for item_dict in items_data:
            item = ContentItem(**item_dict)
            if item.type == ContentType.CHANNEL:
                response.channels.append(item)
            elif item.type == ContentType.SHOW:
                response.shows.append(item)
            elif item.type == ContentType.VOD:
                response.vods.append(item)
            elif item.type == ContentType.CLIP:
                response.clips.append(item)
            elif item.type == ContentType.LIVE_EVENT:
                response.live_events.append(item)
    except Exception as e:
        logger.error(f"Error parsing cached data from key {used_key}: {e}")
        raise HTTPException(status_code=500, detail="Internal data error")

    return response


@router.post("/recommend/{content_type}", response_model=CuratedRecommendationResponse)
async def get_content_recommendations(
    content_type: str,
    request: CuratedRecommendationRequest,
    redis: Redis = Depends(get_redis_client)
):
    """
    Get recommendations for a specific content type.
    Currently supports: 'curated'
    """
    if content_type != "curated":
        raise HTTPException(status_code=400, detail=f"Unsupported content type: {content_type}")

    # Calculate default dates if not provided
    # Default to current week start (Monday) and end (Sunday)
    now = datetime.now()
    
    start_date_str = request.start_date
    if not start_date_str:
        start_of_week = now - timedelta(days=now.weekday())
        start_date_str = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        
    end_date_str = request.end_date
    if not end_date_str:
        # If start_date was computed, end_date is end of that week
        # If start_date was provided, we probably should default end_date relative to it or just current week end?
        # Requirement: "end_date = default to current week end date"
        start_of_week = now - timedelta(days=now.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        end_date_str = end_of_week.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    dma_suffix = str(request.dma_id) if request.dma_id else "global"
    
    # Redis Key: popularity:curated:{start_date}:{end_date}:{dma_id_or_global}
    # We need to extract YYYY-MM-DD from the strings
    
    # redis_key = f"zeam-recommender:popularity:curated:{key_start_date}:{key_end_date}:{dma_suffix}"
    # Use shared function
    # Note: the shared function expects strings, possibly with HH:MM:SS, but it handles splitting.
    redis_key = get_curated_content_redis_key(start_date_str, end_date_str, request.dma_id)
    
    logger.info(f"Fetching curated content from key: {redis_key}")
    
    data = await redis.get(redis_key)
    if not data:
        return CuratedRecommendationResponse()
        
    try:
        items_data = json.loads(data)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode data from {redis_key}")
        return CuratedRecommendationResponse()

    response = CuratedRecommendationResponse()
    
    # Filter/Limit items if needed? 
    # The worker already limits items via SQL, but the request has 'items' count.
    # The user says "items = default to 10". If Redis has more, we should slice?
    # Or if Redis has fewer?
    # I'll slice if it exceeds request.items
    
    count = 0
    limit = request.items if request.items else 10
    
    try:
        for item_dict in items_data:
            if count >= limit:
                break
                
            item = ContentItem(**item_dict)
            response.items.append(item)
            
            count += 1
            
    except Exception as e:
        logger.error(f"Error parsing curated data: {e}")
        return CuratedRecommendationResponse() # Return empty on error to avoid crash? or raise?
        
    return response

