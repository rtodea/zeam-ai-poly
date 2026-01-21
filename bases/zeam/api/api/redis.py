from fastapi import APIRouter, Depends, HTTPException, Query
from redis.asyncio import Redis

from zeam.redis.client import get_redis_client

router = APIRouter()


@router.get("/stats")
async def redis_stats(redis: Redis = Depends(get_redis_client)):
    # Count of keys
    try:
        keys_count = await redis.dbsize()
    except Exception:
        keys_count = 0

    # Memory usage in MiB
    size_mib = 0.0
    try:
        info = await redis.info(section="memory")
        used_bytes = info.get("used_memory")
        if isinstance(used_bytes, (int, float)):
            size_mib = round(float(used_bytes) / (1024 * 1024), 2)
    except Exception:
        size_mib = 0.0

    return {"keys": int(keys_count), "sizeInMiB": size_mib}


@router.get("/keys")
async def list_redis_keys(
    pattern: str | None = Query(default=None, description="Glob-style pattern, e.g. user:*"),
    limit: int | None = Query(default=None, ge=1, le=10000, description="Max number of keys to return"),
    redis: Redis = Depends(get_redis_client),
):
    """
    Return all keys (optionally filtered by a pattern). Uses SCAN to avoid blocking Redis.
    Results are sorted for stable output. If `limit` is provided, collection stops after that many keys.
    """
    try:
        keys_iter = redis.scan_iter(match=pattern) if pattern else redis.scan_iter()
        keys: list[str] = []
        async for k in keys_iter:
            keys.append(k)
            if limit is not None and len(keys) >= limit:
                break
        return sorted(keys)
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unavailable")


@router.get("/{redis_key}")
async def get_redis_key(redis_key: str, redis: Redis = Depends(get_redis_client)):
    try:
        key_type = await redis.type(redis_key)
    except Exception:
        # If Redis is unavailable
        raise HTTPException(status_code=503, detail="Redis unavailable")

    if not key_type or key_type == "none":
        raise HTTPException(status_code=404, detail="Key not found")

    # Fetch value based on type
    if key_type == "string":
        value = await redis.get(redis_key)
        return value
    if key_type == "hash":
        value = await redis.hgetall(redis_key)
        return value
    if key_type == "list":
        value = await redis.lrange(redis_key, 0, -1)
        return value
    if key_type == "set":
        members = await redis.smembers(redis_key)
        # smembers returns a set; convert to a sorted list for stable JSON
        return sorted(list(members))
    if key_type == "zset":
        # Return list of [member, score]
        items = await redis.zrange(redis_key, 0, -1, withscores=True)
        # Ensure deterministic order (zrange already sorted by score then member)
        return [[member, score] for member, score in items]

    # Fallback: return type information only
    return {"type": key_type}


@router.delete("/{redis_key}", status_code=204)
async def delete_redis_key(redis_key: str, redis: Redis = Depends(get_redis_client)):
    """
    Delete a specific Redis key.
    - 204 No Content when deleted
    - 404 if the key does not exist
    - 503 if Redis is unavailable
    """
    try:
        deleted = await redis.delete(redis_key)
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    if not deleted:
        raise HTTPException(status_code=404, detail="Key not found")

    # 204 No Content
    return None
