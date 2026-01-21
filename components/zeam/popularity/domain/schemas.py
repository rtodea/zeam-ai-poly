from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field

class ContentType(str, Enum):
    CHANNEL = "channel"
    SHOW = "show"
    VOD = "vod"
    CLIP = "clip"
    LIVE_EVENT = "live_event"

class RecommendationRequest(BaseModel):
    deviceidentifier: str = Field(..., description="Device/session-level identifier")
    islocalized: bool = Field(..., description="Whether geo fields are populated from localization")
    latitude: Optional[float] = Field(None, description="Latitude (approximate)")
    longitude: Optional[float] = Field(None, description="Longitude (approximate)")
    dmaid: Optional[int] = Field(None, description="DMA market id")
    clientplatformid: Optional[int] = Field(None, description="Client platform taxonomy id")

class CuratedRecommendationRequest(BaseModel):
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD HH:MM:SS)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD HH:MM:SS)")
    dma_id: Optional[int] = Field(None, description="DMA ID")
    items: Optional[int] = Field(10, description="Number of items to return")

class ContentItem(BaseModel):
    id: str
    title: str
    type: ContentType
    description: Optional[str] = None
    image_url: Optional[str] = None
    # Additional metadata can be added here

class RecommendationResponse(BaseModel):
    channels: List[ContentItem] = Field(default_factory=list)
    shows: List[ContentItem] = Field(default_factory=list)
    vods: List[ContentItem] = Field(default_factory=list)
    clips: List[ContentItem] = Field(default_factory=list)
    live_events: List[ContentItem] = Field(default_factory=list)

class CuratedRecommendationResponse(BaseModel):
    items: List[ContentItem] = Field(default_factory=list)

