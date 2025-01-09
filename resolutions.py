from enum import Enum
from typing import Dict

class YTBulkResolution(str, Enum):
    """Video resolution options with corresponding heights in pixels."""
    RES_4K = "4K", 2160
    RES_1080P = "1080p", 1080
    RES_720P = "720p", 720
    RES_480P = "480p", 480
    RES_360P = "360p", 360

    def __new__(cls, value: str, height: int):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.height = height
        return obj

    @property
    def pixels(self) -> int:
        """Alternative name for height property."""
        return self.height

    @classmethod
    def from_height(cls, height: int) -> 'YTBulkResolution':
        """Get the closest resolution that doesn't exceed the given height."""
        available_heights = sorted([r.height for r in cls], reverse=True)
        for res_height in available_heights:
            if height >= res_height:
                return next(r for r in cls if r.height == res_height)
        return cls.RES_360P  # Return lowest if no match found