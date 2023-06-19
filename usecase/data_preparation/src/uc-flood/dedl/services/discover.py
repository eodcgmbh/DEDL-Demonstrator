from datetime import date
from pathlib import Path
from typing import Optional

from xarray import DataArray

from dedl.parameters import Extent
from dedl.services.common import AccessProtocol
from dedl.services.copernicus import PredictedRainfall
from dedl.services.gfm import Flood


class Discover(AccessProtocol):
    def __init__(self, dataset: str, cache_root: Optional[Path] = None):
        if dataset == 'gfm/floods':
            self._service = Flood(cache_root)
        elif dataset == 'copernicus/predicted_rainfall':
            self._service = PredictedRainfall(cache_root)
        else:
            raise NotImplementedError(dataset)

    def get(self, extent: Extent, start: date, end: Optional[date] = None) -> DataArray:
        return self._service.get(extent, start, end)
