from abc import abstractmethod
from typing import Optional
from datetime import date

from xarray import DataArray

from dedl.parameters import Extent

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol


class AccessProtocol(Protocol):
    @abstractmethod
    def get(self, extent: Extent, start: date, end: Optional[date] = None) -> DataArray:
        ...
