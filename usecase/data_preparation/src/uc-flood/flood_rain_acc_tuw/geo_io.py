from pathlib import Path

import geopandas as gpd
import rioxarray
from geopandas import GeoDataFrame
from xarray import DataArray

from dedl.parameters import Extent

DOWNSAMPLE_10M_TO_500M = 500 // 10


def read_shape_roi(shape_file: Path, roi: Extent) -> GeoDataFrame:
    return gpd.read_file(shape_file, bbox=(roi.min_x, roi.min_y, roi.max_x, roi.max_y))


def read_raster_roi(raster_file: Path, roi: Extent) -> DataArray:
    return rioxarray.open_rasterio(raster_file).rio.clip_box(*roi.to_tuple(), crs=roi.crs)
