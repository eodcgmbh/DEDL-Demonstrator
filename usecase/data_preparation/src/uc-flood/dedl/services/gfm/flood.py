import re
from datetime import date
from pathlib import Path
from typing import Optional

import dask
import rioxarray  # noqa
import xarray as xr
from eotransform_pandas.filesystem.gather import gather_files
from eotransform_pandas.filesystem.naming.geopathfinder_conventions import yeoda_naming_convention
from equi7grid.equi7grid import Equi7Grid
from geospade.crs import SpatialRef
from geospade.raster import MosaicGeometry, Tile
from xarray import DataArray

from dedl.parameters import Extent
from dedl.services.common import AccessProtocol

DATA_ROOT = Path(__file__).parent.parent.parent.parent.parent / "resources/DEDL/FLOOD"


def tile_from_e7tile_name(tile_name: str, grid_name: str) -> Tile:
    e7tile = Equi7Grid(20).create_tile(f"{grid_name}_{tile_name}")
    return Tile(*e7tile.shape_px(), sref=SpatialRef(e7tile.core.projection.wkt), geotrans=e7tile.geotransform(),
                name=tile_name)


class Flood(AccessProtocol):
    DOWNSAMPLING_20M_TO_500M = 500 // 20

    def __init__(self, data_root: Optional[Path] = None):
        self._file_df = gather_files(data_root or DATA_ROOT, yeoda_naming_convention, [
            re.compile("V0M2R3"), re.compile(f"EQUI7_(AS|AF|EU|NA|SA|OZ)020M"), re.compile(r"E\d\d\dN\d\d\dT3")
        ], index='datetime_1')

        tiles = []
        for grid in set(self._file_df['grid_name']):
            grid_df = self._file_df[self._file_df['grid_name'] == grid]
            tiles.extend([tile_from_e7tile_name(t, grid) for t in grid_df['tile_name']])

        self._mosaic = MosaicGeometry.from_tile_list(tiles)

    def get(self, extent: Extent, start: date, end: Optional[date] = None) -> DataArray:
        with dask.config.set(**{'array.slicing.split_large_chunks': False}):
            if end is None:
                time_slice = self._file_df.loc[str(start)]
            else:
                time_slice = self._file_df.loc[start:end]
            spatial_slice = self._mosaic.select_by_geom(extent.to_ogr())
            selected_df = time_slice.loc[time_slice['tile_name'].isin(spatial_slice.tile_names)]
            ds = xr.open_mfdataset(selected_df['filepath'], combine='nested', concat_dim='band',
                                   chunks={'band': 1, 'y': 15000, 'x': 15000}).chunk(
                {'band': 1, 'y': 15000, 'x': 15000})
            ds = ds.rio.write_transform(ds.rio.transform(recalc=True))
            da = ds['band_data'].rio.clip_box(*extent.to_tuple(), crs=extent.crs).max('band', skipna=True)
            da.attrs['tiles'] = list(sorted(set(spatial_slice.tile_names)))
            return da
