import argparse
import re

import geopandas
import xarray as xr

from datetime import date
from pathlib import Path

from eotransform_pandas.filesystem.gather import gather_files
from eotransform_pandas.filesystem.naming.geopathfinder_conventions import yeoda_naming_convention

import flood_rain_acc_tuw.geo_io as tuw_io
from dedl.parameters import Extent
from dedl.services import Discover

RESOURCES = Path(__file__).parent.parent.parent / "resources"
USER_RESOURCES = RESOURCES / "user"


def restructure_flood(extent: Extent, ts: date, zarr_archive: Path) -> None:
    coastlines = geopandas.read_file(USER_RESOURCES / "coastlines/ne_10m_land.shp")
    discover_floods = Discover(dataset='gfm/floods')
    flood_map = discover_floods.get(extent, ts)
    flood_map.name = "flood"
    flood_map = flood_map.rio.reproject(extent.crs).chunk({'y': 1000, 'x': 1000})
    flood_map = flood_map.rio.write_crs(extent.crs)
    flood_map = flood_map.rio.clip(coastlines.geometry.values, coastlines.crs, drop=False, invert=False)
    flood_map.to_zarr(zarr_archive)


def restructure_built(extent: Extent, zarr_archive: Path) -> None:
    built_surfaces = tuw_io.read_raster_roi(
        USER_RESOURCES / "built/GHS_BUILT_S_E2018_GLOBE_R2022A_54009_10_V1_0_R6_C25.tif", extent)[0]
    built_surfaces.name = "built"
    built_surfaces = built_surfaces.rio.reproject(extent.crs).chunk({'y': 1000, 'x': 1000})
    built_surfaces = built_surfaces.rio.write_crs(extent.crs)
    built_surfaces.to_zarr(zarr_archive)


def restructure_dem(extent: Extent, dem_tif: Path) -> None:
    file_df = gather_files(RESOURCES / "DEDL/COPDEM", yeoda_naming_convention, [
        re.compile("V01R01"), re.compile(f"EQUI7_(AS|AF|EU|NA|SA|OZ)020M"), re.compile(r"E\d\d\dN\d\d\dT3")
    ], index='tile_name')
    dem = xr.open_mfdataset(file_df['filepath'], chunks={}, mask_and_scale=True)
    dem = dem.rio.write_transform(dem.rio.transform(recalc=True))['band_data'].squeeze(drop=True)
    dem.name = 'DEM'
    step = 500 // 20
    dem = dem.coarsen({'y': step, 'x': step}).mean()
    dem = dem.rio.reproject(extent.crs)
    if '_FillValue' in dem.attrs:
        del dem.attrs['_FillValue']
    if 'scale_factor' in dem.attrs:
        del dem.attrs['scale_factor']
    if 'dtype' in dem.attrs:
        del dem.attrs['dtype']
    dem = dem.rio.set_encoding({'_FillValue': -9999, 'scale_factor': 1.0, 'dtype': 'int16'})
    dem.rio.to_raster(dem_tif)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Select and restructure data for DEDL use case")
    parser.add_argument('extent', type=str,
                        help='description of the extent following the format min_x, min_y, max_x, max_y, crs, '
                             'i.e. "66.0, 22.0, 70.0, 30.0, EPSG:4326"')
    parser.add_argument('dataset', type=str, help='Dataset to restructure ("flood", "built" or "DEM")')
    parser.add_argument('out_zarr', type=Path, help='zarr archive to store restructured data in')
    parser.add_argument('-s', '--start', type=str, help='Start date i.e. 2022-08-18 (optional)', default=None)
    args = parser.parse_args()

    if args.dataset == 'flood':
        restructure_flood(Extent.from_arg_str(args.extent), date.fromisoformat(args.start), args.out_zarr)
    elif args.dataset == 'built':
        restructure_built(Extent.from_arg_str(args.extent), args.out_zarr)
    elif args.dataset == 'DEM':
        restructure_dem(Extent.from_arg_str(args.extent), args.out_zarr)
    else:
        raise NotImplementedError(args.dataset)
