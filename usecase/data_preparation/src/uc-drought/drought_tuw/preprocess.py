import warnings

import geopandas
import numpy as np
from affine import Affine
from pyresample import geometry, kd_tree
from rasterio.enums import Resampling
from shapely.geometry import mapping
from xarray import DataArray
from pathlib import Path

from dedl.schedule import DistributedScheduler

TARGET_RESOLUTION_IN_M = 500
RESOURCES = Path(__file__).parent.parent.parent / "resources"


def normalize(da: DataArray) -> DataArray:
    da_min = da.min()
    return (da - da_min) / (da.max() - da_min)


def preprocess_s1_sm(sm: DataArray, scheduler: DistributedScheduler) -> DataArray:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        print(f"Connecting to {scheduler.host}...")
        print(f"Succeeded!")
        print(f"processing at {scheduler.worker}")
        sm_mm = sm.resample(date='15D').mean('date')
        return normalize(sm_mm).load(scheduler='processes').rio.reproject('EPSG:3857', resampling=Resampling.bilinear)


def preprocess_ascat_sm(sm: DataArray, scheduler: DistributedScheduler) -> DataArray:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        print(f"Connecting to {scheduler.host}...")
        print(f"Succeeded!")
        print(f"processing at {scheduler.worker}")
        sm.rio.write_nodata(np.nan, inplace=True)
        sm_mm = sm.resample(time='10D').mean('time')
        e7proj = "+proj=aeqd +lat_0=53 +lon_0=24 +x_0=5837287.81977 +y_0=2121415.69617 +datum=WGS84 +units=m +no_defs"
        geo_trans = [4200000, 200000, 5500000, 1500000]
        area_def = geometry.AreaDefinition("e7", "", "", e7proj,
                                           200, 200, geo_trans)
        swath_def = geometry.SwathDefinition(lons=sm_mm.lon.values, lats=sm_mm.lat.values)
        sm_area = DataArray(np.full((200, 200, sm_mm.sizes['time']), np.nan, np.float32), {
            'time': sm_mm.time.values,
            'y': np.arange(1500000, 200000, -6500),
            'x': np.arange(4200000, 5500000, 6500)
        }, ['y', 'x', 'time'])
        sm_area.values = kd_tree.resample_nearest(swath_def, sm_mm.values, area_def,
                                                  radius_of_influence=50000)
        sm_area = sm_area.transpose('time', 'y', 'x')
        sm_area = normalize(sm_area)
        sm_area.rio.write_crs(e7proj, inplace=True)
        sm_area.rio.write_transform(Affine.from_gdal(4200000, -6500, 0, 1500000, 0, 6500), inplace=True)
        sm_area.rio.write_nodata(np.nan, inplace=True)
        sm_area = sm_area.rio.reproject('EPSG:3857', resampling=Resampling.bilinear)
        sm_area = clip_dataset_to_shape_file(sm_area, RESOURCES / 'borders/4dmed/catch_med.shp')
        return sm_area.rio.clip_box(7, 37, 18, 46.5, crs='EPSG:4326')


def preprocess_era5l_swvl1(sm: DataArray, scheduler: DistributedScheduler) -> DataArray:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        print(f"Connecting to {scheduler.host}...")
        print(f"Succeeded!")
        print(f"processing at {scheduler.worker}")
        sm_mm = sm.resample(time='15D').mean('time')
        return normalize(sm_mm).load(scheduler='processes').rio.reproject('EPSG:3857', resampling=Resampling.bilinear)


def preprocess_corine(lc: DataArray, scheduler: DistributedScheduler) -> DataArray:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        print(f"Connecting to {scheduler.host}...")
        print(f"Succeeded!")
        print(f"processing at {scheduler.worker}")
        steps = TARGET_RESOLUTION_IN_M // 100
        downsampled = lc.coarsen({'latitude': steps, 'longitude': steps}, boundary='trim').median().load()
        return downsampled.transpose('lc', 'latitude', 'longitude').rio.write_crs('EPSG:4326').rio.reproject(
            'EPSG:3857')


def clip_dataset_to_shape_file(ds, clip_shape_file):
    clip_shape = geopandas.read_file(clip_shape_file)
    ds = ds.rio.clip(clip_shape.geometry.apply(mapping), clip_shape.crs, drop=False, invert=False)
    return ds
