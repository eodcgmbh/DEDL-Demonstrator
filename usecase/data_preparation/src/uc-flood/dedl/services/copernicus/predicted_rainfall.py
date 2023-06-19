from datetime import date, datetime
from pathlib import Path
from typing import Optional

import cdsapi
import dask.array
import numpy as np
import pandas as pd
import xarray as xr
from xarray import DataArray, Dataset

from dedl.geo_grid import calc_grid_box_area
from dedl.parameters import Extent
from dedl.services.common import AccessProtocol

CACHE_ROOT = Path(__file__).parent.parent.parent.parent.parent / "resources/DEDL/predicted_rainfall.zarr"


def request_grib(day: date) -> Path:
    c = cdsapi.Client()

    out_file = f'/tmp/total_precipitation_era5land_{day.year}{day.month}{day.day}.grib'
    c.retrieve(
        'reanalysis-era5-land',
        {
            'product_type': 'reanalysis',
            'format': 'grib',
            'time': [
                '00:00', '01:00', '02:00', '03:00',
                '04:00', '05:00', '06:00', '07:00',
                '08:00', '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00', '15:00',
                '16:00', '17:00', '18:00', '19:00',
                '20:00', '21:00', '22:00', '23:00',
            ],
            'day': day.day,
            'month': day.month,
            'year': day.year,
            'area': [90, -180, -90, 180],

            'variable': 'total_precipitation',
        },
        out_file)

    return Path(out_file)


def create_empty_rainfall_zarr_store(path: Path) -> None:
    lats = np.arange(90, -90.1, -0.1)
    lons = np.arange(-180, 180, 0.1)
    areas = dask.array.from_array(calc_grid_box_area(lats, lons).astype(np.float32), chunks=(100, 100))
    days = pd.date_range(date(2015, 1, 1), date(2024, 1, 1), freq='D')
    dummies_f32 = dask.array.empty(shape=(len(days), *areas.shape), chunks=(100, 100, 100), dtype=np.float32)
    dummies_bool = dask.array.zeros(shape=(len(days),), chunks=(100,), dtype=np.uint8)
    ds = Dataset({
        'tp': (('time', 'latitude', 'longitude'), dummies_f32),
        'covered': (('time',), dummies_bool),
        'area': (('latitude', 'longitude'), areas)
    }, coords={'time': ('time', days), 'latitude': ('latitude', lats), 'longitude': ('longitude', lons)})
    ds['covered'].rio.write_nodata(0, encoded=True, inplace=True)
    ds.to_zarr(path, compute=False)
    ds.drop_vars(('tp', 'covered', 'time')).to_zarr(path, mode='a')


def to_datetime(start):
    return datetime(start.year, start.month, start.day)


def find_index(ds, coord, value):
    return (ds[coord] == ds[coord].sel({coord: value}, method='nearest')).argmax().item()


class PredictedRainfall(AccessProtocol):
    def __init__(self, cache: Optional[Path] = None):
        self._cache = cache or CACHE_ROOT

    def get(self, extent: Extent, start: date, end: Optional[date] = None) -> DataArray:
        if not self._cache.exists():
            create_empty_rainfall_zarr_store(self._cache)
        cache_ds = xr.open_zarr(self._cache)
        self._ensure_cache_covers(cache_ds, start, end)
        selection = cache_ds.sel(time=slice(to_datetime(start), to_datetime(end)),
                                 latitude=slice(extent.max_y, extent.min_y),
                                 longitude=slice(extent.min_x, extent.max_x))
        return selection['tp']

    def _ensure_cache_covers(self, cache_ds, start, end) -> None:
        time_selection = cache_ds['covered'].loc[start:end].load()
        missing = time_selection.where(time_selection != 1, drop=True).time
        for missing_day in missing.time:
            da = xr.open_dataset(request_grib(pd.to_datetime(missing_day.item())),
                                 backend_kwargs=dict(indexpath=''))['tp']
            da = da.sel(time=missing_day).sum('step')
            update_ds = Dataset({'tp': (('time', 'latitude', 'longitude'), da.values[None, ...].astype(np.float32)),
                                 'covered': (('time',), np.array([1], dtype=np.uint8))})
            t_i = find_index(cache_ds, 'time', missing_day)
            update_ds.to_zarr(self._cache, region={
                'time': slice(t_i, t_i + 1),
                'latitude': slice(0, len(cache_ds.latitude)),
                'longitude': slice(0, len(cache_ds.longitude))
            })


def accumulate_to_full_days(da: DataArray) -> DataArray:
    return da.sum('step')
