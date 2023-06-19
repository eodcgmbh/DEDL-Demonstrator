import argparse
import rioxarray # noqa
from datetime import date, datetime
from pathlib import Path

import cdsapi
import dask.array
import numpy as np
import pandas as pd
import xarray as xr
from xarray import Dataset


def request_grib(day: date) -> Path:
    c = cdsapi.Client()

    out_file = f'/tmp/swvl1_era5land_{day.year}{day.month}{day.day}.grib'
    if Path(out_file).exists():
        return out_file
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

            'variable': 'swvl1',
        },
        out_file)

    return Path(out_file)


def create_empty_swvl_zarr_store(path: Path) -> None:
    lats = np.arange(90, -90.1, -0.1)
    lons = np.arange(-180, 180, 0.1)
    days = pd.date_range(date(2015, 1, 1), date(2024, 1, 1), freq='D')
    dummies_f32 = dask.array.empty(shape=(len(days), len(lats), len(lons)), chunks=(100, 100, 100), dtype=np.float32)
    dummies_bool = dask.array.zeros(shape=(len(days),), chunks=(100,), dtype=np.uint8)
    ds = Dataset({
        'swvl1': (('time', 'latitude', 'longitude'), dummies_f32),
        'covered': (('time',), dummies_bool),
    }, coords={'time': ('time', days), 'latitude': ('latitude', lats), 'longitude': ('longitude', lons)})
    ds['covered'].rio.write_nodata(0, encoded=True, inplace=True)
    ds.rio.set_spatial_dims('longitude', 'latitude', inplace=True)
    ds.rio.write_crs('EPSG:4326', inplace=True)
    ds.to_zarr(path, compute=False)
    ds.drop_vars(('swvl1', 'covered', 'time')).to_zarr(path, mode='a')


def retrieve_swvl1(zarr: Path, start: date, end: date) -> None:
    if not zarr.exists():
        create_empty_swvl_zarr_store(zarr)
    cache_ds = xr.open_zarr(zarr)
    update_cache_covers(cache_ds, zarr, start, end)


def update_cache_covers(cache_ds, zarr, start, end) -> None:
    time_selection = cache_ds['covered'].loc[start:end].load()
    missing = time_selection.where(time_selection != 1, drop=True).time
    for missing_day in missing.time:
        da = xr.open_dataset(request_grib(pd.to_datetime(missing_day.item())),
                             backend_kwargs=dict(indexpath=''))['swvl1']
        da = da.mean('time')
        da.rio.set_spatial_dims('longitude', 'latitude', inplace=True)
        da.rio.write_crs('EPSG:4326', inplace=True)
        da = da.rio.reproject('EPSG:4326')
        update_ds = Dataset({'swvl1': (('time', 'latitude', 'longitude'), da.values[None, ...].astype(np.float32)),
                             'covered': (('time',), np.array([1], dtype=np.uint8))})
        t_i = find_index(cache_ds, 'time', missing_day)
        update_ds.to_zarr(zarr, region={
            'time': slice(t_i, t_i + 1),
            'latitude': slice(0, len(cache_ds.latitude)),
            'longitude': slice(0, len(cache_ds.longitude))
        })


def to_datetime(start):
    return datetime(start.year, start.month, start.day)


def find_index(ds, coord, value):
    return (ds[coord] == ds[coord].sel({coord: value}, method='nearest')).argmax().item()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve ERA5-Land swvl1 data for DEDL use case")
    parser.add_argument('out_zarr', type=Path, help='zarr archive to store restructured data in')
    parser.add_argument('start', type=str, help='Start date i.e. 2022-03-01')
    parser.add_argument('end', type=str, help='End date i.e. 2022-08-31')
    args = parser.parse_args()
    retrieve_swvl1(args.out_zarr, date.fromisoformat(args.start), date.fromisoformat(args.end))
