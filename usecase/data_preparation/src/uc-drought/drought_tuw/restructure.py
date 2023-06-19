import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Sequence, Optional

import numpy as np
import pandas as pd
import xarray as xr
from eotransform_pandas.filesystem.gather import gather_files
from xarray import Dataset, DataArray

from drought_tuw.preprocess import clip_dataset_to_shape_file

RESOURCES = Path(__file__).parent.parent.parent / "resources"


def restructure_4dmed_sm(dst: Path):
    src = RESOURCES / "SM/4dmed-grid"
    ds_4dmeds = [xr.open_mfdataset(list(domain.glob('*.nc')))['SM'].load() for domain in src.glob('*')]
    ds_merged = xr.combine_nested(ds_4dmeds, concat_dim='date').sortby('date')
    ds_merged = ds_merged.resample({'date': 'D'}).mean('date')
    ds_merged = ds_merged.transpose('date', 'lat', 'lon')
    ds_merged = ds_merged.rename({'lat': 'latitude', 'lon': 'longitude'})
    ds_merged.rio.write_crs('EPSG:4326', inplace=True)
    ds_merged.rio.set_spatial_dims('longitude', 'latitude', inplace=True)
    ds_merged.to_zarr(dst)


def restructure_ascat_sm(dst: Path):
    src = RESOURCES / "SM/ASCAT-detrended"
    sm_data = []
    lats = []
    lons = []
    max_len = 0
    max_t = None
    for grid in ["1357.nc", "1358.nc", "1359.nc", "1392.nc", "1393.nc", "1394.nc", "1395.nc", "1429.nc", "1430.nc", "1431.nc"]:
        detrended = xr.open_dataset(src / grid)
        loc_info = detrended[['location_id', 'row_size', 'lat', 'lon']]
        loc_info['cum_sum'] = np.cumsum(loc_info['row_size'])
        loc_info['offset'] = loc_info['cum_sum'].shift({'locations': 1}, 0)

        loc_id_area = loc_info.where(loc_info.location_id > 0, drop=True)
        for i in range(loc_id_area.sizes['locations']):
            cl = loc_id_area.isel(locations=i)
            sm = detrended['sm'].isel(obs=slice(int(cl['offset'].values.item()), int(cl['cum_sum'].values.item()))) \
                .set_xindex('time').sel(time=slice(datetime(2022, 1, 1), datetime(2022, 12, 31, 23, 59, 59)))
            sm_data.append((sm, sm.time))
            if sm.sizes['obs'] > max_len:
                max_len = sm.sizes['obs']
                max_t = sm.time
        lats.extend(loc_id_area.lat.values)
        lons.extend(loc_id_area.lon.values)

    max_t = [datetime(t.year, t.month, t.day, t.hour, t.minute) for t in map(pd.to_datetime, max_t.values)]
    sm_ds = Dataset({'sm': DataArray(np.full((len(lats), max_len), np.nan, np.float32),
                                       dims=['location', 'time'],
                                       coords={'lat': ('location', lats),
                                               'lon': ('location', lons),
                                               'time': ('time', max_t)})})

    for i, (sm, time) in enumerate(sm_data):
        ts = sm_ds.sel(time=time, method='nearest').time
        sm_ds['sm'][i].loc[dict(time=ts)] = sm
    sm_ds.to_zarr(dst)


def restructure_cci_lc(dst: Path):
    tiffs = gather_files(RESOURCES / "land-cover/", cci_naming_convention, [
        re.compile('CCI_Land_Cover'),
        re.compile('V1M0R1'),
        re.compile('EQUI7_EU500M'),
        re.compile('E\d\d\dN\d\d\dT6'),
    ])

    cci_ds = load_mosaic(tiffs['filepath'].tolist(), clip_shape=RESOURCES / "borders/4dmed/catch_med.shp", chunks={})
    cci_ds = cci_ds.persist()
    cci_ds = cci_ds.rio.reproject('EPSG:4326')
    cci_ds.rio.write_crs('EPSG:4326', inplace=True)
    cci_ds = cci_ds.rename({'x': 'longitude', 'y': 'latitude', 'band_data': 'cci_lc', 'band': 'lc'})
    cci_ds.rio.set_spatial_dims('longitude', 'latitude', inplace=True)
    cci_ds.to_zarr(dst)


def restructure_corine_lc(dst: Path):
    tiffs = gather_files(RESOURCES / "land-cover/Corine_Land_Cover/2018", corine_naming_convention, [
        re.compile('E\d\d\dN\d\d\dT6'),
    ])

    corine_ds = load_mosaic(tiffs['filepath'].tolist(), clip_shape=RESOURCES / "borders/4dmed/catch_med.shp", chunks={})
    corine_ds = corine_ds.persist()
    corine_ds = corine_ds.rio.reproject('EPSG:4326')
    corine_ds.rio.write_crs('EPSG:4326', inplace=True)
    corine_ds = corine_ds.rename({'x': 'longitude', 'y': 'latitude', 'band_data': 'corine_lc', 'band': 'lc'})
    corine_ds.rio.set_spatial_dims('longitude', 'latitude', inplace=True)
    corine_ds.to_zarr(dst)


def corine_naming_convention(file_name: str) -> Dict:
    try:
        field = {}
        tokens = file_name.split('_')
        tokens[-1] = tokens[-1].split('.')[0]
        field['var_name'] = tokens[0]
        field['grid_name'] = tokens[2]
        field['tile_name'] = tokens[3]
        return field
    except ValueError:
        return {}


def cci_naming_convention(file_name: str) -> Dict:
    try:
        parts = Path(file_name).stem.split('_')
        return {
            'product': parts[0],
            'var_name': parts[1],
            'level': parts[2],
            'type': parts[3] + "-" + parts[4],
            'resolution': parts[5],
            'epoch': parts[6],
            'time': parts[7],
            'version': parts[8],
            'grid_name': parts[9],
            'tile_name': parts[10],
        }
    except ValueError:
        return {}


def load_mosaic(tiffs: Sequence[Path], clip_shape: Optional[Path] = None,
                chunks: Optional = None):
    ds = xr.open_mfdataset(tiffs, engine='rasterio', combine='by_coords', chunks=chunks)
    if clip_shape:
        ds = clip_dataset_to_shape_file(ds, clip_shape)
    return ds


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Select and restructure data for DEDL use case")
    parser.add_argument('dataset', type=str, help='Dataset to restructure ("4dmed", "ascat", "corine", or "cci")')
    parser.add_argument('out_zarr', type=Path, help='zarr archive to store restructured data in')
    args = parser.parse_args()

    if args.dataset == '4dmed':
        restructure_4dmed_sm(args.out_zarr)
    if args.dataset == 'ascat':
        restructure_ascat_sm(args.out_zarr)
    if args.dataset == 'cci':
        restructure_cci_lc(args.out_zarr)
    if args.dataset == 'corine':
        restructure_corine_lc(args.out_zarr)
