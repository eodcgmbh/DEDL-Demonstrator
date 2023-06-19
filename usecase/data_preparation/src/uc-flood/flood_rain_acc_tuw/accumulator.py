from typing import List

import xarray as xr
from eotransform_xarray.transformers.send_to_stream import StreamIn
from rasterio.enums import Resampling
from xarray import DataArray

M_TO_MM = 1000


class AccumulatorStreamIter(StreamIn):
    def __init__(self):
        self._cum_sum_da: List[DataArray] = []

    def send(self, new_obs: DataArray) -> None:
        new_obs_mm = new_obs * M_TO_MM
        if len(self._cum_sum_da) == 0:
            self._cum_sum_da.append(new_obs_mm)
        else:
            acc_val = new_obs_mm.copy(deep=True, data=new_obs_mm.values + self._cum_sum_da[-1].values)
            self._cum_sum_da.append(acc_val)

    def close(self) -> DataArray:
        da = xr.concat(self._cum_sum_da, dim='time')
        da.rio.write_nodata(0, inplace=True)
        da.rio.write_crs('EPSG:4326', inplace=True)
        return da.rio.reproject('EPSG:3857', resolution=500, resampling=Resampling.bilinear)
