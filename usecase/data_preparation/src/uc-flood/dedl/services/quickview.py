import warnings

import matplotlib.colors as clr
import numpy as np
import pandas as pd
import panel as pn
import param as pm
import xarray as xr
from bokeh.models import FuncTickFormatter, FixedTicker
from holoviews import Image, element, Curve, extension, DynamicMap
from rasterio.enums import Resampling
from xarray import DataArray

from dedl.services.schedule import DistributedScheduler

extension('bokeh')

TARGET_RESOLUTION_IN_M = 500


def preprocess_dataset(data_array: DataArray, method: str, scheduler: DistributedScheduler):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        print(f"Connecting to {scheduler.host}...")
        data_array = data_array.squeeze()
        src_res = get_resolution_from_data_array(data_array)
        steps = TARGET_RESOLUTION_IN_M // src_res
        coarsened = data_array.coarsen({'y': steps, 'x': steps}, boundary='trim')
        print(f"Succeeded!")
        print(f"processing at {scheduler.worker}")
        if method == 'median':
            data_array = (coarsened.median().load(scheduler='processes') > 0).astype('float32')
        elif method == 'mean':
            data_array = coarsened.mean().load(scheduler='processes')
        else:
            raise NotImplementedError(method)

        return data_array.rio.reproject(f"EPSG:3857", nodata=np.nan)


def get_resolution_from_data_array(da):
    if da.name == 'flood':
        src_res = 20
    elif da.name == 'built':
        src_res = 10
    else:
        raise NotImplementedError(f"DataArray {da.name} unknown.")
    return src_res


def render_flood_and_built_extent(flood: DataArray, built: DataArray, colorbar=True, **kwargs) -> None:
    built = built.rio.reproject_match(flood)
    combined = xr.zeros_like(flood, dtype=flood.dtype)
    combined.values = np.nanmax(np.stack([flood.values, ((built > 10) * 2).values]), axis=0)

    formatter = FuncTickFormatter(code='''
    return {0.33: 'other', 1.0 : 'flooded', 1.66 : 'built'}
    [tick]
    ''')
    ticker = FixedTicker(ticks=[0.33, 1.0, 1.66])

    color_opts = dict(
        cmap=["#D3D3D3", "rgb(0, 0, 128)", "#FF0000"],
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)'},
        colorbar=colorbar,
        colorbar_opts={'ticker': ticker, 'formatter': formatter}
    )

    flood_view = Image(combined).opts(**color_opts, **kwargs, xlabel="longitude", ylabel="latitude")
    street_view = element.tiles.StamenTonerRetina()
    return flood_view * street_view.opts(alpha=0.3) 

def render_streetview(flood_view, **kwargs):
    street_view = element.tiles.CartoLight()
    return flood_view.opts(alpha=0, colorbar=False, **kwargs) * street_view


def render_rain_prediction(rain_da: DataArray, dem: DataArray, **kwargs):
    color_opts = dict(
        cmap=clr.LinearSegmentedColormap.from_list('custom blue', ['#D3D3D3', '#000080'], N=256),
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )

    background = dem.copy(deep=True)
    background = background.rio.reproject('EPSG:3857')
    ls = clr.LightSource(azdeg=315, altdeg=45)
    background.values[0] = ls.hillshade(background.values[0], vert_exag=0.001)
    bg_view = Image(background[0]).opts(colorbar=False, cmap='gray', clipping_colors={'NaN': '#999999'})

    max_rain = rain_da.max().values.item()

    class RainExplorer(pm.Parameterized):
        time = pm.Selector([pd.to_datetime(t).date() for t in rain_da.time.values])

        @pm.depends('time')
        def rain(self):
            r = rain_da.sel({'time': self.time}, method='nearest').rename('rain')
            return Image(r).redim.range(rain=(0, max_rain))\
                .opts(**color_opts, **kwargs, xlabel="longitude", ylabel="latitude")

        def map(self):
            street_view = element.tiles.StamenTonerRetina()
            return bg_view * street_view.opts(alpha=0.3) * DynamicMap(self.rain).opts(alpha=0.6)

    explorer = RainExplorer(name='')
    time_slider = pn.widgets.DiscreteSlider.from_param(explorer.param.time, width=kwargs.get('width', 512))
    return pn.Column(explorer.map(), time_slider)

def render_flood_extent(dataset, **kwargs):
    # some decorations
    formatter = FuncTickFormatter(code='''
    return {0.25: 'non-flooded', 0.75 : 'flooded'}
    [tick]
    ''')
    ticker = FixedTicker(ticks=[0.25, 0.75])

    color_opts = dict(
        cmap=["#D3D3D3", "rgb(0, 0, 128)"],
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)'},
        colorbar=True,
        colorbar_opts={'ticker': ticker, 'formatter': formatter}
    )

    flood_view = Image(dataset).opts(**color_opts, **kwargs, xlabel="longitude", ylabel="latitude")
    street_view = element.tiles.StamenTonerRetina()

    return flood_view * street_view.opts(alpha=0.3)


def render_built_extent(dataset, **kwargs):
    dataset = dataset.rename('build')
    color_opts = dict(
        cmap="Viridis",
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )

    build_view = Image(dataset).opts(**color_opts, **kwargs, xlabel="longitude", ylabel="latitude")
    street_view = element.tiles.StamenTonerRetina()

    return build_view.redim.range(build=(0, 100)) * street_view.opts(alpha=0.3)

