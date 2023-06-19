from pathlib import Path
from typing import Tuple, List

import matplotlib
import numpy as np
import pandas as pd
import panel as pn
import param as pm
from bokeh.models import FuncTickFormatter, FixedTicker, WheelZoomTool
from holoviews import Image, DynamicMap, extension
from matplotlib.colors import LinearSegmentedColormap
from xarray import DataArray

extension('bokeh')

# prevent panning on axis
wheel_zoom = WheelZoomTool(zoom_on_axis=False)
zoom = dict(default_tools=["pan", wheel_zoom])


def load_cmap(file: Path) -> LinearSegmentedColormap:
    def to_hex_str(c_str: str) -> str:
        r_s, g_s, b_s = c_str.split()
        return f"#{int(r_s):02x}{int(g_s):02x}{int(b_s):02x}"

    ct_lines = Path(file).read_text().splitlines()
    brn_yl_bu_colors = [to_hex_str(clr_str) for clr_str in ct_lines[:200]]
    return matplotlib.colors.LinearSegmentedColormap.from_list("", brn_yl_bu_colors)


def render_s1_sm(sm_da: DataArray, **kwargs) -> pn.Column:
    sm_ct = load_cmap(Path(__file__).parent.parent.parent / "resources/colour-tables/ssm-continuous.ct")
    color_opts = dict(
        cmap=sm_ct,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )

    class S1SMExplorer(pm.Parameterized):
        month = pm.Selector([pd.to_datetime(t).date() for t in sm_da.date.values])

        @pm.depends('month')
        def sm(self):
            r = sm_da.sel({'date': self.month}, method='nearest').rename('SM')
            return Image(r).redim.range(SM=(0, 1)).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")

        def map(self):
            return DynamicMap(self.sm)

    explorer = S1SMExplorer(name='')
    time_slider = pn.widgets.DiscreteSlider.from_param(explorer.param.month, width=kwargs.get('width', 512))
    return pn.Column(explorer.map(), time_slider)


def render_ascat_sm(sm_da: DataArray, **kwargs) -> pn.Column:
    sm_ct = load_cmap(Path(__file__).parent.parent.parent / "resources/colour-tables/ssm-continuous.ct")
    color_opts = dict(
        cmap=sm_ct,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )

    class ASCATSMExplorer(pm.Parameterized):
        month = pm.Selector([pd.to_datetime(t).date() for t in sm_da.time.values])

        @pm.depends('month')
        def sm(self):
            r = sm_da.sel({'time': self.month}, method='nearest').rename('SM')
            return Image(r).redim.range(SM=(0, 1)).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")

        def map(self):
            return DynamicMap(self.sm)

    explorer = ASCATSMExplorer(name='')
    time_slider = pn.widgets.DiscreteSlider.from_param(explorer.param.month, width=kwargs.get('width', 512))
    return pn.Column(explorer.map(), time_slider)


def render_s1_sm_static(sm_da: DataArray, **kwargs) -> Image:
    sm_ct = load_cmap(Path(__file__).parent.parent.parent / "resources/colour-tables/ssm-continuous.ct")
    color_opts = dict(
        cmap=sm_ct,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )
    r = sm_da.sel({'date': pd.to_datetime(sm_da.date.values[0]).date()}, method='nearest').rename('SM')
    return Image(r).redim.range(SM=(0, 1)).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")


def render_ascat_sm_static(sm_da: DataArray, **kwargs) -> Image:
    sm_ct = load_cmap(Path(__file__).parent.parent.parent / "resources/colour-tables/ssm-continuous.ct")
    color_opts = dict(
        cmap=sm_ct,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )
    r = sm_da.sel({'time': pd.to_datetime(sm_da.time.values[0]).date()}, method='nearest').rename('SM')
    return Image(r).redim.range(SM=(0, 1)).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")


def render_era5l_swvl(sm_da: DataArray, **kwargs) -> pn.Column:
    sm_ct = load_cmap(Path(__file__).parent.parent.parent / "resources/colour-tables/ssm-continuous.ct")
    color_opts = dict(
        cmap=sm_ct,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)', 'min': 'rgba(0, 0, 0, 0)'},
        colorbar=True
    )

    class ERA5LSwvl1Explorer(pm.Parameterized):
        month = pm.Selector([pd.to_datetime(t).date() for t in sm_da.time.values])

        @pm.depends('month')
        def sm(self):
            r = sm_da.sel({'time': self.month}, method='nearest').rename('SM')
            return Image(r).redim.range(SM=(0, 1)).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")

        def map(self):
            return DynamicMap(self.sm)

    explorer = ERA5LSwvl1Explorer(name='')
    time_slider = pn.widgets.DiscreteSlider.from_param(explorer.param.month, width=kwargs.get('width', 512))
    return pn.Column(explorer.map(), time_slider)


def render_corine(lc_da: DataArray, **kwargs):
    levels = list(range(1, 6))
    colors = ["#e6004d", "#ffff00", "#4dff00", "#a6a6ff", "#80f2e6"]

    # only major classes
    lc_da_copy = lc_da.copy()
    lc_da_copy.corine_lc.data = np.floor(lc_da_copy.corine_lc.data / 100)

    formatter = FuncTickFormatter(code='''
        return {1.0 : 'Artificial surfaces', 2.0 : 'Agricultural areas', 3.0 : 'Forest and seminatural areas', 4.0 : 'Wetlands', 5.0 : 'Water bodies'}    
        [tick]'''
                                  )
    ticker = FixedTicker(ticks=levels)

    color_opts = dict(
        cmap=colors,
        colorbar=True,
        clipping_colors={'NaN': 'rgba(0, 0, 0, 0)'},
        colorbar_opts={'ticker': ticker, 'formatter': formatter}
    )
    return Image(lc_da_copy['corine_lc'][0]).opts(**color_opts, **kwargs, **zoom, xlabel="longitude", ylabel="latitude")


def _load_corine_legend(legend_file: Path) -> List[Tuple[int, str, str]]:
    def parse_line(line: str) -> Tuple[int, str, str]:
        tokens = [t for t in map(str.strip, line.split('"')) if t != '']
        return int(tokens[0]), tokens[2], tokens[1]

    return [parse_line(l) for l in (legend_file.read_text().splitlines())]
