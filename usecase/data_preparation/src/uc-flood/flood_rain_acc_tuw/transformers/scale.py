from eotransform_xarray.transformers import TransformerOfDataArray
from xarray import DataArray


class Scale(TransformerOfDataArray):
    def __init__(self, value):
        self._value = value

    def __call__(self, x: DataArray) -> DataArray:
        return x * self._value
