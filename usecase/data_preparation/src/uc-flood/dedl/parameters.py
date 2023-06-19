from typing import Tuple

import pytileproj.geometry as tile_geom

from affine import Affine
from attr import dataclass
from geospade.crs import SpatialRef
from geospade.tools import any_geom2ogr_geom
from osgeo import osr
from osgeo.ogr import Geometry
from pyproj import Transformer


@dataclass
class Extent:
    min_x: float
    min_y: float
    max_x: float
    max_y: float
    crs: str

    @classmethod
    def from_arg_str(cls, arg: str) -> "Extent":
        tokens = [t.strip() for t in arg.split(',')]
        crs = None if len(tokens) == 4 else tokens[-1]
        return cls(*tuple(map(float, tokens[:4])) + (crs,))


    def to_tuple(self) -> Tuple[float, float, float, float]:
        return self.min_x, self.min_y, self.max_x, self.max_y

    def to_ogr(self) -> Geometry:
        return any_geom2ogr_geom((self.min_y, self.min_x, self.max_y, self.max_x),
                                 sref=SpatialRef(int(self.crs.split(':')[1])))

    def get_transform(self, sampling: int) -> Affine:
        return Affine.from_gdal(self.min_x, sampling, 0, self.max_y, 0, -sampling)

    def transform_to(self, crs) -> "Extent":
        transformer = Transformer.from_crs(self.crs, crs)
        spref = osr.SpatialReference()
        spref.ImportFromProj4(crs)
        poly = tile_geom.bbox2polygon(((self.min_x, self.min_y), (self.max_x, self.max_y)), spref, segment=25000)
        bbox = tile_geom.get_geometry_envelope(poly, rounding=0.000001)
        min_x, min_y = transformer.transform(bbox[0], bbox[1])
        max_x, max_y = transformer.transform(bbox[2], bbox[3])
        return Extent(min_x, min_y, max_x, max_y, crs)