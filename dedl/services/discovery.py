import s3fs
import xarray as xr
import rioxarray
import rasterio as rio
import boto3
from datetime import datetime
import getpass


class S3DataAccess:
    def __init__(self, location_url: str):
        self.location_url = location_url
        self.key = getpass.getpass(prompt="S3 Key: ")
        self.secret = getpass.getpass(prompt="S3 Secret: ")


def find_gfm_flood(date_str, Extent, S3Access):
    s3_central = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return xr.open_zarr(
        store=s3fs.S3Map(
            root=f"dedl-flood/FLOOD/{date_str}.zarr", s3=s3_central, check=False
        ),
        decode_coords="all",
    )["flood"].assign_attrs(location="central-site", resolution=20)


def find_ghs_built(Extent, S3Access):
    s3_central = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return xr.open_zarr(
        store=s3fs.S3Map(root=f"dedl-user/built_data.zarr", s3=s3_central, check=False),
        decode_coords="all",
    )["built"].assign_attrs(location="central-site", resolution=10)


def find_copdem(Extent, S3Access):
    s3_session = boto3.Session(
        aws_access_key_id=S3Access.key,
        aws_secret_access_key=S3Access.secret,
    )
    with rio.Env(
        rio.session.AWSSession(s3_session),
        GDAL_DISABLE_READDIR_ON_OPEN=True,
        AWS_VIRTUAL_HOSTING=False,
        AWS_S3_ENDPOINT=S3Access.location_url,
        CPL_CURL_VERBOSE=True,
        CPL_DEBUG=True,
    ):
        return rioxarray.open_rasterio(
            "s3://dedl-flood/COPDEM/DEM_PAKISTAN.tif",
            mask_and_scale=True,
            chunks="auto",
        ).assign_attrs(location="central-site")


def find_predicted_rain(startdate, enddate, extent, S3Access):
    s3_bridge = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return xr.open_zarr(
        store=s3fs.S3Map(
            root=f"dedl-flood/predicted_rainfall.zarr", s3=s3_bridge, check=False
        )
    )["tp"].assign_attrs(location="bridge")


def find_corine_LC(extent, S3Access):
    s3_user = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return xr.open_zarr(
        store=s3fs.S3Map(
            root=f"dedl-drought/corine_italy.zarr", s3=s3_user, check=False
        )
    ).assign_attrs(location="central-site")


def find_ascat_sm(startdate, enddate, extent, S3Access):
    s3_central = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return (
        xr.open_zarr(
            store=s3fs.S3Map(
                root=f"dedl-drought/ascat_italy.zarr", s3=s3_central, check=False
            ),
            decode_coords="all",
        )["sm"]
        .sel(time=slice(startdate, enddate))
        .assign_attrs(location="central-site", resolution=20)
    )


def find_predicted_sm(startdate, enddate, extent, S3Access):
    s3_bridge = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return (
        xr.open_zarr(
            store=s3fs.S3Map(
                root=f"dedl-drought/modelled_sm.zarr", s3=s3_bridge, check=False
            ),
            decode_coords="all",
        )["swvl1"]
        .sel(time=slice(startdate, enddate))
        .assign_attrs(location="bridge")
    )

def find_4dmed_sm(startdate, enddate, extent, S3Access):
    s3_central = s3fs.S3FileSystem(
        anon=False,
        key=S3Access.key,
        secret=S3Access.secret,
        use_ssl=True,
        client_kwargs={"endpoint_url": S3Access.location_url},
    )
    return (
        xr.open_zarr(
            store=s3fs.S3Map(
                root=f"dedl-user/4dmed_italy.zarr", s3=s3_central, check=False
            ),
        )["SM"]
        .assign_attrs(location="central-site")
    )
