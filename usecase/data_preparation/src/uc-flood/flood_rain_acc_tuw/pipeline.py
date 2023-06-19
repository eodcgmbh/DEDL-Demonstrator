import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Iterable

import pandas as pd
from eotransform.protocol.transformer import Transformer
from eotransform.sinks.sink_to_progress_report import SinkToProgressReport
from eotransform.streamed_process import streamed_process
from eotransform.transformers.compose import Compose
from eotransform_xarray.transformers.send_to_stream import SendToStream
from eotransform_xarray.transformers.squeeze import Squeeze
from eotransform_xarray.transformers.to_dataset import ToDataset
from tqdm import tqdm
from xarray import DataArray

from dedl.services.schedule import DistributedScheduler
from flood_rain_acc_tuw.accumulator import AccumulatorStreamIter


def accum_rain_predictions(predicted_rain: DataArray, scheduler: DistributedScheduler):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        print(f"Connecting to {scheduler.host}...")
        acc_process, acc_stream = make_rain_accumulation_process()
        print("Succeeded!")
        with ThreadPoolExecutor(max_workers=3) as executor, \
                tqdm(desc=f"processing at {scheduler.worker}", total=len(predicted_rain)) as reporter:
            streamed_process(per_time_step(predicted_rain), acc_process, SinkToProgressReport(reporter), executor)
        return acc_stream.close().rio.write_crs('EPSG:4326')


def make_rain_accumulation_process() \
        -> Tuple[Transformer, AccumulatorStreamIter]:
    accumulator_stream = AccumulatorStreamIter()
    return Compose([
        ToDataset('rain'),
        SendToStream(accumulator_stream, 'rain')
    ]), accumulator_stream


def per_time_step(da: DataArray) -> Iterable[DataArray]:
    for t in da.time:
        yield da.sel(time=[pd.to_datetime(t.item())]).load()
