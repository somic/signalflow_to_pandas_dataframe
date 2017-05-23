#!/usr/bin/env python3

import signalfx
import time
import pandas as pd
from datetime import datetime

def signalflow_to_pandas_dataframe(flow, program, start, stop, resolution=None):
    data = {}
    metadata = {}
    index = []

    try:
        exe = flow.execute(program, start, stop, resolution=resolution)
        for msg in exe.stream():
            if isinstance(msg, signalfx.signalflow.messages.MetadataMessage):
                metadata[msg.tsid] = msg
            elif isinstance(msg, signalfx.signalflow.messages.DataMessage):
                index.append(datetime.fromtimestamp(int(msg.logical_timestamp_ms/1000)))
            for tsid, val in msg.data.items():
                if tsid not in data: data[tsid] = []
                data[tsid].append(val)
    finally:
        flow.close()

    df = pd.DataFrame(data=data, index=index)
    df.metadata = metadata
    return df
