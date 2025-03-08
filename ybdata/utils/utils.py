import pandas as pd
import numpy as np


def median_winsorized(data, num=3.0):
    md = data.median()
    mad_s = data - md
    mad_s = mad_s.apply(np.abs)
    mad = mad_s.median()
    std = mad * 1.483  # 1.483个mad等于1个标准差
    max_thold = data.median() + std * num
    min_thold = data.median() - std * num
    data = data.apply(lambda x: x if x <= max_thold else max_thold)
    data = data.apply(lambda x: x if x >= min_thold else min_thold)
    return data
