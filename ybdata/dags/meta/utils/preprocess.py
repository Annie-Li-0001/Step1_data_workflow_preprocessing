"""
因子预处理函数
"""

import pandas as pd
import numpy as np


def median_winsorized(data, num=3.0):
    """
    中位数去极值
    """
    md = data.median()  # 取中位数
    mad_s = data - md  # 样本数与中位数的差值
    mad_s = mad_s.apply(np.abs)  # 将所有差值变为绝对值
    mad = mad_s.median()  # 取所有绝对值后的差值 的中位数
    std = mad * 1.483  # 1.483个mad等于1个标准差
    max_thold = data.median() + std * num  # 设定上边界为中位数 + 3倍标准差
    min_thold = data.median() - std * num  # 设定下边界为中位数 - 3倍标准差
    data = data.apply(lambda x: x if x <= max_thold else max_thold)
    data = data.apply(lambda x: x if x >= min_thold else min_thold)
    return data


def std_winsorized(data, num=3.0):
    """
    标准差去极值
    """
    max_thold = data.mean() + data.std() * num
    min_thold = data.mean() - data.std() * num
    data = data.apply(lambda x: x if x <= max_thold else max_thold)
    data = data.apply(lambda x: x if x >= min_thold else min_thold)
    return data


def win_and_std(data, cap_weight_data, init_name, final_name):
    """
    MSCI-USE4-201109.pdf中关于Style Factors的描述
    In order to facilitate comparison across style factors, the factors are standardized to have
    a capweighted mean of 0 and an equal-weighted standard deviation of 1. The cap-weighted estimation
    universe, therefore, is style neutral.

    在使用截面回归求解时，必须对风格因子的“因子暴露进行标准化”（国家和行业因子的因子暴露不需要标准化）。
    令 Sn 表示股票 n 的流通市值权重。
    对风格因子的因子暴露进行标准化的初衷是这样的：按照股票的流通市值权重构建的投资组合等同于整个市场，
    而市场对所有的风格因子都应该是中性的。因此，按流通市值权重构建的股票投资组合在所有风格因子上的暴露必须是 0

    此外，我们还必须对风格因子的因子暴露进行“标准差的标准化”，即要求对每一个风格因子 [公式]，[公式] 的标准差为 1。
    这样便完成了对风格因子的因子暴露的标准化。

    :param data:             原始的因子数据
    :param cap_weight_data:  流通市值权重
    :param init_name:        原来的因子名称
    :param final_name:       处理后希望得到的因子名称
    :return:
    """

    data = pd.merge(
        data[["ts_code", init_name]], cap_weight_data, how="outer", on="ts_code"
    )  # weight 是 estu中的

    data["circ_mv_weight"] = data["circ_mv_weight"].fillna(0.0)  # 不在estu中的 权重填0
    data = data.dropna()
    win_name = f"win_{init_name}"

    # 对原始因子值去极值
    data[win_name] = median_winsorized(data[init_name])
    data[win_name] = std_winsorized(data[win_name])

    # 去极值后的因子值需要根据 流通市值做加权求和（就是为做标准归一化）
    cap_weighted_factor = (
        data[win_name] * data["circ_mv_weight"] / data["circ_mv_weight"].sum()
    )  # cap_weighted就是 标准化中的均值mean
    data[final_name] = (
        data[win_name] - cap_weighted_factor.sum()
    )  # 去极值后的因子 - 流通市值加权均值mean  ps: 这里包含了st,次新股，停牌股
    data[final_name] = (
        data[final_name] / data[win_name].std()
    )  # 除以 去极值后的因子的标准差
    del data["circ_mv_weight"]
    return data
