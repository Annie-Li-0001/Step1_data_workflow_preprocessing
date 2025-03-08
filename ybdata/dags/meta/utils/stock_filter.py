import pandas as pd
import numpy as np
from meta.api.data import data_api_obj
from datetime import datetime as dt
from typing import Union, List


def get_stock_history_name(code_list, update=False):
    """
    获取股票的历史曾用名
    """
    history_name = data_api_obj.get_history_name()
    history_name = history_name.assign(
        temp=np.where(history_name["end_date"].isna(), 0, 1)
    )
    history_name = history_name[history_name.name.str.contains("ST")]
    history_name.sort_values(
        ["ts_code", "start_date", "temp"], ascending=False, inplace=True
    )
    history_name = history_name.drop_duplicates(subset=["ts_code", "start_date"])
    history_name.sort_values(["ts_code", "start_date"])
    # if update:
    #     import tushare as ts
    #     import time
    #     # pro = ts.pro_api("c69dbd3f4b9")
    #     # history_name = pd.DataFrame()
    #     history_name = data_api_obj.get_history_name()
    #     # for code in code_list:
    #     #     tmp_name = pro.namechange(ts_code=code, fields='ts_code,name,start_date,end_date,change_reason')
    #     #     time.sleep(0.8)
    #     #     history_name = pd.concat([history_name, tmp_name], axis=0)
    #     # history_name.to_csv("历史曾用名.csv", index=False)
    # else:
    #     history_name = pd.read_csv("历史曾用名.csv")
    #
    #     history_name = history_name.assign(temp=np.where(history_name['end_date'].isna(), 0, 1))
    #     history_name = history_name[history_name.name.str.contains('ST')]
    #     history_name.sort_values(['ts_code', 'start_date', 'temp'], ascending=False, inplace=True)
    #     history_name = history_name.drop_duplicates(subset=['ts_code', 'start_date'])
    #     history_name.sort_values(['ts_code', 'start_date'])

    return history_name


def advanced_st_treat(data: pd.DataFrame, name_df: pd.DataFrame) -> pd.DataFrame:
    """
    判断股票st的情况
    """
    # 不是st的部分
    not_st_df = data[~data["ts_code"].isin(name_df.ts_code.unique())]
    not_st_df["is_st"] = 0

    # 可能是st的部分
    possibol_st_df = data[data["ts_code"].isin(name_df.ts_code.unique())]
    st_df = pd.merge(name_df, possibol_st_df, left_on="ts_code", right_on="ts_code")

    st_df = st_df.assign(
        tmp2=np.where(
            (st_df.trade_date >= st_df.start_date)
            & (st_df.trade_date <= st_df.end_date),
            1,
            0,
        )
    )  # 用assign方法来新增一列
    st_df = st_df.groupby("ts_code")["tmp2"].sum().to_frame("is_st")

    possibol_st_df = pd.merge(
        possibol_st_df, st_df, left_on="ts_code", right_index=True
    )
    return pd.concat([not_st_df, possibol_st_df])


def add_st_label(frame: pd.DataFrame) -> pd.DataFrame:
    """
    frame可以是任何类型的数据，比如股票基本信息、行情等，只要保证有股票代码ts_code、交易日期trade_date两列即可
    本代码会在原始frame基础上在最后一列增加is_st的列
    """
    frame = frame.copy()
    code_list = frame["ts_code"].drop_duplicates().to_list()
    # 注意这里的update参数，本地没有文件要改为True从tushare获取
    name_data = get_stock_history_name(code_list, update=False)

    # 填充一下，有些股票可能正处于ST，没有end_date;有些股票可能在样本开始区间就是ST
    name_data["start_date"] = name_data["start_date"].fillna("19910101")
    name_data["end_date"] = name_data["end_date"].fillna("21000101")
    name_data["start_date"] = name_data["start_date"].apply(lambda x: str(int(x)))
    name_data["end_date"] = name_data["end_date"].apply(lambda x: str(int(x)))
    name_data["start_date"] = pd.to_datetime(name_data["start_date"])
    name_data["end_date"] = pd.to_datetime(name_data["end_date"])

    # 核心点
    res = advanced_st_treat(frame, name_data)
    return res


def check_is_new_stock(date: str, codes: Union[List, str] = None, periods: int = 60):
    """
    用来判断某一个横截面（某一日）内的全市场股票哪些是次新股
    :param date: 日期
    :param codes: 股票list或者str，如果不输入，则默认取全市场进行判断
    :param pariods: 期间，即在date前的多少时间段内上市的认为是新股，默认60个交易日
    """
    stock_info = data_api_obj.get_stock_basic()
    if codes:
        codes = [codes] if isinstance(codes, str) else codes
        stock_info = stock_info[stock_info.ts_code.isin(codes)]

    stock_info["list_date"] = pd.to_datetime(stock_info["list_date"])
    stock_info["delist_date"] = stock_info["delist_date"].fillna("99999999")

    stock_info["list_gap"] = (
        dt.strptime(date, "%Y%m%d") - stock_info["list_date"]
    ).apply(lambda x: x.days)
    stock_info["is_new"] = (stock_info["list_gap"] <= periods) & (
        stock_info["delist_date"] > date
    )
    stock_info["trade_date"] = date
    return stock_info[["trade_date", "ts_code", "is_new"]]


def get_estu(date):
    """
    回归时，回归样本空间
    拥有2个字段：code, in_estu

    ESTU(Estimation Universe)：用作对多因子模型中的因子收益率和特异性收益率进行估计的股票池，
    通常设为：剔除不能正常交易、无行业分类、一年内发生特别处理或者上市时间不足 30 天的当前上市
    的所有 A 股
    """
    # 剔除次新股
    stock_info = check_is_new_stock(date, periods=250)
    stock_info = stock_info[~stock_info.is_new]  # 非
    # 剔除长期停牌，过去252个交易日中超过151个交易日停牌
    long_time_stop_stock_df = data_api_obj.get_longtime_stop(date, 252)
    long_time_stop_stock_df.sort_values("suspend_count", ascending=False, inplace=True)
    long_time_stop_stock_df = long_time_stop_stock_df[
        long_time_stop_stock_df["suspend_count"] >= 151
    ]

    code_list = set(stock_info.ts_code.tolist()) - set(
        long_time_stop_stock_df.index.tolist()
    )

    combine_data = stock_info[stock_info.ts_code.isin(code_list)]
    combine_data["trade_date"] = pd.to_datetime(date)
    # 剔除ST
    res = add_st_label(frame=combine_data)
    res = res[res.is_st == 0]
    return res
