"""
文件说明
"""

from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import re


def get_trading_day(x, day_index):
    trading_days = x
    if day_index > 0 and len(trading_days) >= day_index:
        try:
            return trading_days.index[day_index - 1]
        except:
            return None
    elif day_index < 0 and len(trading_days) > 1:
        try:
            return trading_days.index[day_index]
        except:
            return None
    return None


class RebalanceDatesManager:

    def __init__(self, calendar: pd.DataFrame, start_date: str, end_date: str):
        calendar.tradedays = pd.to_datetime(calendar.tradedays)

        calendar = calendar.query("tradedays>=@start_date and tradedays<=@end_date")
        calendar.index = calendar.tradedays
        self.__calendar = calendar

    @classmethod
    def get_last_day_of_previous_quarters(cls, date_str: str):
        # 获取输入日期的年份和季度
        date = dt.strptime(date_str, "%Y%m%d")
        year = date.year
        quarter = (date.month - 1) // 3 + 1
        # 计算前两个季度的最后一天
        previous_quarters = []
        for i in range(2):
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1
            last_month_of_quarter = quarter * 3
            last_day_of_previous_month = dt(year, last_month_of_quarter, 1) + timedelta(
                days=31
            )
            last_day_of_previous_month = last_day_of_previous_month.replace(
                day=1
            ) - timedelta(days=1)
            last_day_of_previous_month = last_day_of_previous_month.strftime("%Y%m%d")
            previous_quarters.append(last_day_of_previous_month)
        return previous_quarters[-1]

    def parse_rebalance_freq(self, frequency, text, exclude=True):
        freq_enum = ["每周", "每月", "每月最后", "每隔"]
        if frequency not in freq_enum:
            raise ValueError("frequency参数不正确，目前仅支持：", freq_enum)
        # 解析每周交易频率
        match = re.search(r"每周第(\d+)个交易日", text)
        if match:
            # frequency = "每周"
            x = int(match.group(1))
            # print(f"交易频率: {frequency}，数字x: {x}")
            re_dates = (
                self.__calendar.resample("w-fri")
                .apply(get_trading_day, day_index=x)
                .dropna()["tradedays"]
                .values
            )
            # re_dates[:4]
            if exclude:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date) - 1
                    ]
                    for date in re_dates
                ]
            else:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date)
                    ]
                    for date in re_dates
                ]
            re_dates_real = [date.strftime("%Y%m%d") for date in re_dates_real]
            return sorted(re_dates_real)
        # 解析每月交易频率
        match = re.search(r"每月第(\d+)个交易日", text)
        if match:
            # frequency = "每月"
            x = int(match.group(1))
            # print(f"交易频率: {frequency}，数字x: {x}")
            re_dates = (
                self.__calendar.resample("M")
                .apply(get_trading_day, day_index=x)
                .dropna()["tradedays"]
                .values
            )
            if exclude:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date) - 1
                    ]
                    for date in re_dates
                ]
            else:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date)
                    ]
                    for date in re_dates
                ]
            re_dates_real = [date.strftime("%Y%m%d") for date in re_dates_real]
            return sorted(re_dates_real)
        # 解析每月最后交易频率
        match = re.search(r"每月最后第(\d+)个交易日", text)
        if match:
            # frequency = "每月最后"
            x = int(match.group(1))
            # print(f"交易频率: {frequency}，数字x: {x}")
            re_dates = (
                self.__calendar.resample("M")
                .apply(get_trading_day, day_index=-1 * x)
                .dropna()["tradedays"]
                .values
            )
            if exclude:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date) - 1
                    ]
                    for date in re_dates
                ]
            else:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date)
                    ]
                    for date in re_dates
                ]
            re_dates_real = [date.strftime("%Y%m%d") for date in re_dates_real]
            return sorted(re_dates_real)
        # 解析每隔交易频率
        match = re.search(r"每隔(\d+)个交易日", text)
        if match:
            # frequency = "每隔"
            x = int(match.group(1))
            # print(f"交易频率: {frequency}，数字x: {x}")
            period_dates = self.__calendar.tradedays.tolist()
            # x=3
            re_dates = [
                period_dates[i : i + x][0] for i in range(0, len(period_dates), x)
            ]
            if exclude:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date) - 1
                    ]
                    for date in re_dates
                ]
            else:
                re_dates_real = [
                    self.__calendar.index.tolist()[
                        self.__calendar.index.tolist().index(date)
                    ]
                    for date in re_dates
                ]
            re_dates_real = [date.strftime("%Y%m%d") for date in re_dates_real]
            return sorted(re_dates_real)
