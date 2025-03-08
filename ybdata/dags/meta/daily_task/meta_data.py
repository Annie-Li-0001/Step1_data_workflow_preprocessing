"""
文件说明
"""

import time

# from meta.base import API
# from ..config import Config
# from ..db_base.mysql_db import MySQLDB
from . import Freq
import numpy as np
from . import API
from . import MySQLDB
from . import Config
import os
from meta.api.data import data_api_obj
import traceback
import pandas as pd
from typing import Callable
from pymongo import MongoClient

db = MySQLDB(db_url=Config.MYSQL_ENGINE_STR)
api = API()
mongo_client = MongoClient(Config.MONGO_ENGINE_STR)


#  数据同步元类
class TaskMeta(type):
    __data_task_stores__ = []
    __factor_task_stores__ = []

    def __new__(cls, name: str, bases, attrs):

        if name.startswith("PrefixData") or name.startswith("PrefixFactor"):
            # 看视频的同学，这里和课程视频中是不一致的，因为视频录完后，这里的代码进行了更新，增加name.startswith('PrefixFactor')
            # 表示的是因子运行的任务类，不影响大体框架
            if (
                "table" not in attrs
                or "scheme" not in attrs
                or "description" not in attrs
                or "parents" not in attrs
                or "freq" not in attrs
            ):
                raise ValueError(
                    f"{name}没有提供 table、scheme、description、parents信息"
                )
            cls.__data_task_stores__.append(type.__new__(cls, name, bases, attrs))
        else:
            attrs["__data_task_stores__"] = cls.__data_task_stores__
            return type.__new__(cls, name, bases, attrs)


class Indicator:
    def create_indicator(self, raw_data_dir, raw_data_field, indicator_name):
        """主要用于通过日频数据创建日频指标"""
        tmp_dir = os.path.join(self.root, raw_data_dir)
        tdays = [pd.to_datetime(f.split(".")[0]) for f in os.listdir(tmp_dir)]
        tdays = sorted(tdays)
        all_stocks_info = self.meta
        df = pd.DataFrame(index=all_stocks_info.index, columns=tdays)
        for f in os.listdir(tmp_dir):
            tday = pd.to_datetime(f.split(".")[0])
            dat = pd.read_csv(
                os.path.join(tmp_dir, f),
                index_col=["ts_code"],
                engine="python",
                encoding="gbk",
            )
            df[tday] = dat[raw_data_field]
            print(tday)
        df = df.dropna(how="all")  # 删掉全为空的一行
        diff = df.index.difference(
            all_stocks_info.index
        )  # 删除没在股票基础列表中多余的股票行
        df = df.drop(labels=diff)
        self.close_file(df, indicator_name)


class DataQuery:
    def fetch_data_by_day(
        self,
        col_name,
        func,
        save_table,
        unique_keys: list = None,
        filter_key: str = "trade_date",
    ):
        calendar_date_col = "tradedays"
        trade_date_sql = f"""
        select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
        and {calendar_date_col}<={Config.TODAY}
        """
        trade_dates = db.read_sql(trade_date_sql)
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        dates_to_download = list(
            set(trade_dates[calendar_date_col].tolist()).difference(set(dates_in_db))
        )
        dates_to_download.sort()
        for i, date in enumerate(dates_to_download):
            try:
                df = func(**{filter_key: date})
                if df.empty:
                    print("数据为空", date)
                    continue
                if "update_flag" in df.columns:
                    df = df.loc[df["update_flag"] == "1"]
                if unique_keys:
                    df.drop_duplicates(subset=unique_keys, keep="first", inplace=True)
                db.insert_dataframe(save_table, df)
            except:
                traceback.print_exc()
                print(f"{date}报错")
        print("任务完成")

    def fetch_data_by_quarter(
        self,
        col_name,
        func,
        save_table,
        unique_keys: list = None,
        fields: str = None,
        report_type: list = None,
    ):
        qdates = pd.date_range(start=Config.START_DATE, end=Config.TODAY, freq="Q")
        qdates = [date.strftime("%Y%m%d") for date in qdates]
        qdates.sort()
        sql_delete = f"delete from {self.table} where {col_name} in ('{qdates[-1]}','{qdates[-2]}','{qdates[-3]}')"
        db._execute(sql_delete)
        # trade_dates = db.read_sql(f'select end_date from {self.parents[0]} where tradedays<={Config.START_DATE}')
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        print("理论应该有的数据日期:", qdates)
        print("数据库中的数据日期:", dates_in_db)
        dates_to_download = list(set(qdates).difference(set(dates_in_db)))
        dates_to_download.sort()
        print("需要补充的日期:", dates_to_download)
        for i, date in enumerate(dates_to_download):
            try:
                # df = pd.DataFrame()
                if fields:
                    if report_type:
                        # df01 = pd.DataFrame()
                        # df02 = pd.DataFrame()

                        for item in report_type:
                            print(f">>> 正在下载 date:{date}, report_type: {item}...")
                            df01 = func(
                                period=date,
                                offset=0,
                                limit=6400,
                                fields=fields,
                                report_type=item,
                            )
                            df02 = func(
                                period=date,
                                offset=6400,
                                limit=6400,
                                fields=fields,
                                report_type=item,
                            )
                            sub_df = pd.concat([df01, df02])
                            # print('------------------------sub df---------------------------')
                            # print(sub_df[['ts_code', 'end_date', 'report_type']])
                            if unique_keys:
                                # print('准备去重：', unique_keys)
                                sub_df.sort_values(
                                    unique_keys + ["update_flag"], inplace=True
                                )
                                sub_df.drop_duplicates(
                                    subset=unique_keys, keep="last", inplace=True
                                )
                                # df02.sort_values(unique_keys + ['update_flag'], inplace=True)
                                # df02.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                            # df = pd.concat([df, sub_df])
                            db.insert_dataframe(save_table, sub_df)
                            time.sleep(1.5)
                    else:
                        df01 = func(period=date, offset=0, limit=6400, fields=fields)
                        df02 = func(period=date, offset=6400, limit=6400, fields=fields)
                        sub_df = pd.concat([df01, df02])

                        if unique_keys:
                            sub_df.sort_values(
                                unique_keys + ["update_flag"], inplace=True
                            )
                            sub_df.drop_duplicates(
                                subset=unique_keys, keep="last", inplace=True
                            )
                            # df02.sort_values(unique_keys + ['update_flag'], inplace=True)
                            # df02.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                        # df = pd.concat([df, sub_df])
                        db.insert_dataframe(save_table, sub_df)
                        time.sleep(1.5)
                else:
                    if report_type:
                        # df01 = pd.DataFrame()
                        # df02 = pd.DataFrame()

                        for item in report_type:
                            print(f">>> 正在下载 date:{date}, report_type: {item}...")
                            df01 = func(
                                period=date, offset=0, limit=6400, report_type=item
                            )
                            df02 = func(
                                period=date, offset=6400, limit=6400, report_type=item
                            )
                            sub_df = pd.concat([df01, df02])
                            if unique_keys:
                                sub_df.sort_values(
                                    unique_keys + ["update_flag"], inplace=True
                                )
                                sub_df.drop_duplicates(
                                    subset=unique_keys, keep="last", inplace=True
                                )
                            # df = pd.concat([df, sub_df])
                            db.insert_dataframe(save_table, sub_df)
                            time.sleep(1.5)
                            # df01 = pd.concat(
                            #     [df01, func(period=date, offset=0, limit=6400, report_type=item)])
                            # df02 = pd.concat(
                            #     [df02, func(period=date, offset=6400, limit=6400, report_type=item)])
                            # if unique_keys:
                            #     df01.sort_values(unique_keys + ['update_flag'], inplace=True)
                            #     df01.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                            #     df02.sort_values(unique_keys + ['update_flag'], inplace=True)
                            #     df02.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                            # time.sleep(1.5)
                    else:
                        df01 = func(period=date, offset=0, limit=6400)
                        df02 = func(period=date, offset=6400, limit=6400)
                        sub_df = pd.concat([df01, df02])

                        if unique_keys:
                            sub_df.sort_values(
                                unique_keys + ["update_flag"], inplace=True
                            )
                            sub_df.drop_duplicates(
                                subset=unique_keys, keep="last", inplace=True
                            )
                            # df02.sort_values(unique_keys + ['update_flag'], inplace=True)
                            # df02.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                        # df = pd.concat([df, sub_df])
                        db.insert_dataframe(save_table, sub_df)
                        time.sleep(1.5)
                # df = pd.concat([df01, df02])
                # print(df)
                # if df.empty:
                #     print(f'WARNING: date:{date}数据为空。。。')
                #     continue
                # if 'update_flag' in df.columns:
                #     df = df.loc[df['update_flag'] == '1']
                # if unique_keys:
                #     df.sort_values(unique_keys + ['update_flag'], inplace=True)
                #     df.drop_duplicates(subset=unique_keys, keep='last', inplace=True)
                # db.insert_dataframe(save_table, df)
            except:
                traceback.print_exc()

                print(f"{date}报错")
                raise Exception(f"{date}报错")
        print("任务完成")


#  数据同步任务基类
class DataTask(DataQuery, Indicator, metaclass=TaskMeta):
    def sql_task(self):
        raise NotImplementedError

    def data_task(self):
        raise NotImplementedError


class FactorTask(DataQuery, metaclass=TaskMeta):
    def sql_task(self):
        raise NotImplementedError

    def data_task(self):
        raise NotImplementedError

    @classmethod
    def calc_reg_weight(cls, date):
        """
        正态化纠正之后的总市值权重，用以进行 总市值 中性化计算
        """
        close_data = data_api_obj.get_ashareeodprice(
            start_date=date, end_date=date, fields=["close"]
        )  # 当天收盘价
        total_share_data = data_api_obj.get_ashareeodbasic(
            start_date=date, end_date=date, fields=["total_share"]
        )  # 当天总股本
        cap_data = pd.merge(
            close_data, total_share_data, how="outer", on=["trade_date", "ts_code"]
        )
        cap_data["univ_cap"] = cap_data["close"] * cap_data["total_share"]  # 当天总市值
        cap_data.index = cap_data["ts_code"].tolist()  # 设置索引
        cap_data["univ_cap"] = cap_data["univ_cap"].fillna(
            cap_data["univ_cap"].mean()
        )  # 用均值填充缺失值
        cap_data["square_cap"] = cap_data["univ_cap"].apply(
            lambda x: x**0.5
        )  # 进行正态化纠正，参考因子分析中的讲解
        cap_data["regression_weight"] = (
            cap_data["square_cap"] / cap_data["square_cap"].sum()
        )  # 正态化纠正之后的市值权重
        return cap_data[["ts_code", "regression_weight"]]

    @classmethod
    def cal_raw_lcap(cls, date):
        """
        计算总市值的ln
        """
        data = data_api_obj.get_factor_data("total_mv", start_date=date, end_date=date)
        data["raw_ln_total_mv"] = data["total_mv"].apply(np.log)
        data.reset_index(inplace=True)
        return data[["ts_code", "raw_ln_total_mv"]]

    @classmethod
    def cal_circ_mv_weight(cls, date):
        """
        计算截面中所有股票流通市值权重
        """
        cap_data = data_api_obj.get_factor_data(
            "circ_mv", start_date=date, end_date=date
        ).reset_index()
        cap_data["circ_mv"] = cap_data["circ_mv"].fillna(
            cap_data["circ_mv"].mean()
        )  # 采用均值填充缺失值
        cap_data["circ_mv_weight"] = cap_data["circ_mv"] / cap_data["circ_mv"].sum()
        return cap_data[["ts_code", "circ_mv_weight"]]
