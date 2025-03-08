"""
因子运行任务集合
"""

from meta.daily_task.meta_data import *
from meta.db_base.redis_mgr import redis_con
import numpy as np
from datetime import datetime as dt
import time
import statsmodels.api as sm
import numba as nb
from numba import jit
from meta.utils.stock_filter import (
    get_stock_history_name,
    advanced_st_treat,
    add_st_label,
    check_is_new_stock,
    get_estu,
)
from meta.api.data import data_api_obj
from meta.utils.preprocess import median_winsorized, std_winsorized, win_and_std
import traceback

db = MySQLDB(db_url=Config.MYSQL_ENGINE_FACTOR_STR)
db_base = MySQLDB(db_url=Config.MYSQL_ENGINE_STR)
api = API()


# data_api_obj = RedQuantData()


class PrefixFactorPcfNcfTTMM(FactorTask):
    table: str = "pcf_ncf_ttm_m"
    scheme: str = "factors"
    description: str = """
        总市值/现金及现金等价物净增加额(TTM)
    """
    parents: list = ["month_map", "AShareEodBasic", "asharecashflow"]
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                pcf_ncf_ttm_m double NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "trade_date"

        q_dates = pd.date_range(Config.START_DATE, Config.TODAY, freq="Q")
        q_dates = [date.strftime("%Y%m%d") for date in q_dates]
        print("q_dates:", q_dates)
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        print("dates_in_db:", dates_in_db)
        dates_to_download = list(set(q_dates).difference(set(dates_in_db)))
        dates_to_download.sort()
        print("dates_to_download:", dates_to_download)
        cash_flow = pd.DataFrame()
        for to_calc_date in dates_to_download:
            print("to_calc_date:", to_calc_date)
            # 1. 先获取交易日历-自然日的对应表
            month_map_df = data_api_obj.get_month_map()

            # 2. 将日历对应表转为字典
            month_c_t_map = {
                cal_date: trade_date
                for cal_date, trade_date in zip(
                    month_map_df["calendar_date"], month_map_df["trade_date"]
                )
            }

            # 3. 将自然日转为交易日
            to_calc_trade_date = month_c_t_map[to_calc_date]

            # 4. 获取当日的total_mv 总市值数据
            total_mv = data_api_obj.get_ashareeodbasic(
                start_date=to_calc_trade_date, end_date=to_calc_trade_date
            )[["ts_code", "trade_date", "total_mv"]]

            # 5.用pandas的时间序列生成 截至to_calc_date的季度日期
            latest_q_date = (
                pd.date_range(Config.START_DATE, to_calc_date, freq="Q")
                .tolist()[-1]
                .strftime("%Y%m%d")
            )

            # 6. 下面使用了 date_col='ann_date' 来表示我们的start_date与end_date都是作用到了date_col上面来选择
            cash_flow = data_api_obj.get_cashflow(
                start_date=Config.START_DATE,
                end_date=latest_q_date,
                date_col="ann_date",
            )[["ts_code", "ann_date", "end_date", "n_incr_cash_cash_equ"]]
            cash_flow.sort_values("end_date", inplace=True)
            print("cash_flow:\n", cash_flow)

            def treat_q_value(df):

                if df.empty:
                    return None
                latest_end_date = df.end_date.iloc[-1]  # 最近的一个报告日期
                month = int(latest_end_date[4:6])  # 获取日期的月份
                if month == 12:  # 如果是12月，那么年报已经就是ttm
                    return df["n_incr_cash_cash_equ"].iloc[-1]  # 直接返回
                else:
                    try:
                        # 为了防止日期的错乱，我们要保证 上一年年报数据和 上年同日季报有数据，如果没有，直接返回None
                        last_year_end_date = str(int(latest_end_date[:4]) - 1) + "1231"
                        last_year_season_date = str(int(latest_end_date) - 10000)
                        if (
                            last_year_end_date not in df.end_date.tolist()
                            or last_year_season_date not in df.end_date.tolist()
                        ):
                            return None

                        last_year_value = df.set_index("end_date").loc[
                            last_year_end_date, "n_incr_cash_cash_equ"
                        ]  # 上年（相对于最近的一个报告日期latest_end_date）年报
                        last_year_season_value = df.set_index("end_date").loc[
                            last_year_season_date, "n_incr_cash_cash_equ"
                        ]  # 上年（相对于最近的一个报告日期latest_end_date）同期

                        res = (
                            df["n_incr_cash_cash_equ"].iloc[-1]
                            + last_year_value
                            - last_year_season_value
                        )  # 计算最终ttm值
                        return res
                    except:
                        traceback.print_exc()
                        print(traceback.format_exc())
                        return None

            cash_flow_sub = cash_flow.groupby("ts_code").apply(treat_q_value)
            # cash_flow_factor = cash_flow_sub.to_frame(name='q_value')  # Series转DataFrame
            # cash_flow_factor = cash_flow_sub  # Series转DataFrame
            # print(cash_flow_factor)
            if cash_flow_sub.empty:
                print(f"！！ {to_calc_date} 日期为空")
                continue
            print(
                "--------------------------------------- 因子值 ------------------------------------"
            )
            # print(cash_flow_factor)
            cash_flow_factor = cash_flow_sub.to_frame(name="q_value")
            # cash_flow_factor.columns = ['q_value']

            # 合并数据
            total_mv.set_index("ts_code", inplace=True)
            com_df = pd.concat([cash_flow_factor, total_mv], axis=1)

            # 计算最终因子
            com_df["pcf_ncf_ttm_m"] = com_df["total_mv"] / com_df["q_value"]
            # 去掉多余列
            com_df.drop(["q_value", "total_mv"], axis=1, inplace=True)
            com_df.reset_index(inplace=True)
            com_df["trade_date"] = com_df["trade_date"].fillna(method="ffill")
            factor_df = com_df
            factor_df = factor_df[~factor_df["pcf_ncf_ttm_m"].isnull()]
            print(f"{to_calc_date} 准备存储")
            db.insert_dataframe(self.table, factor_df, self.scheme)
            print(f"{to_calc_date} 存储完成！")


class PrefixFactorSize(FactorTask):
    table: str = "size"
    scheme: str = "factors"
    description: str = """
        Barra市值因子
    """
    parents: list = ["asharecalendar", "AShareEodBasic"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                {self.table} double NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    @classmethod
    def calc_size(cls, date, cap_weight_data):
        """
        市值因子
        SIZE: 1.0 * ln(mv)
        :param date:  日期
        :param cap_weight_data:  流通市值权重（剔除了 次新股、st股、一段时间内长期停牌） ts_code, cap_weight 两列
        :return:  'code', 'SIZE'
        """
        lcap_data = cls.cal_raw_lcap(date)  # ln(总市值)

        # 处理缺失值
        lcap_data["raw_ln_total_mv"] = lcap_data["raw_ln_total_mv"].fillna(
            lcap_data["raw_ln_total_mv"].mean()
        )
        # 去极值、流通市值加权、标准化处理
        raw_lcap_data = win_and_std(
            lcap_data, cap_weight_data, "raw_ln_total_mv", "win_std_total_mv"
        )
        raw_lcap_data["SIZE"] = raw_lcap_data["win_std_total_mv"]
        raw_lcap_data.index = raw_lcap_data["ts_code"].tolist()
        return raw_lcap_data[["ts_code", "SIZE"]]

    def data_task(self):
        # now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "trade_date"
        calendar_date_col = "tradedays"
        trade_date_sql = f"""
            select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
            and {calendar_date_col}<={Config.TODAY}
        """
        trade_dates = db_base.read_sql(trade_date_sql)
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        dates_to_download = list(
            set(trade_dates[calendar_date_col].tolist()).difference(set(dates_in_db))
        )
        dates_to_download.sort()
        print("dates_to_download:", dates_to_download)
        for i, date in enumerate(dates_to_download):
            print("date:", date)
            raw_cap_weight_data = self.cal_circ_mv_weight(date)  # 流通市值权重
            estu_data = get_estu(date)  # 获取当天的estu

            # 流通市值-ESTU权重
            cap_weight_data = pd.merge(
                raw_cap_weight_data, estu_data, how="right", on="ts_code"
            )
            # cap_weight_data.drop(["list_date",'delist_date', 'is_st'], axis=1, inplace=True)
            cap_weight_data.drop(["is_st"], axis=1, inplace=True)
            cap_weight_data = cap_weight_data.fillna(
                0.0
            )  # 不在estu中的cap weight被换成0

            size_data = self.calc_size(date, cap_weight_data)
            size_data.reset_index(drop=True, inplace=True)
            size_data["trade_date"] = date
            db.insert_dataframe(self.table, size_data, self.scheme)

            print(f"{date} 存储完成！")


class PrefixFactorSizeNL(FactorTask):
    table: str = "sizenl"
    scheme: str = "factors"
    description: str = """
        Barra非线性规模因子SIZENL
    """
    parents: list = [
        "asharecalendar",
        "ashareeodprice",
        "AShareEodBasic",
        "size",
    ]  # 根据每个task的table的字段来关联
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                {self.table} double NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    @classmethod
    def calc_sizenl(cls, size_data, regression_weight, cap_weight_data):
        """
        SIZENL 非线性规模因子
        :param size_data: barra size因子数据
        :param regression_weight:  总市值权重
        :param cap_weight_data:  流通市值权重（剔除了 次新股、st股、一段时间内长期停牌） code, cap_weight 两列
        :return:  'ts_code', 'SIZENL'
        """
        data = pd.merge(
            size_data, regression_weight, how="right", on="ts_code"
        )  # 合并总市值权重
        data["regression_weight"] = data["regression_weight"].fillna(0.0)

        # regression: cal raw sizenl
        data["size"] = data["size"].fillna(data["size"].mean())
        data["size3"] = data["size"] ** 3.0  # size的三次方
        mod = sm.WLS(
            data["size3"], data["size"], weights=data["regression_weight"]
        )  # 加权最小二乘 WLS 回归
        mod = mod.fit()
        data["raw_sizenl"] = mod.resid  # 残差值就是因子

        # winsorized
        data = win_and_std(
            data, cap_weight_data, "raw_sizenl", "lcap"
        )  # 去极值、流通市值加权、标准化处理
        data["sizenl"] = data["lcap"] * 1.0
        data.index = data["ts_code"].tolist()
        return data[["ts_code", "sizenl"]]

    def data_task(self):
        # now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "trade_date"
        calendar_date_col = "tradedays"
        trade_date_sql = f"""
            select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
            and {calendar_date_col}<={Config.TODAY}
        """
        trade_dates = db_base.read_sql(trade_date_sql)
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        dates_to_download = list(
            set(trade_dates[calendar_date_col].tolist()).difference(set(dates_in_db))
        )
        dates_to_download.sort()
        print("dates_to_download:", dates_to_download)

        for i, date in enumerate(dates_to_download):

            print("date:", date)

            regression_weight = self.calc_reg_weight(date)
            raw_cap_weight_data = self.cal_circ_mv_weight(date)  # 流通市值权重
            estu_data = get_estu(date)  # 获取当天的estu
            # 流通市值-ESTU权重
            cap_weight_data = pd.merge(
                raw_cap_weight_data, estu_data, how="right", on="ts_code"
            )
            # cap_weight_data.drop(["list_date",'delist_date', 'is_st'], axis=1, inplace=True)
            cap_weight_data.drop(["is_st"], axis=1, inplace=True)
            cap_weight_data = cap_weight_data.fillna(
                0.0
            )  # 不在estu中的cap weight被换成0
            size_data = data_api_obj.get_factor_data(
                "SIZE", start_date=date, end_date=date
            )
            size_data.reset_index(inplace=True)
            sizenl_data = self.calc_sizenl(
                size_data, regression_weight, cap_weight_data
            )
            sizenl_data.reset_index(drop=True, inplace=True)
            sizenl_data["trade_date"] = date
            db.insert_dataframe(self.table, sizenl_data, self.scheme)

            print(f"{date} 存储完成！")


# ========================================== 成长类 因子 ============================================
"""
基于季节性随机游走模型预测的净利润和营业收入计算标准化的预期外净利润（SUE）和营业收入（SUR），
Earnings Surprise 或者 Revenue Surprise用来度量业绩超预期的程度，根据
模型是否带漂移项，计算 SUE0、SUE1、SUR0 和 SUR1 共 4 个业
绩超预期类指标。

SUR0 和 SUR1 两个标准化预期外营收（standardized unexpected
revenue， SUR）作为 Revenue Surprise 的度量

业绩超预期类因子——东方证券
https://qiniu-images.datayes.com/tolishuai/0703.1.pdf
"""

# class PrefixFactorSUR4(FactorTask):
#     """
#     基于季节性随机游走模型预测的净利润和营业收入计算标准化的预期
#     外净利润（SUE）和营业收入（SUR），用来度量业绩超预期的程度，根据
#     模型是否带漂移项，我们计算了 SUE0、SUE1、SUR0 和 SUR1 共 4 个业
#     绩超预期类指标。
#     """
#     table: str = 'SUR4'
#     scheme: str = 'factors'
#     description: str = """
#         标准化预期外营业收入
#     """
#     parents: list = ['month_map', 'AShareEodBasic', 'asharecashflow']
#     turn_on: bool = True
#     freq = Freq.daily.value
#
#     def sql_task(self):
#         sql = f"""
#             CREATE TABLE if not exists {self.scheme}.{self.table}(
#                 ts_code varchar(9) NULL,
#                 trade_date varchar(8) NULL,
#                 pcf_ncf_ttm_m double NULL
#             )
#             ENGINE=InnoDB
#             DEFAULT CHARSET=utf8
#             COLLATE=utf8_general_ci
#         """
#         return sql
#
#     def data_task(self):
#         now = dt.now()
#         if now.weekday() != 5:  # 只在周六运行，非周六直接返回
#             return
#         col_name = 'trade_date'
#
#         q_dates = pd.date_range(Config.START_DATE, Config.TODAY, freq='Q')
#         q_dates = [date.strftime('%Y%m%d') for date in q_dates]
#         dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[col_name].tolist()
#         dates_to_download = list(set(q_dates).difference(set(dates_in_db)))
#         dates_to_download.sort()
#         cash_flow = pd.DataFrame()
#         for to_calc_date in dates_to_download:
#             # 1. 先获取交易日历-自然日的对应表
#             month_map_df = data_api_obj.get_month_map()
#
#             # 2. 将日历对应表转为字典
#             month_c_t_map = {cal_date: trade_date for cal_date, trade_date in
#                              zip(month_map_df['calendar_date'], month_map_df['trade_date'])}
#
#             # 3. 将自然日转为交易日
#             to_calc_trade_date = month_c_t_map[to_calc_date]
#
#             # 4. 获取当日的total_mv 总市值数据
#             total_mv = data_api_obj.get_ashareeodbasic(start_date=to_calc_trade_date, end_date=to_calc_trade_date)[
#                 ['ts_code', 'trade_date', 'total_mv']]
#
#             # 5.用pandas的时间序列生成 截至to_calc_date的季度日期
#             latest_q_date = pd.date_range(Config.START_DATE, to_calc_date, freq='Q').tolist()[-1].strftime('%Y%m%d')
#
#             # 6. 下面使用了 date_col='ann_date' 来表示我们的start_date与end_date都是作用到了date_col上面来选择
#             cash_flow = \
#                 data_api_obj.get_cashflow(start_date=Config.START_DATE, end_date=latest_q_date, date_col='ann_date')[
#                     ['ts_code', 'ann_date', 'end_date', 'n_incr_cash_cash_equ']]
#             cash_flow.sort_values('end_date', inplace=True)
#
#             def treat_q_value(df):
#
#                 if df.empty:
#                     return None
#                 latest_end_date = df.end_date.iloc[-1]  # 最近的一个报告日期
#                 month = int(latest_end_date[4:6])  # 获取日期的月份
#                 if month == 12:  # 如果是12月，那么年报已经就是ttm
#                     return df['n_incr_cash_cash_equ'].iloc[-1]  # 直接返回
#                 else:
#                     try:
#                         # 为了防止日期的错乱，我们要保证 上一年年报数据和 上年同日季报有数据，如果没有，直接返回None
#                         last_year_end_date = str(int(latest_end_date[:4]) - 1) + '1231'
#                         last_year_season_date = str(int(latest_end_date) - 10000)
#                         if last_year_end_date not in df.end_date.tolist() or last_year_season_date not in df.end_date.tolist():
#                             return None
#
#                         last_year_value = df.set_index('end_date').loc[
#                             last_year_end_date, 'n_incr_cash_cash_equ']  # 上年（相对于最近的一个报告日期latest_end_date）年报
#                         last_year_season_value = df.set_index('end_date').loc[
#                             last_year_season_date, 'n_incr_cash_cash_equ']  # 上年（相对于最近的一个报告日期latest_end_date）同期
#
#                         res = df['n_incr_cash_cash_equ'].iloc[-1] + last_year_value - last_year_season_value  # 计算最终ttm值
#                         return res
#                     except:
#                         return None
#
#             cash_flow_sub = cash_flow.groupby('ts_code').apply(treat_q_value)
#             cash_flow_factor = cash_flow_sub.to_frame(name='q_value')  # Series转DataFrame
#
#             # 合并数据
#             total_mv.set_index('ts_code', inplace=True)
#             com_df = pd.concat([cash_flow_factor, total_mv], axis=1)
#
#             # 计算最终因子
#             com_df['pcf_ncf_ttm_m'] = com_df['total_mv'] / com_df['q_value']
#             # 去掉多余列
#             com_df.drop(['q_value', 'total_mv'], axis=1, inplace=True)
#             com_df.reset_index(inplace=True)
#             com_df['trade_date'] = com_df['trade_date'].fillna(method='ffill')
#             factor_df = com_df
#             factor_df = factor_df[~factor_df['pcf_ncf_ttm_m'].isnull()]
#             db.insert_dataframe(self.table, factor_df, self.scheme)
