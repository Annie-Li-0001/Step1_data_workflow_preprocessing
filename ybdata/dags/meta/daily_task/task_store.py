"""
文件说明
"""

import pandas as pd

# from typing import Callable
# import traceback
# import pandas as pd
# from . import Config
# import time
# from tqdm import tqdm
# from . import API
# from . import MySQLDB
# import random
# from meta.const import Freq
# from meta.base import API
# from meta.config import Config
# from meta.db_base.mysql_db import MySQLDB
# from meta.daily_task.meta_data import *
from meta.daily_task.meta_data import *
from meta.db_base.redis_mgr import redis_con
from meta.utils.date_func import RebalanceDatesManager
import numpy as np
from datetime import datetime as dt
import time
from typing import List
import numba as nb
from numba import jit
from meta.api.data import RedQuantData

db = MySQLDB(db_url=Config.MYSQL_ENGINE_STR)
api = API()
data_api_obj = RedQuantData()


# mongo_client = MongoClient(Config.MONGO_ENGINE_STR)


#  所有数据同步任务类都以PrefixData开头
class PrefixDataStockBasic(DataTask):
    table: str = "stock_basic"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股股票基本信息下载
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                name varchar(12) NULL,
                symbol varchar(12) NULL,
                area varchar(12) NULL,
                industry varchar(12) NULL,
                list_date varchar(8) NULL,
                delist_date varchar(8) NULL,
                market varchar(8) NULL,
                exchange varchar(8) NULL,
                list_status varchar(8) NULL,
                is_hs varchar(8) NULL,
                act_ent_type varchar(8) NULL,
                KEY `stock_basic_ts_code_IDX` (`ts_code`) USING BTREE
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        df_list = []
        fields = "ts_code,name,symbol,area,industry,list_date,delist_date,market,exchange,list_status,is_hs,act_ent_type"
        df = api.stock_basic(exchange="", fields=fields)
        df_list.append(df)
        df = api.stock_basic(exchange="", fields=fields, list_status="D")
        df_list.append(df)
        df = api.stock_basic(exchange="", fields=fields, list_status="P")
        df_list.append(df)
        df = pd.concat(df_list)
        df.drop_duplicates(subset=["ts_code"], keep="first", inplace=True)
        df.sort_values(by=["list_date"], inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, df, self.scheme)


class PrefixDataCalendar(DataTask):
    table: str = "asharecalendar"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股交易日历下载
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                tradedays varchar(8) NULL,
                is_open varchar(1) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci;
        """
        return sql

    def data_task(self):
        df = api.trade_cal(is_open="1")
        df = df[["cal_date", "is_open"]]
        df = df.rename(columns={"cal_date": "tradedays"})
        df.sort_values(by="tradedays", inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, df, self.scheme)


class PrefixDataMonthMap(DataTask):
    table: str = "month_map"
    scheme: str = "stock"
    description: str = """
            每月交易日与自然日最后一天的日期映射
        """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                calendar_date varchar(8) NULL,
                trade_date varchar(8) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        tdays = db.read_sql(f"select tradedays, is_open from {self.parents[0]}")
        tdays.rename(columns={"tradedays": "calendar_date"}, inplace=True)
        tdays.set_index("calendar_date", inplace=True)
        s_dates = pd.Series(tdays.index, index=pd.to_datetime(tdays.index))
        print(s_dates)
        func_last = lambda ser: ser.iat[-1]
        new_dates = s_dates.resample("M").apply(func_last)
        month_map = new_dates.to_frame(name="trade_date")
        month_map.index.name = "calendar_date"
        month_map.reset_index(inplace=True)
        month_map["calendar_date"] = month_map["calendar_date"].apply(
            lambda x: x.strftime("%Y%m%d")
        )
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, month_map, self.scheme)


class PrefixDataAShareSuspend(DataTask):
    table: str = "asharesuspend"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股停复牌数据
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
            ts_code varchar(9) NULL,
            trade_date varchar(8) NULL,
            suspend_timing varchar(12) NULL,
            suspend_type varchar(1) NULL
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8
        COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.suspend_d
        self.fetch_data_by_day(col_name, func, self.table)


class PrefixDataAShareEodPrice(DataTask):
    table: str = "ashareeodprice"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股股票不复权行情数据下载
    """
    parents: list = ["asharecalendar", "asharesuspend"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
              `ts_code` varchar(9) DEFAULT NULL,
              `trade_date` varchar(8) DEFAULT NULL,
              `open` double DEFAULT NULL,
              `high` double DEFAULT NULL,
              `low` double DEFAULT NULL,
              `close` double DEFAULT NULL,
              `pre_close` double DEFAULT NULL,
              `change` double DEFAULT NULL,
              `pct_chg` double DEFAULT NULL,
              `vol` double DEFAULT NULL,
              `amount` double DEFAULT NULL,
              UNIQUE KEY `AShareEodPrice_date_IDX` (`trade_date`,`ts_code`) USING BTREE,
              KEY `AShareEodPrice_code_IDX` (`ts_code`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.daily
        unique_keys = ["ts_code", "trade_date"]

        calendar_date_col = "tradedays"
        trade_date_sql = f"""
                select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
                and {calendar_date_col}<={Config.TODAY}
                """
        trade_date_df = db.read_sql(trade_date_sql)
        trade_dates: List = trade_date_df[calendar_date_col].tolist()
        dates_in_db = db.read_sql(f"select distinct {col_name} from {self.table}")[
            col_name
        ].tolist()
        dates_to_download = list(set(trade_dates).difference(set(dates_in_db)))
        dates_to_download.sort()
        for i, date in enumerate(dates_to_download):
            try:
                df = func(**{"trade_date": date})
                if df.empty:
                    print("数据为空", date)
                    continue
                if "update_flag" in df.columns:
                    df = df.loc[df["update_flag"] == "1"]
                if unique_keys:
                    df.drop_duplicates(subset=unique_keys, keep="first", inplace=True)
                # 检查当天的停牌情况
                # df_with_suspension_stock = df
                if date != trade_dates[0]:  # 第一个同步日期之前没有行情，所以不能判断
                    # 第二个同步日期开始，只要当天是停牌的，就用前一个交易日来填充当天的行情
                    suspension_stock_df: pd.DataFrame = db.read_sql(
                        f"select * from {self.parents[1]} where trade_date='{date}' and suspend_type='S'"
                    )
                    suspension_stock_list: List = suspension_stock_df.ts_code.tolist()
                    suspension_stock_sql_str = "','".join(suspension_stock_list)
                    previous_trade_date = trade_dates[trade_dates.index(date) - 1]
                    previous_market_sql = f"""
                        select * from {self.table} 
                        where ts_code in ('{suspension_stock_sql_str}') and trade_date='{previous_trade_date}'
                    """
                    previous_market_data = db.read_sql(previous_market_sql)
                    previous_market_data["trade_date"] = date
                    df_with_suspension_stock = pd.concat([df, previous_market_data])
                    # print(f'本日行情{date}与停牌{suspension_stock_list}的交集:')
                    # print(set(df.ts_code.tolist()) & set(suspension_stock_list))
                    df_with_suspension_stock.drop_duplicates(
                        subset=unique_keys, keep="first", inplace=True
                    )
                    db.insert_dataframe(self.table, df_with_suspension_stock)
                else:
                    db.insert_dataframe(self.table, df)
            except:
                traceback.print_exc()

                print(f"！！！=============================== {date}报错")
        print("任务完成")
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataAShareAdjFactor(DataTask):
    table: str = "ashareadjfactor"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股股票复权因子
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table} (
              `ts_code` varchar(9) DEFAULT NULL,
              `trade_date` varchar(8) DEFAULT NULL,
              `adj_factor` double DEFAULT NULL,
              UNIQUE KEY `ashareadjfactor_trade_date_IDX` (`trade_date`,`ts_code`) USING BTREE,
              KEY `AShareAdjFactor_date_IDX` (`ts_code`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.adj_factor
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataLimitList(DataTask):
    table: str = "asharelimitlist"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股涨跌停列表
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
            trade_date varchar(8) NULL,
            ts_code varchar(9) NULL,
            name varchar(7) NULL,
            `close` DOUBLE NULL,
            pct_chg DOUBLE NULL,
            amp DOUBLE NULL,
            fc_ratio DOUBLE NULL,
            fl_ratio DOUBLE NULL,
            fd_amount DOUBLE NULL,
            first_time varchar(8) NULL,
            last_time varchar(8) NULL,
            open_times INT NULL,
            strth DOUBLE NULL,
            `limit` varchar(1) NULL
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8
        COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.limit_list
        self.fetch_data_by_day(col_name, func, self.table)


class PrefixDataDailyBasic(DataTask):
    table: str = "AShareEodBasic"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股每日指标表
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
          `ts_code` varchar(9) DEFAULT NULL,
          `trade_date` varchar(8) DEFAULT NULL,
          `close` double DEFAULT NULL,
          `turnover_rate` double DEFAULT NULL,
          `turnover_rate_f` double DEFAULT NULL,
          `volume_ratio` double DEFAULT NULL,
          `pe` double DEFAULT NULL,
          `pe_ttm` double DEFAULT NULL,
          `pb` double DEFAULT NULL,
          `ps` double DEFAULT NULL,
          `ps_ttm` double DEFAULT NULL,
          `dv_ratio` double DEFAULT NULL,
          `dv_ttm` double DEFAULT NULL,
          `total_share` double DEFAULT NULL,
          `float_share` double DEFAULT NULL,
          `free_share` double DEFAULT NULL,
          `total_mv` double DEFAULT NULL,
          `circ_mv` double DEFAULT NULL,
          UNIQUE KEY `AShareEodBasic_date_IDX` (`trade_date`,`ts_code`) USING BTREE,
          KEY `AShareEodBasic_code_IDX` (`ts_code`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等'
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.daily_basic
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataMoneyFlow(DataTask):
    table: str = "asharemoneyflow"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股每日资金流表
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
          `ts_code` varchar(9) DEFAULT NULL,
          `trade_date` varchar(8) DEFAULT NULL,
          `buy_sm_vol` int(11) DEFAULT NULL,
          `buy_sm_amount` double DEFAULT NULL,
          `sell_sm_vol` int(11) DEFAULT NULL,
          `sell_sm_amount` double DEFAULT NULL,
          `buy_md_vol` int(11) DEFAULT NULL,
          `buy_md_amount` double DEFAULT NULL,
          `sell_md_vol` int(11) DEFAULT NULL,
          `sell_md_amount` double DEFAULT NULL,
          `buy_lg_vol` int(11) DEFAULT NULL,
          `buy_lg_amount` double DEFAULT NULL,
          `sell_lg_vol` int(11) DEFAULT NULL,
          `sell_lg_amount` double DEFAULT NULL,
          `buy_elg_vol` int(11) DEFAULT NULL,
          `buy_elg_amount` double DEFAULT NULL,
          `sell_elg_vol` int(11) DEFAULT NULL,
          `sell_elg_amount` double DEFAULT NULL,
          `net_mf_vol` int(11) DEFAULT NULL,
          `net_mf_amount` double DEFAULT NULL,
           UNIQUE KEY `asharemoneyflow_code_date_IDX` (`trade_date`,`ts_code`) USING BTREE,
           KEY `asharemoneyflow_ts_code_IDX` (`ts_code`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.moneyflow
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataFinaIndicator(DataTask):
    # 本任务作为周频任务，但是作为airflow的任务依然放在同一个日频的dag下面，只是在data_task中进行调度日的判断
    table: str = "asharefinaindicator"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股财务指标数据
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
          `ts_code` varchar(9) DEFAULT NULL,
          `ann_date` varchar(8) DEFAULT NULL,
          `end_date` varchar(8) DEFAULT NULL,
          `eps` double DEFAULT NULL,
          `dt_eps` double DEFAULT NULL,
          `total_revenue_ps` double DEFAULT NULL,
          `revenue_ps` double DEFAULT NULL,
          `capital_rese_ps` double DEFAULT NULL,
          `surplus_rese_ps` double DEFAULT NULL,
          `undist_profit_ps` double DEFAULT NULL,
          `extra_item` double DEFAULT NULL,
          `profit_dedt` double DEFAULT NULL,
          `gross_margin` double DEFAULT NULL,
          `current_ratio` double DEFAULT NULL,
          `quick_ratio` double DEFAULT NULL,
          `cash_ratio` double DEFAULT NULL,
          `invturn_days` double DEFAULT NULL,
          `arturn_days` double DEFAULT NULL,
          `inv_turn` double DEFAULT NULL,
          `ar_turn` double DEFAULT NULL,
          `ca_turn` double DEFAULT NULL,
          `fa_turn` double DEFAULT NULL,
          `assets_turn` double DEFAULT NULL,
          `op_income` double DEFAULT NULL,
          `valuechange_income` double DEFAULT NULL,
          `interst_income` double DEFAULT NULL,
          `daa` double DEFAULT NULL,
          `ebit` double DEFAULT NULL,
          `ebitda` double DEFAULT NULL,
          `fcff` double DEFAULT NULL,
          `fcfe` double DEFAULT NULL,
          `current_exint` double DEFAULT NULL,
          `noncurrent_exint` double DEFAULT NULL,
          `interestdebt` double DEFAULT NULL,
          `netdebt` double DEFAULT NULL,
          `tangible_asset` double DEFAULT NULL,
          `working_capital` double DEFAULT NULL,
          `networking_capital` double DEFAULT NULL,
          `invest_capital` double DEFAULT NULL,
          `retained_earnings` double DEFAULT NULL,
          `diluted2_eps` double DEFAULT NULL,
          `bps` double DEFAULT NULL,
          `ocfps` double DEFAULT NULL,
          `retainedps` double DEFAULT NULL,
          `cfps` double DEFAULT NULL,
          `ebit_ps` double DEFAULT NULL,
          `fcff_ps` double DEFAULT NULL,
          `fcfe_ps` double DEFAULT NULL,
          `netprofit_margin` double DEFAULT NULL,
          `grossprofit_margin` double DEFAULT NULL,
          `cogs_of_sales` double DEFAULT NULL,
          `expense_of_sales` double DEFAULT NULL,
          `profit_to_gr` double DEFAULT NULL,
          `saleexp_to_gr` double DEFAULT NULL,
          `adminexp_of_gr` double DEFAULT NULL,
          `finaexp_of_gr` double DEFAULT NULL,
          `impai_ttm` double DEFAULT NULL,
          `gc_of_gr` double DEFAULT NULL,
          `op_of_gr` double DEFAULT NULL,
          `ebit_of_gr` double DEFAULT NULL,
          `roe` double DEFAULT NULL,
          `roe_waa` double DEFAULT NULL,
          `roe_dt` double DEFAULT NULL,
          `roa` double DEFAULT NULL,
          `npta` double DEFAULT NULL,
          `roic` double DEFAULT NULL,
          `roe_yearly` double DEFAULT NULL,
          `roa2_yearly` double DEFAULT NULL,
          `roe_avg` double DEFAULT NULL,
          `opincome_of_ebt` double DEFAULT NULL,
          `investincome_of_ebt` double DEFAULT NULL,
          `n_op_profit_of_ebt` double DEFAULT NULL,
          `tax_to_ebt` double DEFAULT NULL,
          `dtprofit_to_profit` double DEFAULT NULL,
          `salescash_to_or` double DEFAULT NULL,
          `ocf_to_or` double DEFAULT NULL,
          `ocf_to_opincome` double DEFAULT NULL,
          `capitalized_to_da` double DEFAULT NULL,
          `debt_to_assets` double DEFAULT NULL,
          `assets_to_eqt` double DEFAULT NULL,
          `dp_assets_to_eqt` double DEFAULT NULL,
          `ca_to_assets` double DEFAULT NULL,
          `nca_to_assets` double DEFAULT NULL,
          `tbassets_to_totalassets` double DEFAULT NULL,
          `int_to_talcap` double DEFAULT NULL,
          `eqt_to_talcapital` double DEFAULT NULL,
          `currentdebt_to_debt` double DEFAULT NULL,
          `longdeb_to_debt` double DEFAULT NULL,
          `ocf_to_shortdebt` double DEFAULT NULL,
          `debt_to_eqt` double DEFAULT NULL,
          `eqt_to_debt` double DEFAULT NULL,
          `eqt_to_interestdebt` double DEFAULT NULL,
          `tangibleasset_to_debt` double DEFAULT NULL,
          `tangasset_to_intdebt` double DEFAULT NULL,
          `tangibleasset_to_netdebt` double DEFAULT NULL,
          `ocf_to_debt` double DEFAULT NULL,
          `ocf_to_interestdebt` double DEFAULT NULL,
          `ocf_to_netdebt` double DEFAULT NULL,
          `ebit_to_interest` double DEFAULT NULL,
          `longdebt_to_workingcapital` double DEFAULT NULL,
          `ebitda_to_debt` double DEFAULT NULL,
          `turn_days` double DEFAULT NULL,
          `roa_yearly` double DEFAULT NULL,
          `roa_dp` double DEFAULT NULL,
          `fixed_assets` double DEFAULT NULL,
          `profit_prefin_exp` double DEFAULT NULL,
          `non_op_profit` double DEFAULT NULL,
          `op_to_ebt` double DEFAULT NULL,
          `nop_to_ebt` double DEFAULT NULL,
          `ocf_to_profit` double DEFAULT NULL,
          `cash_to_liqdebt` double DEFAULT NULL,
          `cash_to_liqdebt_withinterest` double DEFAULT NULL,
          `op_to_liqdebt` double DEFAULT NULL,
          `op_to_debt` double DEFAULT NULL,
          `roic_yearly` double DEFAULT NULL,
          `total_fa_trun` double DEFAULT NULL,
          `profit_to_op` double DEFAULT NULL,
          `q_opincome` double DEFAULT NULL,
          `q_investincome` double DEFAULT NULL,
          `q_dtprofit` double DEFAULT NULL,
          `q_eps` double DEFAULT NULL,
          `q_netprofit_margin` double DEFAULT NULL,
          `q_gsprofit_margin` double DEFAULT NULL,
          `q_exp_to_sales` double DEFAULT NULL,
          `q_profit_to_gr` double DEFAULT NULL,
          `q_saleexp_to_gr` double DEFAULT NULL,
          `q_adminexp_to_gr` double DEFAULT NULL,
          `q_finaexp_to_gr` double DEFAULT NULL,
          `q_impair_to_gr_ttm` double DEFAULT NULL,
          `q_gc_to_gr` double DEFAULT NULL,
          `q_op_to_gr` double DEFAULT NULL,
          `q_roe` double DEFAULT NULL,
          `q_dt_roe` double DEFAULT NULL,
          `q_npta` double DEFAULT NULL,
          `q_opincome_to_ebt` double DEFAULT NULL,
          `q_investincome_to_ebt` double DEFAULT NULL,
          `q_dtprofit_to_profit` double DEFAULT NULL,
          `q_salescash_to_or` double DEFAULT NULL,
          `q_ocf_to_sales` double DEFAULT NULL,
          `q_ocf_to_or` double DEFAULT NULL,
          `basic_eps_yoy` double DEFAULT NULL,
          `dt_eps_yoy` double DEFAULT NULL,
          `cfps_yoy` double DEFAULT NULL,
          `op_yoy` double DEFAULT NULL,
          `ebt_yoy` double DEFAULT NULL,
          `netprofit_yoy` double DEFAULT NULL,
          `dt_netprofit_yoy` double DEFAULT NULL,
          `ocf_yoy` double DEFAULT NULL,
          `roe_yoy` double DEFAULT NULL,
          `bps_yoy` double DEFAULT NULL,
          `assets_yoy` double DEFAULT NULL,
          `eqt_yoy` double DEFAULT NULL,
          `tr_yoy` double DEFAULT NULL,
          `or_yoy` double DEFAULT NULL,
          `q_gr_yoy` double DEFAULT NULL,
          `q_gr_qoq` double DEFAULT NULL,
          `q_sales_yoy` double DEFAULT NULL,
          `q_sales_qoq` double DEFAULT NULL,
          `q_op_yoy` double DEFAULT NULL,
          `q_op_qoq` double DEFAULT NULL,
          `q_profit_yoy` double DEFAULT NULL,
          `q_profit_qoq` double DEFAULT NULL,
          `q_netprofit_yoy` double DEFAULT NULL,
          `q_netprofit_qoq` double DEFAULT NULL,
          `equity_yoy` double DEFAULT NULL,
          `rd_exp` double DEFAULT NULL,
          `update_flag` varchar(1) DEFAULT NULL,
          UNIQUE KEY `asharefinaindicator_ts_code_end_IDX` (`ts_code`,`end_date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        return sql

    def data_task(self):
        now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "end_date"
        func = api.fina_indicator_vip
        unique_keys = ["ts_code", "end_date"]
        fields = "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        self.fetch_data_by_quarter(
            col_name, func, self.table, unique_keys=unique_keys, fields=fields
        )


class PrefixDataIncome(DataTask):
    table: str = "ashareincome"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股利润表数据
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
          `ts_code` varchar(9) DEFAULT NULL,
          `ann_date` varchar(8) DEFAULT NULL,
          `f_ann_date` varchar(8) DEFAULT NULL,
          `end_date` varchar(8) DEFAULT NULL,
          `report_type` varchar(2) DEFAULT NULL,
          `comp_type` varchar(2) DEFAULT NULL,
          `end_type` varchar(2) DEFAULT NULL,
          `basic_eps` double DEFAULT NULL,
          `diluted_eps` double DEFAULT NULL,
          `total_revenue` double DEFAULT NULL,
          `revenue` double DEFAULT NULL,
          `int_income` double DEFAULT NULL,
          `prem_earned` double DEFAULT NULL,
          `comm_income` double DEFAULT NULL,
          `n_commis_income` double DEFAULT NULL,
          `n_oth_income` double DEFAULT NULL,
          `n_oth_b_income` double DEFAULT NULL,
          `prem_income` double DEFAULT NULL,
          `out_prem` double DEFAULT NULL,
          `une_prem_reser` double DEFAULT NULL,
          `reins_income` double DEFAULT NULL,
          `n_sec_tb_income` double DEFAULT NULL,
          `n_sec_uw_income` double DEFAULT NULL,
          `n_asset_mg_income` double DEFAULT NULL,
          `oth_b_income` double DEFAULT NULL,
          `fv_value_chg_gain` double DEFAULT NULL,
          `invest_income` double DEFAULT NULL,
          `ass_invest_income` double DEFAULT NULL,
          `forex_gain` double DEFAULT NULL,
          `total_cogs` double DEFAULT NULL,
          `oper_cost` double DEFAULT NULL,
          `int_exp` double DEFAULT NULL,
          `comm_exp` double DEFAULT NULL,
          `biz_tax_surchg` double DEFAULT NULL,
          `sell_exp` double DEFAULT NULL,
          `admin_exp` double DEFAULT NULL,
          `fin_exp` double DEFAULT NULL,
          `assets_impair_loss` double DEFAULT NULL,
          `prem_refund` double DEFAULT NULL,
          `compens_payout` double DEFAULT NULL,
          `reser_insur_liab` double DEFAULT NULL,
          `div_payt` double DEFAULT NULL,
          `reins_exp` double DEFAULT NULL,
          `oper_exp` double DEFAULT NULL,
          `compens_payout_refu` double DEFAULT NULL,
          `insur_reser_refu` double DEFAULT NULL,
          `reins_cost_refund` double DEFAULT NULL,
          `other_bus_cost` double DEFAULT NULL,
          `operate_profit` double DEFAULT NULL,
          `non_oper_income` double DEFAULT NULL,
          `non_oper_exp` double DEFAULT NULL,
          `nca_disploss` double DEFAULT NULL,
          `total_profit` double DEFAULT NULL,
          `income_tax` double DEFAULT NULL,
          `n_income` double DEFAULT NULL,
          `n_income_attr_p` double DEFAULT NULL,
          `minority_gain` double DEFAULT NULL,
          `oth_compr_income` double DEFAULT NULL,
          `t_compr_income` double DEFAULT NULL,
          `compr_inc_attr_p` double DEFAULT NULL,
          `compr_inc_attr_m_s` double DEFAULT NULL,
          `ebit` double DEFAULT NULL,
          `ebitda` double DEFAULT NULL,
          `insurance_exp` double DEFAULT NULL,
          `undist_profit` double DEFAULT NULL,
          `distable_profit` double DEFAULT NULL,
          `rd_exp` double DEFAULT NULL,
          `fin_exp_int_exp` double DEFAULT NULL,
          `fin_exp_int_inc` double DEFAULT NULL,
          `transfer_surplus_rese` double DEFAULT NULL,
          `transfer_housing_imprest` double DEFAULT NULL,
          `transfer_oth` double DEFAULT NULL,
          `adj_lossgain` double DEFAULT NULL,
          `withdra_legal_surplus` double DEFAULT NULL,
          `withdra_legal_pubfund` double DEFAULT NULL,
          `withdra_biz_devfund` double DEFAULT NULL,
          `withdra_rese_fund` double DEFAULT NULL,
          `withdra_oth_ersu` double DEFAULT NULL,
          `workers_welfare` double DEFAULT NULL,
          `distr_profit_shrhder` double DEFAULT NULL,
          `prfshare_payable_dvd` double DEFAULT NULL,
          `comshare_payable_dvd` double DEFAULT NULL,
          `capit_comstock_div` double DEFAULT NULL,
          `continued_net_profit` double DEFAULT NULL,
          `update_flag` double DEFAULT NULL,
          UNIQUE KEY `income_ts_code_IDX` (`ts_code`,`end_date`,`report_type`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8

        """
        return sql

    def data_task(self):
        # now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "end_date"
        func = api.income_vip
        unique_keys = ["ts_code", "end_date", "report_type"]
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag"

        self.fetch_data_by_quarter(
            col_name,
            func,
            self.table,
            unique_keys=unique_keys,
            fields=fields,
            report_type=list(range(1, 13)),
        )


class PrefixDataExpress(DataTask):
    table: str = "ashareexpress"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股业绩快报
    """
    parents: list = None
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists  {self.scheme}.{self.table} (
          `ts_code` varchar(9) DEFAULT NULL,
          `ann_date` varchar(9) DEFAULT NULL,
          `end_date` varchar(9) DEFAULT NULL,
          `revenue` double DEFAULT NULL,
          `operate_profit` double DEFAULT NULL,
          `total_profit` double DEFAULT NULL,
          `n_income` double DEFAULT NULL,
          `total_assets` double DEFAULT NULL,
          `total_hldr_eqy_exc_min_int` double DEFAULT NULL,
          `diluted_eps` double DEFAULT NULL,
          `diluted_roe` double DEFAULT NULL,
          `yoy_net_profit` double DEFAULT NULL,
          `bps` double DEFAULT NULL,
          `yoy_sales` double DEFAULT NULL,
          `yoy_op` double DEFAULT NULL,
          `yoy_tp` double DEFAULT NULL,
          `yoy_dedu_np` double DEFAULT NULL,
          `yoy_eps` double DEFAULT NULL,
          `yoy_roe` double DEFAULT NULL,
          `growth_assets` double DEFAULT NULL,
          `yoy_equity` double DEFAULT NULL,
          `growth_bps` double DEFAULT NULL,
          `or_last_year` double DEFAULT NULL,
          `op_last_year` double DEFAULT NULL,
          `tp_last_year` double DEFAULT NULL,
          `np_last_year` double DEFAULT NULL,
          `eps_last_year` double DEFAULT NULL,
          `open_net_assets` double DEFAULT NULL,
          `open_bps` double DEFAULT NULL,
          `perf_summary` text,
          `is_audit` bigint(20) DEFAULT NULL,
          `remark` text
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        return sql

    def data_task(self):
        now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "end_date"
        func = api.express_vip
        unique_keys = ["ts_code", "end_date"]
        fields = "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark"
        self.fetch_data_by_quarter(
            col_name, func, self.table, unique_keys=unique_keys, fields=fields
        )


class PrefixDataBalanceSheet(DataTask):
    table: str = "asharebalancesheet"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股资产负债表
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}(
          `ts_code` varchar(9) DEFAULT NULL,
          `ann_date` varchar(8) DEFAULT NULL,
          `f_ann_date` varchar(8) DEFAULT NULL,
          `end_date` varchar(8) DEFAULT NULL,
          `report_type` varchar(2) DEFAULT NULL,
          `comp_type` varchar(2) DEFAULT NULL,
          `end_type` varchar(2) DEFAULT NULL,
          `total_share` double DEFAULT NULL,
          `cap_rese` double DEFAULT NULL,
          `undistr_porfit` double DEFAULT NULL,
          `surplus_rese` double DEFAULT NULL,
          `special_rese` double DEFAULT NULL,
          `money_cap` double DEFAULT NULL,
          `trad_asset` double DEFAULT NULL,
          `notes_receiv` double DEFAULT NULL,
          `accounts_receiv` double DEFAULT NULL,
          `oth_receiv` double DEFAULT NULL,
          `prepayment` double DEFAULT NULL,
          `div_receiv` double DEFAULT NULL,
          `int_receiv` double DEFAULT NULL,
          `inventories` double DEFAULT NULL,
          `amor_exp` double DEFAULT NULL,
          `nca_within_1y` double DEFAULT NULL,
          `sett_rsrv` double DEFAULT NULL,
          `loanto_oth_bank_fi` double DEFAULT NULL,
          `premium_receiv` double DEFAULT NULL,
          `reinsur_receiv` double DEFAULT NULL,
          `reinsur_res_receiv` double DEFAULT NULL,
          `pur_resale_fa` double DEFAULT NULL,
          `oth_cur_assets` double DEFAULT NULL,
          `total_cur_assets` double DEFAULT NULL,
          `fa_avail_for_sale` double DEFAULT NULL,
          `htm_invest` double DEFAULT NULL,
          `lt_eqt_invest` double DEFAULT NULL,
          `invest_real_estate` double DEFAULT NULL,
          `time_deposits` double DEFAULT NULL,
          `oth_assets` double DEFAULT NULL,
          `lt_rec` double DEFAULT NULL,
          `fix_assets` double DEFAULT NULL,
          `cip` double DEFAULT NULL,
          `const_materials` double DEFAULT NULL,
          `fixed_assets_disp` double DEFAULT NULL,
          `produc_bio_assets` double DEFAULT NULL,
          `oil_and_gas_assets` double DEFAULT NULL,
          `intan_assets` double DEFAULT NULL,
          `r_and_d` double DEFAULT NULL,
          `goodwill` double DEFAULT NULL,
          `lt_amor_exp` double DEFAULT NULL,
          `defer_tax_assets` double DEFAULT NULL,
          `decr_in_disbur` double DEFAULT NULL,
          `oth_nca` double DEFAULT NULL,
          `total_nca` double DEFAULT NULL,
          `cash_reser_cb` double DEFAULT NULL,
          `depos_in_oth_bfi` double DEFAULT NULL,
          `prec_metals` double DEFAULT NULL,
          `deriv_assets` double DEFAULT NULL,
          `rr_reins_une_prem` double DEFAULT NULL,
          `rr_reins_outstd_cla` double DEFAULT NULL,
          `rr_reins_lins_liab` double DEFAULT NULL,
          `rr_reins_lthins_liab` double DEFAULT NULL,
          `refund_depos` double DEFAULT NULL,
          `ph_pledge_loans` double DEFAULT NULL,
          `refund_cap_depos` double DEFAULT NULL,
          `indep_acct_assets` double DEFAULT NULL,
          `client_depos` double DEFAULT NULL,
          `client_prov` double DEFAULT NULL,
          `transac_seat_fee` double DEFAULT NULL,
          `invest_as_receiv` double DEFAULT NULL,
          `total_assets` double DEFAULT NULL,
          `lt_borr` double DEFAULT NULL,
          `st_borr` double DEFAULT NULL,
          `cb_borr` double DEFAULT NULL,
          `depos_ib_deposits` double DEFAULT NULL,
          `loan_oth_bank` double DEFAULT NULL,
          `trading_fl` double DEFAULT NULL,
          `notes_payable` double DEFAULT NULL,
          `acct_payable` double DEFAULT NULL,
          `adv_receipts` double DEFAULT NULL,
          `sold_for_repur_fa` double DEFAULT NULL,
          `comm_payable` double DEFAULT NULL,
          `payroll_payable` double DEFAULT NULL,
          `taxes_payable` double DEFAULT NULL,
          `int_payable` double DEFAULT NULL,
          `div_payable` double DEFAULT NULL,
          `oth_payable` double DEFAULT NULL,
          `acc_exp` double DEFAULT NULL,
          `deferred_inc` double DEFAULT NULL,
          `st_bonds_payable` double DEFAULT NULL,
          `payable_to_reinsurer` double DEFAULT NULL,
          `rsrv_insur_cont` double DEFAULT NULL,
          `acting_trading_sec` double DEFAULT NULL,
          `acting_uw_sec` double DEFAULT NULL,
          `non_cur_liab_due_1y` double DEFAULT NULL,
          `oth_cur_liab` double DEFAULT NULL,
          `total_cur_liab` double DEFAULT NULL,
          `bond_payable` double DEFAULT NULL,
          `lt_payable` double DEFAULT NULL,
          `specific_payables` double DEFAULT NULL,
          `estimated_liab` double DEFAULT NULL,
          `defer_tax_liab` double DEFAULT NULL,
          `defer_inc_non_cur_liab` double DEFAULT NULL,
          `oth_ncl` double DEFAULT NULL,
          `total_ncl` double DEFAULT NULL,
          `depos_oth_bfi` double DEFAULT NULL,
          `deriv_liab` double DEFAULT NULL,
          `depos` double DEFAULT NULL,
          `agency_bus_liab` double DEFAULT NULL,
          `oth_liab` double DEFAULT NULL,
          `prem_receiv_adva` double DEFAULT NULL,
          `depos_received` double DEFAULT NULL,
          `ph_invest` double DEFAULT NULL,
          `reser_une_prem` double DEFAULT NULL,
          `reser_outstd_claims` double DEFAULT NULL,
          `reser_lins_liab` double DEFAULT NULL,
          `reser_lthins_liab` double DEFAULT NULL,
          `indept_acc_liab` double DEFAULT NULL,
          `pledge_borr` double DEFAULT NULL,
          `indem_payable` double DEFAULT NULL,
          `policy_div_payable` double DEFAULT NULL,
          `total_liab` double DEFAULT NULL,
          `treasury_share` double DEFAULT NULL,
          `ordin_risk_reser` double DEFAULT NULL,
          `forex_differ` double DEFAULT NULL,
          `invest_loss_unconf` double DEFAULT NULL,
          `minority_int` double DEFAULT NULL,
          `total_hldr_eqy_exc_min_int` double DEFAULT NULL,
          `total_hldr_eqy_inc_min_int` double DEFAULT NULL,
          `total_liab_hldr_eqy` double DEFAULT NULL,
          `lt_payroll_payable` double DEFAULT NULL,
          `oth_comp_income` double DEFAULT NULL,
          `oth_eqt_tools` double DEFAULT NULL,
          `oth_eqt_tools_p_shr` double DEFAULT NULL,
          `lending_funds` double DEFAULT NULL,
          `acc_receivable` double DEFAULT NULL,
          `st_fin_payable` double DEFAULT NULL,
          `payables` double DEFAULT NULL,
          `hfs_assets` double DEFAULT NULL,
          `hfs_sales` double DEFAULT NULL,
          `cost_fin_assets` double DEFAULT NULL,
          `fair_value_fin_assets` double DEFAULT NULL,
          `contract_assets` double DEFAULT NULL,
          `contract_liab` double DEFAULT NULL,
          `accounts_receiv_bill` double DEFAULT NULL,
          `accounts_pay` double DEFAULT NULL,
          `oth_rcv_total` double DEFAULT NULL,
          `fix_assets_total` double DEFAULT NULL,
          `cip_total` double DEFAULT NULL,
          `oth_pay_total` double DEFAULT NULL,
          `long_pay_total` double DEFAULT NULL,
          `debt_invest` double DEFAULT NULL,
          `oth_debt_invest` double DEFAULT NULL,
          `update_flag` double DEFAULT NULL,
          UNIQUE KEY `balance_sheet_code_IDX` (`ts_code`,`end_date`,`report_type`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8

        """
        return sql

    def data_task(self):
        # now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "end_date"
        func = api.balancesheet_vip
        unique_keys = ["ts_code", "end_date", "report_type"]
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,cost_fin_assets,fair_value_fin_assets,cip_total,oth_pay_total,long_pay_total,debt_invest,oth_debt_invest,oth_eq_invest,oth_illiq_fin_assets,oth_eq_ppbond,receiv_financing,use_right_assets,lease_liab,contract_assets,contract_liab,accounts_receiv_bill,accounts_pay,oth_rcv_total,fix_assets_total,update_flag"

        self.fetch_data_by_quarter(
            col_name,
            func,
            self.table,
            unique_keys=unique_keys,
            fields=fields,
            report_type=list(range(1, 13)),
        )


class PrefixDataCashFlow(DataTask):
    table: str = "asharecashflow"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据A股现金流量表
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table}  (
          `ts_code` varchar(9) DEFAULT NULL,
          `ann_date` varchar(8) DEFAULT NULL,
          `f_ann_date` varchar(8) DEFAULT NULL,
          `end_date` varchar(8) DEFAULT NULL,
          `comp_type` varchar(2) DEFAULT NULL,
          `report_type` varchar(2) DEFAULT NULL,
          `end_type` varchar(2) DEFAULT NULL,
          `net_profit` double DEFAULT NULL,
          `finan_exp` double DEFAULT NULL,
          `c_fr_sale_sg` double DEFAULT NULL,
          `recp_tax_rends` double DEFAULT NULL,
          `n_depos_incr_fi` double DEFAULT NULL,
          `n_incr_loans_cb` double DEFAULT NULL,
          `n_inc_borr_oth_fi` double DEFAULT NULL,
          `prem_fr_orig_contr` double DEFAULT NULL,
          `n_incr_insured_dep` double DEFAULT NULL,
          `n_reinsur_prem` double DEFAULT NULL,
          `n_incr_disp_tfa` double DEFAULT NULL,
          `ifc_cash_incr` double DEFAULT NULL,
          `n_incr_disp_faas` double DEFAULT NULL,
          `n_incr_loans_oth_bank` double DEFAULT NULL,
          `n_cap_incr_repur` double DEFAULT NULL,
          `c_fr_oth_operate_a` double DEFAULT NULL,
          `c_inf_fr_operate_a` double DEFAULT NULL,
          `c_paid_goods_s` double DEFAULT NULL,
          `c_paid_to_for_empl` double DEFAULT NULL,
          `c_paid_for_taxes` double DEFAULT NULL,
          `n_incr_clt_loan_adv` double DEFAULT NULL,
          `n_incr_dep_cbob` double DEFAULT NULL,
          `c_pay_claims_orig_inco` double DEFAULT NULL,
          `pay_handling_chrg` double DEFAULT NULL,
          `pay_comm_insur_plcy` double DEFAULT NULL,
          `oth_cash_pay_oper_act` double DEFAULT NULL,
          `st_cash_out_act` double DEFAULT NULL,
          `n_cashflow_act` double DEFAULT NULL,
          `oth_recp_ral_inv_act` double DEFAULT NULL,
          `c_disp_withdrwl_invest` double DEFAULT NULL,
          `c_recp_return_invest` double DEFAULT NULL,
          `n_recp_disp_fiolta` double DEFAULT NULL,
          `n_recp_disp_sobu` double DEFAULT NULL,
          `stot_inflows_inv_act` double DEFAULT NULL,
          `c_pay_acq_const_fiolta` double DEFAULT NULL,
          `c_paid_invest` double DEFAULT NULL,
          `n_disp_subs_oth_biz` double DEFAULT NULL,
          `oth_pay_ral_inv_act` double DEFAULT NULL,
          `n_incr_pledge_loan` double DEFAULT NULL,
          `stot_out_inv_act` double DEFAULT NULL,
          `n_cashflow_inv_act` double DEFAULT NULL,
          `c_recp_borrow` double DEFAULT NULL,
          `proc_issue_bonds` double DEFAULT NULL,
          `oth_cash_recp_ral_fnc_act` double DEFAULT NULL,
          `stot_cash_in_fnc_act` double DEFAULT NULL,
          `free_cashflow` double DEFAULT NULL,
          `c_prepay_amt_borr` double DEFAULT NULL,
          `c_pay_dist_dpcp_int_exp` double DEFAULT NULL,
          `incl_dvd_profit_paid_sc_ms` double DEFAULT NULL,
          `oth_cashpay_ral_fnc_act` double DEFAULT NULL,
          `stot_cashout_fnc_act` double DEFAULT NULL,
          `n_cash_flows_fnc_act` double DEFAULT NULL,
          `eff_fx_flu_cash` double DEFAULT NULL,
          `n_incr_cash_cash_equ` double DEFAULT NULL,
          `c_cash_equ_beg_period` double DEFAULT NULL,
          `c_cash_equ_end_period` double DEFAULT NULL,
          `c_recp_cap_contrib` double DEFAULT NULL,
          `incl_cash_rec_saims` double DEFAULT NULL,
          `uncon_invest_loss` double DEFAULT NULL,
          `prov_depr_assets` double DEFAULT NULL,
          `depr_fa_coga_dpba` double DEFAULT NULL,
          `amort_intang_assets` double DEFAULT NULL,
          `lt_amort_deferred_exp` double DEFAULT NULL,
          `decr_deferred_exp` double DEFAULT NULL,
          `incr_acc_exp` double DEFAULT NULL,
          `loss_disp_fiolta` double DEFAULT NULL,
          `loss_scr_fa` double DEFAULT NULL,
          `loss_fv_chg` double DEFAULT NULL,
          `invest_loss` double DEFAULT NULL,
          `decr_def_inc_tax_assets` double DEFAULT NULL,
          `incr_def_inc_tax_liab` double DEFAULT NULL,
          `decr_inventories` double DEFAULT NULL,
          `decr_oper_payable` double DEFAULT NULL,
          `incr_oper_payable` double DEFAULT NULL,
          `others` double DEFAULT NULL,
          `im_net_cashflow_oper_act` double DEFAULT NULL,
          `conv_debt_into_cap` double DEFAULT NULL,
          `conv_copbonds_due_within_1y` double DEFAULT NULL,
          `fa_fnc_leases` double DEFAULT NULL,
          `im_n_incr_cash_equ` double DEFAULT NULL,
          `net_dism_capital_add` double DEFAULT NULL,
          `net_cash_rece_sec` double DEFAULT NULL,
          `credit_impa_loss` double DEFAULT NULL,
          `use_right_asset_dep` double DEFAULT NULL,
          `oth_loss_asset` double DEFAULT NULL,
          `end_bal_cash` double DEFAULT NULL,
          `beg_bal_cash` double DEFAULT NULL,
          `end_bal_cash_equ` double DEFAULT NULL,
          `beg_bal_cash_equ` double DEFAULT NULL,
          `update_flag` double DEFAULT NULL,
          UNIQUE KEY `{self.table}_code_IDX` (`ts_code`,`end_date`,`report_type`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        return sql

    def data_task(self):
        # now = dt.now()
        # if now.weekday() != 5:  # 只在周六运行，非周六直接返回
        #     return
        col_name = "end_date"
        func = api.cashflow_vip
        unique_keys = ["ts_code", "end_date", "report_type"]
        fields = "ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,end_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,im_n_incr_cash_equ,net_dism_capital_add,net_cash_rece_sec,credit_impa_loss,use_right_asset_dep,oth_loss_asset,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,update_flag"

        self.fetch_data_by_quarter(
            col_name,
            func,
            self.table,
            unique_keys=unique_keys,
            fields=fields,
            report_type=list(range(1, 13)),
        )


class PrefixDataMutualFundBasic(DataTask):
    table: str = "asharemutualfundbasic"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据公募基金基本信息下载
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.weekly.value

    def sql_task(self):
        # code varchar(20) 是因为有部分基金信息code长度超过9
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
          `ts_code` varchar(20),
          `name` varchar(128),
          `management` varchar(128),
          `custodian` varchar(128),
          `fund_type` varchar(20),
          `found_date` varchar(8),
          `due_date` varchar(8),
          `list_date` varchar(8),
          `issue_date` varchar(8),
          `delist_date` varchar(8),
          `issue_amount` double DEFAULT NULL,
          `m_fee` double DEFAULT NULL,
          `c_fee` double DEFAULT NULL,
          `duration_year` double DEFAULT NULL,
          `p_value` double DEFAULT NULL,
          `min_amount` double DEFAULT NULL,
          `exp_return` double DEFAULT NULL,
          `benchmark` varchar(512),
          `status` varchar(1),
          `invest_type` varchar(20),
          `type` varchar(20),
          `trustee` varchar(128),
          `purc_startdate` varchar(8),
          `redm_startdate` varchar(8),
          `market` varchar(1)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql

    def data_task(self):
        e_df = api.fund_basic(market="E")
        out_df = api.fund_basic(market="O")
        df = pd.concat([e_df, out_df])
        df.drop_duplicates(subset=["ts_code"], keep="first", inplace=True)
        df.sort_values(by=["list_date"], inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, df, self.scheme)


class PrefixDataMutualFundDaily(DataTask):
    table: str = "asharemutualfunddaily"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据公募基金场内基金日线行情
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        # code varchar(20) 是因为有部分基金信息code长度超过9
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
          `ts_code` varchar(20),
          `trade_date` varchar(8),
          `pre_close` double DEFAULT NULL,
          `open` double DEFAULT NULL,
          `high` double DEFAULT NULL,
          `low` double DEFAULT NULL,
          `close` double DEFAULT NULL,
          `change` double DEFAULT NULL,
          `pct_chg` double DEFAULT NULL,
          `vol` double DEFAULT NULL,
          `amount` double DEFAULT NULL,
           UNIQUE KEY `asharemutualfunddaily_code_date_IDX` (`trade_date`,`ts_code`) USING BTREE,
           KEY `asharemutualfunddaily_ts_code_IDX` (`ts_code`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.fund_daily
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataMutualFundNavAdjFactor(DataTask):
    table: str = "mutualfundadjfactor"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据基金复权因子
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table} (
              `ts_code` varchar(9) DEFAULT NULL,
              `trade_date` varchar(8) DEFAULT NULL,
              `adj_factor` double DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.fund_adj
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataMutualFundNav(DataTask):
    table: str = "asharemutualfundnav"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据获取公募基金净值数据
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        # code varchar(20) 是因为有部分基金信息code长度超过9
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
          `ts_code` varchar(20),
          `ann_date` varchar(8),
          `nav_date` varchar(8),
          `unit_nav` double DEFAULT NULL,
          `accum_nav` double DEFAULT NULL,
          `accum_div` double DEFAULT NULL,
          `net_asset` double DEFAULT NULL,
          `total_netasset` double DEFAULT NULL,
          `adj_nav` double DEFAULT NULL,
          `update_flag` varchar(1) DEFAULT NULL,
           KEY `{self.table}_code_date_IDX` (`nav_date`, `ts_code`) USING BTREE,
           KEY `{self.table}_ts_code_IDX` (`ts_code`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        return sql

    def data_task(self):
        col_name = "nav_date"
        func = api.fund_nav
        unique_keys = ["ts_code", "nav_date"]
        self.fetch_data_by_day(
            col_name, func, self.table, unique_keys=unique_keys, filter_key="nav_date"
        )


class PrefixDataTHSIndexBasic(DataTask):
    table: str = "ths_index_basic"
    scheme: str = "stock"
    description: str = """
            Tushare量化数据同花顺概念和行业指数下载
        """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists stock.{self.table} (
            ts_code varchar(9) NOT NULL,
            name varchar(12) NULL,
            count DOUBLE NULL,
            exchange varchar(2) NULL,
            list_date varchar(8) NULL,
            `type` varchar(1) NULL
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8
        COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        df = api.ths_index()
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, df, self.scheme)


class PrefixDataTHSIndexDaily(DataTask):
    table: str = "ths_index_daily"
    scheme: str = "stock"
    description: str = """
            Tushare量化数据同花顺板块指数行情数据下载
        """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
            ts_code varchar(9) NOT NULL,
            trade_date varchar(8) NOT NULL,
            `close` DOUBLE NULL,
            `open` DOUBLE NULL,
            high DOUBLE NULL,
            low DOUBLE NULL,
            pre_close DOUBLE NULL,
            avg_price DOUBLE NULL,
            `change` FLOAT NULL,
            pct_change DOUBLE NULL,
            vol DOUBLE NULL,
            turnover_rate FLOAT NULL
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8
        COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        col_name = "trade_date"
        func = api.ths_daily
        unique_keys = ["ts_code", "trade_date"]
        self.fetch_data_by_day(col_name, func, self.table, unique_keys=unique_keys)


class PrefixDataTHSMember(DataTask):
    table: str = "ths_member"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据同花顺板块指数行情数据下载
    """
    parents: list = ["ths_index_basic"]
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
            ts_code varchar(9) NOT NULL,
            code varchar(9) NOT NULL,
            name varchar(6) NULL,
            create_time varchar(20) NOT NULL
        )
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8
        COLLATE=utf8_general_ci;
        """
        return sql

    def data_task(self):
        df = api.ths_index()
        codes = set(df.ts_code.tolist())
        now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        for ts_code in codes:
            df_member = api.ths_member(ts_code=ts_code)
            df_member["create_time"] = now
            db.insert_dataframe(self.table, df_member, self.scheme)
            time.sleep(0.35)


class PrefixDataIndexDaily(DataTask):
    table: str = "ashareindexeod"
    scheme: str = "stock"
    description: str = """
        Tushare量化数据指数行情数据下载
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
        CREATE TABLE if not exists {self.scheme}.{self.table} (
          `ts_code` varchar(9) DEFAULT NULL,
          `trade_date` varchar(9) DEFAULT NULL,
          `close` double DEFAULT NULL,
          `open` double DEFAULT NULL,
          `high` double DEFAULT NULL,
          `low` double DEFAULT NULL,
          `pre_close` double DEFAULT NULL,
          `change` double DEFAULT NULL,
          `pct_chg` double DEFAULT NULL,
          `vol` double DEFAULT NULL,
          `amount` double DEFAULT NULL,
          UNIQUE KEY `ashareindexeod_ts_code_IDX` (`ts_code`,`trade_date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        return sql

    def data_task(self):
        today = dt.now().strftime("%Y%m%d")
        index_code_list = [
            "399106.SZ",
            "000001.SH",
            "000300.SH",
            "000905.SH",
            "000906.SH",
            "000852.SH",
        ]
        db.truncate(self.table, self.scheme)

        for ts_code in index_code_list:
            df = api.index_daily(ts_code=ts_code, start_date="19900101", end_date=today)
            db.insert_dataframe(self.table, df, self.scheme)
            time.sleep(0.35)


class PrefixDataListDayDaily(DataTask):
    table: str = "ListDay"  # 本数据存在redis中，实际是key，但是这里也使用table
    scheme: str = ""
    description: str = """
        Tushare量化数据每日股票上市存续周期日矩阵
    """
    parents: list = ["asharecalendar", "stock_basic"]
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f""""""
        return sql

    def data_task(self):

        sql = f"""
            select * from asharecalendar
            where tradedays >= '{Config.START_DATE}'
        """
        trade_days = db.read_sql(sql)
        trade_days.set_index("tradedays", inplace=True)
        # trade_days = trade_days[-20:]

        print("trade_days:", trade_days)
        df = db.read_sql(
            """
            select * from stock_basic
        """
        )
        df = df.rename(columns={"list_date": "ipo_date"})
        df = df.rename(columns={"name": "sec_name"})
        df = df.rename(columns={"ts_code": "code"})
        df.drop_duplicates(subset=["code"], keep="first", inplace=True)
        df.sort_values(by=["ipo_date"], inplace=True)
        df.set_index(["code"], inplace=True)
        print("df：", df)
        meta = df
        listday_dat = pd.DataFrame(index=meta.index, columns=trade_days.index)
        print("listday_dat：", listday_dat)

        def if_listed(series):
            code = series.name
            ipo_date = meta.at[code, "ipo_date"]
            delist_date = meta.at[code, "delist_date"]
            daterange = series.index
            if delist_date is None or delist_date is pd.NaT:
                res = np.where(daterange >= ipo_date, 1, 0)
            else:
                res = np.where(
                    daterange < ipo_date, 0, np.where(daterange <= delist_date, 1, 0)
                )
            print("sub-res:", res)
            return pd.Series(res, index=series.index)

        res = listday_dat.apply(if_listed, axis=1)
        print("res:", res)
        redis_con.write(self.table, res)


class PrefixDataMonthBeginEndMap(DataTask):
    table: str = "trade_days_begin_end_of_month"
    scheme: str = "stock"
    description: str = """
            每月第一个和最后一个交易日映射
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                month_start varchar(8) NULL,
                month_end varchar(8) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        tdays = db.read_sql(f"select tradedays, is_open from {self.parents[0]}")
        tdays["tradedays"] = pd.to_datetime(tdays["tradedays"])
        tdays.set_index("tradedays", inplace=True)
        tdays["trade_date"] = tdays.index
        start_dates = tdays.resample("m").first()
        end_dates = tdays.resample("m").last()
        res = pd.DataFrame(columns=["month_start", "month_end"])
        res["month_start"] = start_dates.trade_date.tolist()
        res["month_end"] = end_dates.trade_date.tolist()
        res = res.applymap(lambda x: x.strftime("%Y%m%d"))
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, res, self.scheme)


@jit(nopython=True)
def make_industry(
    index_code_list, stock_code_list, in_date_list, out_date_list, is_new_list, dates
):
    """
    构造行业数据
    :param index_code_list: 指数代码
    :param stock_code_list: 股票代码
    :param in_date_list: 入选行业的日期
    :param out_date_list: 剔出行业的日期
    :param is_new_list: 是否最新
    :param dates: 需要同步的交易日
    :return:
    """
    res = []
    size = len(index_code_list)
    print("size:", size)
    for date in dates:
        for i in range(size):
            index_code = index_code_list[i]
            stock_code = stock_code_list[i]
            in_date = in_date_list[i]
            out_date = out_date_list[i]
            is_new = is_new_list[i]
            if in_date <= date <= out_date:
                res.append([stock_code, index_code, date, is_new])
    return res


############################### 注意事项 ####################################
#   看视频的同学，你看到的代码和视频里面可能会有不同，那是因为课程录制之后，代码又
#   做了调整，新增加了更多的任务，这是正常现象，我们的任务结构总体没有变化，不会影响
#   课程的学习。
############################################################################


class PrefixDataSWIndustryL1(DataTask):
    table: str = "sw_industry"
    scheme: str = "stock"
    description: str = """
        每日申万一级行业分类成分
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                stock_code varchar(9) NULL,
                class_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                is_new varchar(8) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        sw_lv01 = api.index_classify(level="L1", src="SW2021")
        fields = "index_code, index_name,con_code,con_name,in_date,out_date, is_new"
        res = pd.DataFrame()
        for code in sw_lv01.index_code:
            ind_df_con = api.index_member(index_code=code, fields=fields)
            res = pd.concat([res, ind_df_con])
        res_temp = res.fillna("99999999")
        data = res_temp.to_dict("list")

        calendar_date_col = "tradedays"
        trade_date_sql = f"""
            select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
            and {calendar_date_col}<={Config.TODAY}
        """
        trade_dates = db.read_sql(trade_date_sql)
        dates_in_db = db.read_sql(f"select distinct trade_date from {self.table}")[
            "trade_date"
        ].tolist()
        dates_to_download = list(
            set(trade_dates[calendar_date_col].tolist()).difference(set(dates_in_db))
        )
        dates_to_download.sort()
        print("dates_to_download:", dates_to_download)
        group_size = 10
        if len(dates_to_download) == 0:
            return
        for i in range(0, len(data["index_code"]), group_size):
            res = make_industry(
                nb.typed.List(data["index_code"][i : i + group_size]),
                nb.typed.List(data["con_code"][i : i + group_size]),
                nb.typed.List(data["in_date"][i : i + group_size]),
                nb.typed.List(data["out_date"][i : i + group_size]),
                nb.typed.List(data["is_new"][i : i + group_size]),
                nb.typed.List(dates_to_download),
            )
            industry_df = pd.DataFrame(
                res, columns=["stock_code", "class_code", "trade_date", "is_new"]
            )
            db.insert_dataframe(self.table, industry_df, self.scheme)


class PrefixDataSWIndustryClassifyL1(DataTask):
    table: str = "sw_industry_classify_lv1"
    scheme: str = "stock"
    description: str = """
        每日申万一级行业分类
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                index_code varchar(9) NULL,
                industry_name varchar(9) NULL,
                level_name varchar(2) NULL,
                industry_code varchar(6) NULL,
                is_pub varchar(1) NULL,
                parent_code varchar(6) NULL,
                src varchar(6) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        sw_lv01 = api.index_classify(level="L1", src="SW2021")
        sw_lv01.rename(columns={"level": "level_name"}, inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, sw_lv01, self.scheme)


class PrefixDataSWIndustryClassifyL2(DataTask):
    table: str = "sw_industry_classify_lv2"
    scheme: str = "stock"
    description: str = """
        每日申万二级行业分类
    """
    parents: list = None
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                index_code varchar(9) NULL,
                industry_name varchar(9) NULL,
                level_name varchar(2) NULL,
                industry_code varchar(6) NULL,
                is_pub varchar(1) NULL,
                parent_code varchar(6) NULL,
                src varchar(6) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        sw_lv02 = api.index_classify(level="L2", src="SW2021")
        sw_lv02.rename(columns={"level": "level_name"}, inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, sw_lv02, self.scheme)


class PrefixDataSWIndustryClassifyL3(DataTask):
    table: str = "sw_industry_classify_lv3"
    scheme: str = "stock"
    description: str = """
        每日申万三级行业分类
    """
    parents: list = None
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                index_code varchar(9) NULL,
                industry_name varchar(9) NULL,
                level_name varchar(2) NULL,
                industry_code varchar(6) NULL,
                is_pub varchar(1) NULL,
                parent_code varchar(6) NULL,
                src varchar(6) NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        sw_lv03 = api.index_classify(level="L3", src="SW2021")
        sw_lv03.rename(columns={"level": "level_name"}, inplace=True)
        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, sw_lv03, self.scheme)


class PrefixDataSWIndustryL2(DataTask):
    table: str = "sw_industry_lv2"
    scheme: str = "stock"
    description: str = """
        每日申万二级行业分类成分
    """
    parents: list = ["asharecalendar"]
    turn_on: bool = False
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                stock_code varchar(9) NULL,
                class_code varchar(9) NULL,
                trade_date varchar(8) NULL
                
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        sw_lv02 = api.index_classify(level="L2", src="SW2021")
        res = pd.DataFrame()

        for code in sw_lv02.index_code:
            ind_df_con = api.index_member(index_code=code)
            res = pd.concat([res, ind_df_con])
        res_temp = res.fillna("99999999")
        data = res_temp.to_dict("list")

        calendar_date_col = "tradedays"
        trade_date_sql = f"""
            select {calendar_date_col} from {self.parents[0]} where {calendar_date_col}>={Config.START_DATE}
            and {calendar_date_col}<={Config.TODAY}
        """
        trade_dates = db.read_sql(trade_date_sql)
        dates_in_db = db.read_sql(f"select distinct trade_date from {self.table}")[
            "trade_date"
        ].tolist()
        dates_to_download = list(
            set(trade_dates[calendar_date_col].tolist()).difference(set(dates_in_db))
        )
        dates_to_download.sort()
        print("dates_to_download:", dates_to_download)
        group_size = 10
        if len(dates_to_download) == 0:
            return
        for i in range(0, len(data["index_code"]), group_size):
            res = make_industry(
                nb.typed.List(data["index_code"][i : i + group_size]),
                nb.typed.List(data["con_code"][i : i + group_size]),
                nb.typed.List(data["in_date"][i : i + group_size]),
                nb.typed.List(data["out_date"][i : i + group_size]),
                nb.typed.List(dates_to_download),
            )
            industry_df = pd.DataFrame(
                res, columns=["stock_code", "class_code", "trade_date"]
            )
            db.insert_dataframe(self.table, industry_df, self.scheme)


class PrefixDataIndexConstZZ500(DataTask):
    table: str = "zz500_const"
    scheme: str = "stock"
    description: str = """
        中证500指数成分股
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                index_code varchar(9) NULL,
                con_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                weight double NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        today = dt.now().strftime("%Y%m%d")
        # trade_dates = pd.date_range(Config.START_DATE, today, freq='M')
        calendar = data_api_obj.get_calendar()
        rebalance_mgr = RebalanceDatesManager(calendar, Config.START_DATE, today)
        trade_dates = rebalance_mgr.parse_rebalance_freq(
            "每月最后", "每月最后第1个交易日", exclude=False
        )

        # trade_dates = [date.strftime('%Y%m%d') for date in trade_dates]

        dates_in_db = db.read_sql(f"select distinct trade_date from {self.table}")[
            "trade_date"
        ].tolist()
        dates_to_download = list(set(trade_dates).difference(set(dates_in_db)))
        dates_to_download.sort()
        for date in dates_to_download:
            df = api.index_weight(
                index_code="000905.SH", start_date=date, end_date=date
            )
            db.insert_dataframe(self.table, df, self.scheme)


class PrefixDataIndexConstHS300(DataTask):
    table: str = "hs300_const"
    scheme: str = "stock"
    description: str = """
        沪深300指数成分股
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                index_code varchar(9) NULL,
                con_code varchar(9) NULL,
                trade_date varchar(8) NULL,
                weight double NULL
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        today = dt.now().strftime("%Y%m%d")
        calendar = data_api_obj.get_calendar()
        # trade_dates = pd.date_range(Config.START_DATE, today, freq='M')
        rebalance_mgr = RebalanceDatesManager(calendar, Config.START_DATE, today)
        trade_dates = rebalance_mgr.parse_rebalance_freq(
            "每月最后", "每月最后第1个交易日", exclude=False
        )
        # trade_dates = [date.strftime('%Y%m%d') for date in trade_dates]

        dates_in_db = db.read_sql(f"select distinct trade_date from {self.table}")[
            "trade_date"
        ].tolist()
        dates_to_download = list(set(trade_dates).difference(set(dates_in_db)))
        dates_to_download.sort()
        for date in dates_to_download:
            df = api.index_weight(
                index_code="399300.SZ", start_date=date, end_date=date
            )
            db.insert_dataframe(self.table, df, self.scheme)


# class PrefixDataDailyTurn(DataTask):
#     table: str = 'daily_turn'
#     scheme: str = 'stock'
#     description: str = """
#             A股每日换手率
#     """
#     parents: list = ['asharecalendar']
#     freq = Freq.daily.value
#
#     def sql_task(self):
#         sql = """
#             CREATE TABLE if not exists {self.scheme}.{self.table}(
#                 month_start varchar(8) NULL,
#                 month_end varchar(8) NULL
#             )
#             ENGINE=InnoDB
#             DEFAULT CHARSET=utf8
#             COLLATE=utf8_general_ci
#         """
#         return sql
#
#     def data_task(self):
#         ...


class PrefixDataStockHistoryName(DataTask):
    table: str = "stock_history_name"
    scheme: str = "stock"
    description: str = """
        A股股票历史曾用名
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                name varchar(12) NULL,
                start_date varchar(8) NULL,
                end_date varchar(8) NULL,
                ann_date varchar(8) NULL,
                change_reason varchar(10) NULL
                
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        history_name = api.namechange(
            start_date="19900101",
            end_date=Config.TODAY,
            fields="ts_code,name,start_date,end_date,ann_date,change_reason",
        )

        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, history_name, self.scheme)


class PrefixDataCITIC(DataTask):
    table: str = "stock_history_name"
    scheme: str = "stock"
    description: str = """
        A股股票历史曾用名
    """
    parents: list = None
    turn_on: bool = True
    freq = Freq.daily.value

    def sql_task(self):
        sql = f"""
            CREATE TABLE if not exists {self.scheme}.{self.table}(
                ts_code varchar(9) NULL,
                name varchar(12) NULL,
                start_date varchar(8) NULL,
                end_date varchar(8) NULL,
                ann_date varchar(8) NULL,
                change_reason varchar(10) NULL

            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8
            COLLATE=utf8_general_ci
        """
        return sql

    def data_task(self):
        history_name = api.namechange(
            start_date="19900101",
            end_date=Config.TODAY,
            fields="ts_code,name,start_date,end_date,ann_date,change_reason",
        )

        db.truncate(self.table, self.scheme)
        db.insert_dataframe(self.table, history_name, self.scheme)
