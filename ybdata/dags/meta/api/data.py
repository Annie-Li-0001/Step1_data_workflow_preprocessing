"""
文件说明
"""

from meta.config import Config
from .models import *
from .factor_repository_models import *
from sqlalchemy import select, or_, and_
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import decimal
from typing import Union, List

# 创建数据库连接引擎，这里要换成你自己的数据库连接，输入密码和ip地址
engine_market = create_engine(
    Config.MYSQL_ENGINE_STR,
    max_overflow=5,
    pool_size=10,
    connect_args={"read_timeout": 120},
    pool_pre_ping=True,
    pool_recycle=25200,
)

engine_factor = create_engine(
    Config.MYSQL_ENGINE_FACTOR_STR,
    max_overflow=5,
    pool_size=10,
    connect_args={"read_timeout": 120},
    pool_pre_ping=True,
    pool_recycle=25200,
)
# 创建数据库会话
Session_factor = sessionmaker(bind=engine_factor)
session_factor = Session_factor()
############################################## 分割线 ##################################################

# 创建数据库会话
Session_market = sessionmaker(bind=engine_market)
session_market = Session_market()


class MetaData:
    # 基础类，提供统一的数据查询接口，提供给下游子类接口使用
    def query_db(
        self, sql_ex, debug: bool = False, engine=engine_market
    ) -> pd.DataFrame:
        if debug:
            print(sql_ex)
        with engine.connect() as conn:
            result = conn.execute(sql_ex)
            res_data = pd.DataFrame.from_records(result, columns=result.keys())
            res_data = res_data.applymap(
                lambda x: round(float(x), 2) if isinstance(x, decimal.Decimal) else x
            )
        return res_data

    def query_timeseries_common_data(
        self,
        table,
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        date_col: str = "trade_date",
        fields: List = None,
        schema="stock",
    ) -> pd.DataFrame:
        # table = t_asharecashflow
        if isinstance(codes, str):
            codes = [codes]
        if fields:
            columns = [col.lower() for col in fields]
            if "trade_date" not in columns:
                columns.insert(0, "trade_date")
            if "ts_code" not in columns:
                columns.insert(0, "ts_code")
            query_fields = [
                getattr(table.c, col) for col in columns if hasattr(table.c, col)
            ]
        else:
            query_fields = [table]
        conds = []
        if start_date:
            conds.append(getattr(table.c, date_col) >= start_date)
        if end_date:
            conds.append(getattr(table.c, date_col) <= end_date)
        if codes is not None:
            conds.append(getattr(table.c, "ts_code").in_(codes))
        if len(conds) == 0:

            ex = select(*query_fields).where(and_(*conds)).limit(50)
        else:
            ex = select(*query_fields).where(and_(*conds))
        if schema == "stock":
            res_data = self.query_db(ex)
        elif schema == "factors":
            res_data = self.query_db(ex, engine=engine_factor)
        else:
            raise ValueError("没有提供正确的schema，目前支持factors和stock")
        return res_data


class StockMarketData(MetaData):
    # 股票行情接口
    def get_stock_basic(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_stock_suspend_status(
        self, start_date: str, end_date: str, suspend_type: str
    ):
        raise NotImplementedError()

    def get_longtime_stop(date, periods):
        raise NotImplementedError()

    def get_calendar(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_month_map(self) -> pd.DataFrame:
        raise NotImplementedError()

    def get_ashareeodprice(
        self,
        codes: Union[str, List],
        start_date: str,
        end_date: str,
        price_type="bfq",
        fields: List = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def get_ashareeodbasic(
        self,
        codes: Union[str, List],
        start_date: str,
        end_date: str,
        fields: List = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()


class StockFinanceData(MetaData):
    # 股票财务信息接口
    def get_cashflow(
        self, codes: Union[str, List] = None, start_date=None, end_date=None
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def get_balancesheet(
        self, codes: Union[str, List] = None, start_date=None, end_date=None
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def get_income(
        self, codes: Union[str, List] = None, start_date=None, end_date=None
    ) -> pd.DataFrame:
        raise NotImplementedError()


class IndexData(MetaData):
    # 指数相关数据
    def get_index_price(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        fields: List = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def get_index_const(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        fields: List = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()


class IndustryData(MetaData):
    # 行业相关数据接口
    def get_sw_const(
        self,
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        level: int = 1,
    ):
        raise NotImplementedError()

    def get_sw_industry(self, level: str):
        raise NotImplementedError()


class FactorData(MetaData):
    def __query_factor_location(self, factor_name: Union[str, List]) -> pd.DataFrame:
        """
        本方法用来定位所查询的因子的存储位置
        """
        table = FactorStore.__table__
        factor_name = [factor_name] if isinstance(factor_name, str) else factor_name
        fields = [
            "factor_code",
            "storage_table",
            "database",
            "factor_column_name",
            "date_col",
        ]
        columns = [col.lower() for col in fields]
        query_fields = [table.c[col] for col in columns if col in table.c]
        conds = [table.c.factor_code.in_(factor_name)]

        ex = select(*query_fields).where(and_(*conds))
        res_data = self.query_db(ex, engine=engine_factor)
        return res_data

    def get_factor_data(
        self,
        factor_name: Union[str, List],
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> Union[pd.DataFrame, None]:
        """
        本方法通过因子配置表中的数据，来定位因子的存储位置，然后获取因子的时间序列数据
        **********！！注意，这里要求，所有数据models类都要以t_开头，否则会报错**********
        """
        query_result: pd.DataFrame = self.__query_factor_location(factor_name)
        if query_result.empty:
            return None
        data_array = []
        date_col = None
        for _, row in query_result.iterrows():
            database = row.loc["database"]
            storage_table = row.loc["storage_table"]
            factor_colume = row.loc["factor_column_name"]
            date_col = row.loc["date_col"]

            table_obj = globals()[
                f"t_{storage_table}"
            ]  # 注意，这里要求，所有数据models类都要以t_开头，否则会报错
            factor_df = self.query_timeseries_common_data(
                table_obj,
                codes=codes,
                start_date=start_date,
                end_date=end_date,
                date_col=date_col,
                fields=[date_col, "ts_code", factor_colume],
                schema=database,
            )
            data_array.append(factor_df)
        result_df = pd.concat(
            [df.set_index([date_col, "ts_code"]) for df in data_array], axis=1
        )
        return result_df


class TuMarketData(StockMarketData):
    # 行情数据接口类
    def get_stock_basic(self) -> pd.DataFrame:
        table = t_stock_basic
        query_fields = [table]

        ex = select(*query_fields)
        res_data = self.query_db(ex)

        return res_data

    def get_history_name(self) -> pd.DataFrame:
        table = t_stock_history_name
        query_fields = [table]

        ex = select(*query_fields)
        res_data = self.query_db(ex)

        return res_data

    def get_stock_suspend_status(
        self, start_date: str = None, end_date: str = None, suspend_type: str = "S"
    ):
        table = t_asharesuspend
        conds = [getattr(table.c, "suspend_type") == suspend_type]
        if start_date:
            conds.append(getattr(table.c, "trade_date") >= start_date)
        if end_date:
            conds.append(getattr(table.c, "trade_date") <= end_date)

        columns = ["ts_code", "trade_date", "suspend_type"]
        if "trade_date" not in columns:
            columns.insert(0, "trade_date")
        if "ts_code" not in columns:
            columns.insert(0, "ts_code")
        query_fields = [
            getattr(table.c, col) for col in columns if hasattr(table.c, col)
        ]
        ex = select(*query_fields).where(and_(*conds))
        res_data = self.query_db(ex)

        return res_data

    def get_longtime_stop(self, date, periods):
        """
        获取某一段时间内停牌的天数
        params: date, 某一个横截面
        params: periods, 以date为截止，往前计算的交易日数量
        使用方式：get_longtime_stop('20221230', 252)
        """
        calendar_df = self.get_calendar()
        algo_start_date = (
            calendar_df[calendar_df.tradedays <= date]
            .iloc[-1 * periods]
            .loc["tradedays"]
        )
        suspend_df = self.get_stock_suspend_status(
            start_date=algo_start_date, end_date=date
        )

        suspend_days_df = suspend_df.groupby(by="ts_code").apply(
            lambda x: len(x.suspend_type)
        )
        suspend_days_df = suspend_days_df.to_frame("suspend_count")
        suspend_days_df.sort_values("suspend_count", ascending=False)
        return suspend_days_df

    def get_calendar(self) -> pd.DataFrame:
        table = t_asharecalendar
        query_fields = [table]

        ex = select(*query_fields)
        res_data = self.query_db(ex)

        return res_data

    def get_month_map(self) -> pd.DataFrame:
        table = t_month_map
        query_fields = [table]

        ex = select(*query_fields)
        res_data = self.query_db(ex)

        return res_data

    def get_ashareeodbasic(
        self,
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        fields: List = None,
    ) -> pd.DataFrame:
        res_data = self.query_timeseries_common_data(
            t_ashareeodbasic, codes, start_date, end_date, fields=fields
        )
        return res_data

    def get_ashareeodprice(
        self,
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        price_type="bfq",
        fields: List = None,
    ) -> pd.DataFrame:
        """
        接口:查询A股市场行情数据
        """
        if fields:
            fields = [field.lower() for field in fields]
        res_data = self.query_timeseries_common_data(
            t_ashareeodprice, codes, start_date, end_date, fields=fields
        )
        raw_columns = res_data.columns.tolist()
        cond01 = not fields
        cond02 = fields and (
            "open" in fields or "high" in fields or "low" in fields or "close" in fields
        )
        if (price_type == "hfq" and cond02) or (price_type == "hfq" and cond01):
            adj_factor_data = self.query_timeseries_common_data(
                t_ashareadjfactor, codes, start_date, end_date
            )
            com = pd.concat(
                [
                    res_data.set_index(["ts_code", "trade_date"]),
                    adj_factor_data.set_index(["ts_code", "trade_date"]),
                ],
                axis=1,
            )
            for price_type in ["open", "high", "low", "close"]:
                if price_type in com.columns:
                    com[f"hfq_{price_type}"] = com["adj_factor"] * com[price_type]
                    com.drop(price_type, axis=1, inplace=True)

            com.drop("adj_factor", axis=1, inplace=True)
            com.reset_index(inplace=True)
            com.rename(
                columns={
                    "hfq_open": "open",
                    "hfq_high": "high",
                    "hfq_low": "low",
                    "hfq_close": "close",
                    "trade_date_x": "trade_date",
                },
                inplace=True,
            )
            com = com[raw_columns]
            res_data = com
        for col in [
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ]:
            if col in res_data.columns:
                res_data[col] = res_data[col].apply(lambda x: round(float(x), 2))
        return res_data


class TuStockFinanceData(StockFinanceData):
    def get_cashflow(
        self,
        codes: Union[str, List] = None,
        start_date=None,
        end_date=None,
        date_col="end_date",
    ) -> pd.DataFrame:
        res_data = self.query_timeseries_common_data(
            t_asharecashflow, codes, start_date, end_date, date_col
        )
        return res_data

    def get_balancesheet(
        self,
        codes: Union[str, List] = None,
        start_date=None,
        end_date=None,
        date_col="end_date",
    ) -> pd.DataFrame:
        res_data = self.query_timeseries_common_data(
            t_asharebalancesheet, codes, start_date, end_date, date_col
        )

        return res_data

    def get_income(
        self,
        codes: Union[str, List] = None,
        start_date=None,
        end_date=None,
        date_col="end_date",
    ) -> pd.DataFrame:
        res_data = self.query_timeseries_common_data(
            t_ashareincome, codes, start_date, end_date, date_col
        )

        return res_data

    def get_finance_indicator(
        self,
        codes: Union[str, List] = None,
        start_date=None,
        end_date=None,
        date_col="end_date",
        fields=[],
    ):
        res_data = self.query_timeseries_common_data(
            t_asharefinaindicator,
            codes,
            start_date,
            end_date,
            date_col=date_col,
            fields=fields,
        )

        return res_data


class TuIndexData(IndexData):
    def get_index_price(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        fields: List = None,
    ) -> pd.DataFrame:
        res_data = self.query_timeseries_common_data(
            t_ashareindexeod, code, start_date, end_date, fields=fields
        )
        return res_data

    def get_index_const(
        self,
        code: str,
        start_date: str = None,
        end_date: str = None,
        fields: List = None,
    ) -> pd.DataFrame:
        table01 = t_zz500_const
        table02 = t_hs300_const
        if code == "hs300":
            table = table02
        elif code == "zz500":
            table = table01
        else:
            raise Exception("仅支持hs300与zz500")

        query_fields = [table]
        conds = []
        date_col = "trade_date"
        if start_date:
            conds.append(getattr(table.c, date_col) >= start_date)
        if end_date:
            conds.append(getattr(table.c, date_col) <= end_date)
        if len(conds) == 0:

            ex = select(*query_fields).where(and_(*conds)).limit(50)
        else:
            ex = select(*query_fields).where(and_(*conds))
        res_data = self.query_db(ex)
        return res_data


class TuIndustryData(IndustryData):
    def get_sw_const(
        self,
        codes: Union[str, List] = None,
        start_date: str = None,
        end_date: str = None,
        level: int = 1,
    ):
        lv01_table = t_sw_industry
        lv02_table = t_sw_industry_lv2

        if level == 1:
            table = lv01_table
        elif level == 2:
            table = lv02_table
        else:
            raise Exception("level参数仅支持1与2")

        query_fields = [table]
        conds = []
        date_col = "trade_date"
        if start_date:
            conds.append(getattr(table.c, date_col) >= start_date)
        if end_date:
            conds.append(getattr(table.c, date_col) <= end_date)
        if codes:
            codes = [codes] if isinstance(codes, str) else codes
            conds.append(getattr(table.c, "stock_code").in_(codes))
        if len(conds) == 0:

            ex = select(*query_fields).where(and_(*conds)).limit(50)
        else:
            ex = select(*query_fields).where(and_(*conds))
        res_data = self.query_db(ex)
        return res_data

    def get_sw_industry(self, level: str):
        if level == "lv01":
            table = t_sw_industry_classify_lv1
        elif level == "lv02":
            table = t_sw_industry_classify_lv2
        elif level == "lv03":
            table = t_sw_industry_classify_lv3
        else:
            raise Exception("只支持lv01,lv02,lv03")
        query_fields = [table]

        ex = select(*query_fields)
        res_data = self.query_db(ex)

        return res_data


class RedQuantData(
    TuMarketData, TuStockFinanceData, TuIndexData, TuIndustryData, FactorData
):
    pass


data_api_obj = RedQuantData()
