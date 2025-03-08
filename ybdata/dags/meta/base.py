import os
import yaml
import tushare as ts
from sqlalchemy import create_engine
from urllib.parse import quote
import pandas as pd
from .config import Config


class API:
    def __init__(self):
        token = self.__load_token()
        self.__api = ts.pro_api(token)
        self.__db_engine = self.__make_db_engine()
        self.__ts = ts
        self.__ts.set_token(token)

    def __getattr__(self, name):
        if name == "pro_bar":

            return ts.pro_bar
        else:
            func = getattr(self.__api, name)
            return func

    def __make_db_engine(self):
        engine_alpha = create_engine(Config.MYSQL_ENGINE_STR)
        return engine_alpha

    def __load_token(self):
        config_path = Config.TOKEN_PATH
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = f.read()
        settings = yaml.load(cfg, Loader=yaml.FullLoader)
        tushare_token = settings["tushareToken"]
        return tushare_token

    def read_mysql(self, sql):
        data = pd.read_sql(sql, self.__db_engine)
        return data

    def get_calender(self, start: str, end: str):
        """获取交易日历"""
        df = self.__api.trade_cal(
            exchange="SSE", start_date=start, end_date=end, is_open=1
        )
        return df.cal_date.tolist()
