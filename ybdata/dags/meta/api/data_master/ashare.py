"""
文件说明
"""

import pandas as pd
from typing import Union, List
from ..exceptions import APIParamError


def get_market_data(
    codes: Union[str, List, None] = None,
    start_date: str = None,
    end_date: str = None,
    mode="basic",
):
    """中国A股日行情"""
    if mode not in ["basic", "hfq"]:
        raise APIParamError(
            f"mode只支持basic与hfq，表示不复权与后复权数据，传入的mode为{mode}"
        )

    if trade_date:
        start_date = end_date = trade_date
    if not codes:
        if mode == "basic":
            market_data_sql = StockSQL.market_data_sql_date_range_all
        else:
            market_data_sql = StockSQL.market_data_sql_date_range_all_hfq
        sql = market_data_sql.format(start_date=start_date, end_date=end_date)
        mat = wind_db.read_sql(sql)
        mat.sort_values("trade_date", inplace=True)
        return mat
    else:
        if mode == "basic":
            market_data_sql = StockSQL.market_data_sql_date_range
        else:
            market_data_sql = StockSQL.market_data_sql_date_range_hfq
        # 传入了codes， start_date， end_date
        if isinstance(codes, str):
            codes = codes.split(",")
        codes = list(set(codes))
        size = len(codes)

        batch_size = 50
        if size > batch_size:
            num = math.ceil(size / batch_size)
            mat = pd.DataFrame()
            code_list_arr = np.array_split(np.array(codes), num)
            for code_list in code_list_arr:
                codes_str = "','".join(list(code_list))
                sql = market_data_sql.format(
                    code_arr=codes_str, start_date=start_date, end_date=end_date
                )
                # print(sql)
                df = wind_db.read_sql(sql)
                mat = pd.concat([mat, df])
        else:
            code_str = "','".join(codes) if isinstance(codes, list) else codes
            sql = market_data_sql.format(
                code_arr=code_str, start_date=start_date, end_date=end_date
            )
            # print(sql)
            mat = wind_db.read_sql(sql)
        mat.sort_values("trade_date", inplace=True)
        return mat


def wd_asharecalendar(
    s_info_exchmarket: Union[str, List, None] = "",
    trade_days: Union[str, List, None] = "",
    fields: str = "*",
):
    """
    中国A股交易日历[AShareCalendar]
    该接口返回全量数据，不返回迭代器
    其中exchange字段含义如下
    SSE:上海交易所 SZSE:深圳交易所 SHN:沪股通 SZN:深股通
    """
    import sys

    schema = SchemaEnum.ETERMINAL.value
    table = TableEnum.asharecalendar.value

    sgt = SQLGenerateTools(f"{schema}.{table}", fields)

    sgt.set_filters(
        {"s_info_exchmarket": s_info_exchmarket, "trade_days": trade_days}, "in"
    )
    sql = sgt.get_select_sql()
    try:
        df = wind_db.read_sql(sql)
    except Exception as e:
        print(sys.exc_info())
        raise
    df.sort_values("trade_days", ascending=True, inplace=True)
    df.index = range(len(df))
    return df
