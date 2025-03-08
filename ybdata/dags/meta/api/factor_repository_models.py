"""
因子数据表与python类orm的对应
"""

# coding: utf-8
from sqlalchemy import Column, Float, String, TIMESTAMP, Table, text
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class FactorStore(Base):
    __tablename__ = "factor_store"

    id = Column(INTEGER(11), primary_key=True)
    category = Column(String(50))
    factor_name = Column(String(50))
    factor_code = Column(String(50))
    calculation_method = Column(String(100))
    frequency = Column(String(20))
    storage_table = Column(String(15))
    database = Column(String(50))
    api = Column(String(50))
    factor_level = Column(String(10))
    factor_column_name = Column(String(50))
    date_col = Column(String(18), nullable=False)
    owner = Column(String(20))
    insert_time = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    update_time = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
    )


t_pcf_ncf_ttm_m = Table(
    "pcf_ncf_ttm_m",
    metadata,
    Column("ts_code", String(9)),
    Column("trade_date", String(8)),
    Column("pcf_ncf_ttm_m", Float(asdecimal=True)),
)


t_size = Table(
    "size",
    metadata,
    Column("ts_code", String(9)),
    Column("trade_date", String(8)),
    Column("size", Float(asdecimal=True)),
)
