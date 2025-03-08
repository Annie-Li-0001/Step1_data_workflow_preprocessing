Sql语句
-- tushare.AShareEodPrice definition
A股股票行情数据
CREATE TABLE `AShareEodPrice` (
`code` varchar(9) DEFAULT NULL,
`date` varchar(8) DEFAULT NULL,
`open` double DEFAULT NULL,
`high` double DEFAULT NULL,
`low` double DEFAULT NULL,
`close` double DEFAULT NULL,
`pre_close` double DEFAULT NULL,
`change` double DEFAULT NULL,
`pct_chg` double DEFAULT NULL,
`vol` double DEFAULT NULL,
`amount` double DEFAULT NULL,
UNIQUE KEY `AShareEodPrice_date_IDX` (`date`,`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- tushare.AShareEodBasic definition
A股指标数据
CREATE TABLE `AShareEodBasic` (
  `code` varchar(9) DEFAULT NULL,
  `date` varchar(8) DEFAULT NULL,
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
  UNIQUE KEY `AShareEodBasic_date_IDX` (`date`,`code`) USING BTREE,
  KEY `AShareEodBasic_code_IDX` (`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等';

Sql语句
后复权行情数据
-- tushare.AShareEodPrice definition

CREATE TABLE `AShareEodPriceHFQ` (
`code` varchar(9) DEFAULT NULL,
`date` varchar(8) DEFAULT NULL,
`open` double DEFAULT NULL,
`high` double DEFAULT NULL,
`low` double DEFAULT NULL,
`close` double DEFAULT NULL,
`pre_close` double DEFAULT NULL,
`change` double DEFAULT NULL,
`pct_chg` double DEFAULT NULL,
`vol` double DEFAULT NULL,
`amount` double DEFAULT NULL,
UNIQUE KEY `AShareEodPriceHFQ_date_IDX` (`date`,`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


Sql语句
可转债
-- tushare.CBEodPrice definition

CREATE TABLE `CBEodPrice` (
`code` varchar(9) DEFAULT NULL,
`date` varchar(8) DEFAULT NULL,
`open` double DEFAULT NULL,
`high` double DEFAULT NULL,
`low` double DEFAULT NULL,
`close` double DEFAULT NULL,
`pre_close` double DEFAULT NULL,
`change` double DEFAULT NULL,
`pct_chg` double DEFAULT NULL,
`vol` double DEFAULT NULL,
`amount` double DEFAULT NULL,
UNIQUE KEY `CBEodPrice_date_IDX` (`date`,`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




-- tushare.AShareMutualFundDaily definition

CREATE TABLE `AShareMutualFundDaily` (
  `ts_code` varchar(10),
  `trade_date` varchar(8),
  `pre_close` double DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8;


-- tushare.AShareMutualFund definition

CREATE TABLE `AShareMutualFund` (
  `ts_code` varchar(10),
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8;
