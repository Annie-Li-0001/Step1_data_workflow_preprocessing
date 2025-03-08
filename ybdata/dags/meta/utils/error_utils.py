"""
文件说明
"""


class ParamError(Exception):
    pass


class DBError(Exception):
    pass


class MysqlError(Exception):
    pass


class OracleError(Exception):
    pass


class PostgresError(Exception):
    pass


class SQLError(Exception):
    pass


class RedisError(Exception):
    pass


class MongoError(Exception):
    pass


class NotSupportError(Exception):
    pass


class DoesNotExist(Exception):
    pass
