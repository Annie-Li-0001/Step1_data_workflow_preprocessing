"""
文件说明
"""

import pickle
import zlib

import redis
from meta.config import Config
import pandas as pd
import gzip
import redis
import io


class RedisMgr:
    def __init__(self):
        self.__host = Config.REDIS_HOST
        self.__port = Config.REDIS_PORT
        self.__redis_conn = redis.Redis(
            connection_pool=redis.ConnectionPool(
                host=self.__host, port=self.__port, db=0
            )
        )

    def write(self, key, dataframe, expire=True):
        dataframe_string = dataframe.to_csv(index=True)
        compressed_string = gzip.compress(dataframe_string.encode())
        if expire:
            self.__redis_conn.set(
                key, compressed_string, ex=Config.REDIS_KEY_EXPIRE_TIME
            )
        else:
            self.__redis_conn.set(key, compressed_string)

    def read_dataframe_from_redis(self, redis_key):
        # 连接到Redis
        compressed_string = self.__redis_conn.get(redis_key)
        dataframe_string = gzip.decompress(compressed_string).decode("utf-8")
        dataframe = pd.read_csv(io.StringIO(dataframe_string))

        return dataframe

    def read(self, key):
        data = self.__redis_conn.get(key)
        if data is None:
            return None
        res = pickle.loads(zlib.decompress(data))
        return res


redis_con = RedisMgr()
