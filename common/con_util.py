# -*- coding: utf-8 -*-

from common.mysql import Connection
import redis


class ConUtil(object):

    @staticmethod
    def connect_mysql(configs):
        return Connection(host="%s:%s" % (configs["host"], configs["port"]),
                          database=configs["database"],
                          user=configs["user"],
                          password=configs["password"],
                          max_idle_time=configs.get("max_idle_time", 3200),
                          )

    @staticmethod
    def connect_redis(configs):
        return redis.Redis(host=configs.get("host", "127.0.0.1"), port=configs.get('port', 6379), password=configs.get('password', None))
