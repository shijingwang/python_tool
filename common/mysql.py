# -*- coding: utf-8 -*-
#
# Copyright (c) 2013 feilong.me All rights reserved.
#
# @author: Felinx Lee <felinx.lee@gmail.com>
# Created on Mar 19, 2013
#

import copy
import logging
import time

import MySQLdb.converters

try:
    from tornado.database import Connection as BaseConnection
except ImportError:
    from torndb import Connection as BaseConnection


class Connection(BaseConnection):
    _in_transaction = False

    def __init__(self, host, database, user, password, max_idle_time=7 * 3600,
                 init_command='SET time_zone = "+8:00"'):
        self.host = host
        self.database = database
        self.max_idle_time = max_idle_time

        args = dict(conv=CONVERSIONS, use_unicode=True, charset="utf8",
                    db=database, init_command=init_command,
                    sql_mode="TRADITIONAL")
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

CONVERSIONS = copy.deepcopy(MySQLdb.converters.conversions)
