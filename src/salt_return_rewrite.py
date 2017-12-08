# -*- coding: utf-8 -*-

import os
import json
import MySQLdb
import ConfigParser
import traceback
import sys

import salt.config
import salt.utils.event
from celery import platforms
from celery import Celery
from celery.utils.log import get_task_logger

conf = ConfigParser.ConfigParser()
cfg_path = os.path.join(os.path.dirname(__file__), 'cfg.ini')
logger = get_task_logger(__name__)

conf.read(cfg_path)
redis = conf.get("db_cfg", "redis")
app = Celery("tasks", broker="redis://" + redis + "/1")


@app.task
def insert_sql(event_tag, event_data):
    conf = ConfigParser.ConfigParser()
    cfg_path = os.getcwd() + '/cfg.ini'
    conf.read(cfg_path)
    cfg_host = conf.get("db_cfg", "host")
    cfg_user = conf.get("db_cfg", "user")
    cfg_passwd = conf.get("db_cfg", "passwd")
    cfg_db = conf.get("db_cfg", "db")
    cfg_port = conf.get("db_cfg", "port")
    conn = MySQLdb.connect(
        host=cfg_host,
        user=cfg_user,
        passwd=cfg_passwd,
        db=cfg_db,
        port=3306
    )

    cur = conn.cursor()
    sql = '''INSERT INTO `salt_returns`
        (`fun`, `jid`, `return`, `id`, `success`, `full_ret` )
        VALUES (%s, %s, %s, %s, %s, %s)'''
    logger.info(event_data)
    cur.execute(sql, (
        event_data['fun'],
        event_data['jid'],
        json.dumps(event_data['return']),
        event_data['id'],
        event_data.get('success', False),
        json.dumps(event_data)
    ))
    conn.commit()
    cur.close()
    conn.close()
