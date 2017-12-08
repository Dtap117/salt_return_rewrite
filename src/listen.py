#_*_ coding: UTF-8 _*_
#by dTap

import os
import json
import time
import MySQLdb

import salt.config
import salt.utils.event

import salt_returner_ext

class Producer:

    def __init__(self):
        self.salt_config_path = "/etc/salt/master"
        
    def get_event(self):
        __opts__ = salt.config.client_config(self.salt_config_path)
        event = salt.utils.event.MasterEvent(__opts__['sock_dir'])
        
        for eachevent in event.iter_events(full=True):
            try:
                date = time.strftime('%Y-%m-%d', time.localtime())
                log_path = os.path.join(os.path.dirname(__file__), date + '.log')
                event_tag = eachevent['tag']
                event_data = eachevent['data']
                if event_data.has_key('fun') and event_data.has_key('id') and event_data.has_key('return'):
                    if event_data['fun'] == "cmd.run":
                        id = salt_returner_ext.insert_sql.delay(event_tag, event_data)
                        self.write_log(id, log_path)
                    else:
                        msg = event_data
                        self.write_log(msg, log_path)
                else:
                    msg = event_data
                    self.write_log(msg, log_path)
            except Exception, e:
                error_log_path = os.path.join(os.path.dirname(__file__), date + '_error.log')
                msg = e
                self.write_log(msg, error_log_path)
                
    def write_log(self, log, log_path):
        ISOTIMEFORMAT='%Y-%m-%d %X'
        time_now = time.strftime(ISOTIMEFORMAT, time.localtime())
        open_log = open(log_path, "a+")
        open_log.write(str(log) + "\t time:" + str(time_now) + "\n")
        open_log.close()

Producer().get_event()
