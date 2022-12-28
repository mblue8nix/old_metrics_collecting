#!/usr/bin/python
'''Old Metrics script in python2.7
'''
import logging
import logging.handlers
import socket
import sys
import time
import MySQLdb
from ConfigParser import ConfigParser

# logging
__format__ = "%(asctime)s - %(levelname)s : %(message)s"
logging.getLogger('GraphiteMstatsLoader')
logging.basicConfig(filename='/tmp/get_mysql_sys_stats.log',
                    level=logging.DEBUG, format=__format__)

# Config
config = ConfigParser()
config.read('/etc/default/get_mysql_stats')

if config.has_section("graphite"):
    graphite_defaults = dict(config.items("graphite"))

if config.has_section("mysql"):
    mysql_defaults = dict(config.items("mysql"))


if 'graphite_host' in graphite_defaults:
    graphite_host = graphite_defaults['graphite_host']

if 'graphite_port' in graphite_defaults:
    graphite_port = graphite_defaults['graphite_port']

if 'mysql_user' in mysql_defaults:
    mysql_user = mysql_defaults['mysql_user']

if 'mysql_pass' in mysql_defaults:
    mysql_pass = mysql_defaults['mysql_pass']

if 'mysql_port' in mysql_defaults:
    mysql_port = mysql_defaults['mysql_port']

graphite_port = int(graphite_port)

__mysql_host__ = socket.gethostname()
__split_hostname__ = __mysql_host__.split('.'[0])
__prefix__ = __split_hostname__[1]
__profix__ = __split_hostname__[0]
__service__ = 'sys'
__metric_name__ = 'servers.'+__prefix__+'.'+__profix__+'.'+__service__
__now__ = int(time.time())
__no_connect_message__ = "Couldn't connect to {0} on port {1}".format(graphite_host, graphite_port)
__debug_metric_message__ = "Metric format: {0}.metric.name.value {1}".format(__metric_name__, __now__)

db = MySQLdb.connect(__mysql_host__, mysql_user, mysql_pass, "sys")
cursor = db.cursor()

"""
Function to convert picoseconds to nanoseconds for grafana.
Grafan only suports up to nano pnc (pico nano converter)
"""


def pnc(x):
    return x*.001


"""
Testing only

ttime = time.ctime()
logging.debug('Start: %s' % ttime)
logging.debug(__debug_metric_message__)
"""

"""
User Summary 
Split connections per user metrics
Helpful in shared eviroments
"""
def user_connections():
    user_connections_sql ="""
SELECT USER,
              current_connections,
              total_connections,
              unique_hosts
FROM sys.user_summary
WHERE current_connections <> 0
ORDER BY current_connections DESC;
"""
    try:
        server_metrics = dict()
        cursor.execute(user_connections_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            connections = row[1]
            m1=("\n{0}.users.{1}.connections {2} {3}\n".format(__metric_name__, user,connections, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_connections failed")

"""
USER STATS
Using sys.x$user_summary_by_statement_type
"""


def user_deletes():
    user_delete_sql ="""
SELECT USER,
              total
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='delete'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""
    try:
        server_metrics = dict()
        cursor.execute(user_delete_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            total = row[1]
            m1=("\n{0}.users.{1}.deleted {2} {3}\n".format(__metric_name__, user, total, __now__))
            sock = socket.socket()
            try:
                sock.connect((graphite_host, graphite_port))
            except:
                logging.error(__no_connect_message__)
                sys.exit(1)
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_deletes failed")


def user_insert_latency():
    user_insert_latency_sql ="""
SELECT USER,
  total_latency
FROM sys.x $user_summary_by_statement_type
WHERE STATEMENT = 'insert'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""

    try:
        server_metrics = dict()
        cursor.execute(user_insert_latency_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            latency = row[1]
            latency = (pnc(latency))
            m1=("\n{0}.users.{1}.insert_latency {2} {3}\n".format(__metric_name__, user, latency, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_insert_latency failed")


def user_updates():
    user_updates_sql ="""
SELECT USER,
              total
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='update'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""

    try:
        server_metrics = dict()
        cursor.execute(user_updates_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            total = row[1]
            m1=("\n{0}.users.{1}.update {2} {3}\n".format(__metric_name__, user, total, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
       logging.error("Function user_updates failed")


def user_update_latency():
    user_update_latency_sql ="""
SELECT USER,
              total_latency
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='update'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""

    try:
        server_metrics = dict()
        cursor.execute(user_update_latency_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            latency = row[1]
            latency = (pnc(latency))
            m1=("\n{0}.users.{1}.update_latency {2} {3}\n".format(__metric_name__, user, latency, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_update_latency failed")


def user_inserts():
    user_inserts_sql ="""
SELECT USER,
              total
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='insert'
    AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""

    try:
        server_metrics = dict()
        cursor.execute(user_inserts_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            total = row[1]
            m1=("\n{0}.users.{1}.inserts {2} {3}\n".format(__metric_name__, user, total, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_inserts failed")


def user_select_latency():
    user_select_latency_sql ="""
SELECT USER,
              total_latency
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='select'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""
    try:
        server_metrics = dict()
        cursor.execute(user_select_latency_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            latency = row[1]
            latency = (pnc(latency))
            m1=("\n{0}.users.{1}.selects_latency {2} {3}\n".format(__metric_name__, user,latency, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_select_latency failed")


def user_selects():
    user_selects_sql ="""
SELECT USER,
              total
FROM sys.x$user_summary_by_statement_type
WHERE STATEMENT ='select'
  AND USER NOT IN (
    'USER1',
    'USER2',
    'USER3'
  );
"""
    try:
        server_metrics = dict()
        cursor.execute(user_selects_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            user = row[0]
            user = user.replace('.', '_')
            total = row[1]
            m1=("\n{0}.users.{1}.selects {2} {3}\n".format(__metric_name__,user,total, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.close()
    except:
        logging.error("Function user_selects failed")

"""
TABLE STATS
Using sys.x$schema_table_statistics
Tons of data
"""


def table_statistics():
    table_statistics_sql ="""
  SELECT table_schema,
              table_name,
              rows_fetched,
              fetch_latency,
              rows_inserted ,
              insert_latency,
              rows_updated,
              update_latency,
              rows_deleted,
              delete_latency
FROM sys.x$schema_table_statistics
WHERE table_schema NOT IN ( 
    'mysql',
    'performance_schema',
    'sys')
AND rows_inserted + rows_updated <> '0';
"""

    try:
        server_metrics = dict()
        cursor.execute(table_statistics_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            table_schema = row[0]
            table_name = row[1]
            rows_fetched = row[2]
            fetch_latency = row[3]
            fetch_latency = (pnc(fetch_latency))
            rows_inserted = row[4]
            insert_latency = row[5]
            insert_latency = (pnc(insert_latency))
            rows_updated = row[6]
            update_latency = row[7]
            update_latency = (pnc(update_latency))
            rows_deleted = row[8]
            delete_latency = row[9]
            delete_latency = (pnc(delete_latency))
            m1 = ("\n{0}.table_stats.{1}.{2}.fetched {3} {4}\n".format(__metric_name__,table_schema,table_name,rows_fetched,__now__))
            m2 = ("\n{0}.table_stats.{1}.{2}.fetched_latency {3} {4}\n".format(__metric_name__,table_schema,table_name,fetch_latency,__now__))
            m3 = ("\n{0}.table_stats.{1}.{2}.inserts {3} {4}\n".format(__metric_name__,table_schema,table_name,rows_inserted,__now__))
            m4 = ("\n{0}.table_stats.{1}.{2}.insert_latency {3} {4}\n".format(__metric_name__,table_schema,table_name,insert_latency,__now__))
            m5 = ("\n{0}.table_stats.{1}.{2}.updates {3} {4}\n".format(__metric_name__,table_schema,table_name,rows_updated,__now__))
            m6 = ("\n{0}.table_stats.{1}.{2}.update_latency {3} {4}\n".format(__metric_name__,table_schema,table_name,update_latency,__now__))
            m7 = ("\n{0}.table_stats.{1}.{2}.deleted {3} {4}\n".format(__metric_name__,table_schema,table_name,rows_deleted,__now__))
            m8 = ("\n{0}.table_stats.{1}.{2}.delete_latency {3} {4}\n".format(__metric_name__,table_schema,table_name,delete_latency,__now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.sendall(m2)
            sock.sendall(m3)
            sock.sendall(m4)
            sock.sendall(m5)
            sock.sendall(m6)
            sock.sendall(m7)
            sock.sendall(m8)
            sock.close()
    except:
        logging.error("Function table_statistics_sys failed")

"""
STATEMENT ERRORS AND WARNINGS
using sys.statements_with_errors_or_warnings
Checking any statement errors or warnings that are not logged by mysql.
"""


def stmnt_errors_warnings():
    stmnt_errors_sql ="""
SELECT SUM(errors)
FROM sys.statements_with_errors_or_warnings;
"""

    try:
        server_metrics = dict()
        cursor.execute(stmnt_errors_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            count_errors = row[0]
            met1=("\n{0}.stmnt_stats.errors {1} {2}\n".format(__metric_name__, count_errors, __now__))
    except:
        logging.error("Function stmnt_errors failed")

    stmnt_warnings_sql ="""
SELECT SUM(warnings)
FROM sys.statements_with_errors_or_warnings;
"""

    try:
        server_metrics = dict()
        cursor.execute(stmnt_warnings_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            count_warnings = row[0]
            met2=("\n{0}.stmnt_stats.warnings {1} {2}\n".format(__metric_name__, count_warnings, __now__))
    except:
        logging.error("Function stmnt_warnings failed")

    sock = socket.socket()
    sock.connect((graphite_host, graphite_port))
    sock.sendall(met1)
    sock.sendall(met2)
    sock.close()

"""
AUTO INCREMENT STATS
Using table sys.schema_auto_increment_columns
Useful for checking auto inc ratios
"""


def auto_inc():
    auto_inc_sql ="""
SELECT table_schema,
       table_name,
       column_name,
       max_value,
       auto_increment,
       auto_increment_ratio
FROM sys.schema_auto_increment_columns
ORDER BY auto_increment_ratio DESC LIMIT 5;
"""

    try:
        server_metrics = dict()
        cursor.execute(auto_inc_sql)
        server_metrics = cursor.fetchall()

        for row in server_metrics:
            table_schema = row[0]
            table_name = row[1]
            column_name = row[2]
            max_val = row[3]
            auto_inc_val = row[4]
            auto_inc_ratio = row[5]

            m1=("\n{0}.auto_inc.{1}.{2}.{3}.auto_inc_ratio {4} {5}\n".format(__metric_name__, table_schema, table_name, column_name, auto_inc_ratio, __now__))
            m2=("\n{0}.auto_inc.{1}.{2}.{3}.auto_inc_val {4} {5}\n".format(__metric_name__, table_schema, table_name, column_name, auto_inc_val, __now__))
            m3=("\n{0}.auto_inc.{1}.{2}.{3}.auto_inc_max_val {4} {5}\n".format(__metric_name__, table_schema, table_name, column_name, max_val, __now__))
            sock = socket.socket()
            sock.connect((graphite_host, graphite_port))
            sock.sendall(m1)
            sock.sendall(m2)
            sock.sendall(m3)
            sock.close()
    except:
        logging.error("Function auto_inc failed")

"""
Testing only
ttime = time.ctime()
logging.debug('Completed: %s \n' % ttime)
"""

user_connections()
user_deletes()
user_insert_latency()
user_updates()
user_update_latency()
user_inserts()
user_select_latency()
user_selects()
table_statistics()
stmnt_errors_warnings()
auto_inc()


db.close()
