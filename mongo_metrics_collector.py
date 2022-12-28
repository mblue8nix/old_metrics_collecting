#!/usr/bin/env python2.7
"""
This module was created to connect to a mongo db instance and send mongo \
stats to bigfishgames graphite relay servers.
The module - "pymongo" is a dependency to connect to mongo db.
TO DO:  More stats can be added.  Currently what was dded seemed important for now.
Could also add ConfigParser
NOTE: Debug is turned on since this is a new script and logs into "/tmp/mongo_get_stats.log" .
Also info will log if there are missing stat categories like replication stats.
We can turn it off in time.
"""
import logging
import logging.handlers
import socket
import sys
import time
from pymongo import MongoClient

__author__ = 'michael.blue'

# logging
__format__ = "%(asctime)s - %(levelname)s : %(message)s"
logging.getLogger('GraphiteMstatsLoader')
logging.basicConfig(filename='/tmp/mongo_get_stats.log',
                    level=logging.DEBUG, format=__format__)

# info
__graphitehost__ = 'GRAPHITE_HOST_NAME'
__graphiteport__ = 2003
__service__ = 'mongodb.'

__mongo_host__ = socket.gethostname()
__split_hostname__ = __mongo_host__.split('.'[0])
__prefix__ = __split_hostname__[1]
__metric_name__ = ' servers.'+__prefix__+'.'
{%- if config.authorization == 'enabled' %}
__urifoo__  = "mongodb://nagios:{{ mon_pw }}@localhost"   # for salt
__connect__ = MongoClient(__urifoo__)
{% else %}
__connect__ = MongoClient()
{% endif %}

# error/info/warning messages
__no_connect_message__ = "Couldn't connect to %(server)s on port %(port)d" \
                     % {'server': __graphitehost__, 'port': __graphiteport__}
__not_replset_info__ = "No ReplSet stats! %(host)s is not running with a replSet" \
                   % {'host': __mongo_host__}


def upload_to_graphite(metrics):
    """
    Upload metrics to graphite in metric name + value + Unix time
    """
    now = int(time.time())
    lines = []

    for name, value in metrics.iteritems():
        if name.find('mongo') == -1:
            name = __mongo_host__.split('.')[0]+'.'+__service__+name
        lines.append(__metric_name__+name+' %s %d' % (value, now))

    sock = socket.socket()

    try:
        sock.connect((__graphitehost__, __graphiteport__))
    except:
        print(__no_connect_message__)
        logging.error(__no_connect_message__)
        sys.exit(1)
    message = '\n'.join(lines) + '\n'

    # Uncomment below debugging stats

    #logging.debug('Stats Output {'+'\n'+message+'}')

    sock.sendall(message)
    sock.close()


def calculate_lag_times(replStatus, primaryDate):
    """
    Calculates Lag time function for the mongo db servers.
    """
    lags = dict()

    for hostState in replStatus['members']:
        lag = primaryDate - hostState['optimeDate']
        hostName = hostState['name'].lower().split('.')[0]
        lags[hostName+'.'+__service__+'replSet.'+'lag_seconds'] = '%.0f' % \
            ((lag.microseconds +
              (lag.seconds + lag.days * 24 * 3600) * 10**6) / 10**6)
    return lags


def gather_replication_metrics():
    """
    NOTE: Per instance it will show replication stats \
    for all servers in the replSet.
    """
    __replica_metrics__ = dict()
    __repl_status__ = __connect__.admin.command("replSetGetStatus")

    for hostState in __repl_status__['members']:
        if hostState['stateStr'] == \
                'PRIMARY' and hostState['name'].lower().startswith(__mongo_host__):
            lags = calculate_lag_times(__repl_status__, hostState['optimeDate'])
            __replica_metrics__.update(lags)
        if hostState['name'].lower().startswith(__mongo_host__):
            thisHostsState = hostState

    __replica_metrics__['replSet.state'] = \
        thisHostsState['state']
    return __replica_metrics__


def gather_server_status_metrics():
    """
    Gathers metric stats from serverStatus and outputs \
    the metric name for graphite with the stats.
    Added storage engine name for WireTiger Stats exception
    and logging
    """

    server_metrics = dict()
    server_status = __connect__.admin.command('serverStatus')
    engine_type = server_status['storageEngine']['name']

    server_metrics['connections.current'] = \
        server_status['connections']['current']
    server_metrics['lock.queue.total'] = \
        server_status['globalLock']['currentQueue']['total']
    server_metrics['lock.queue.readers'] = \
        server_status['globalLock']['currentQueue']['readers']
    server_metrics['lock.queue.writers'] = \
        server_status['globalLock']['currentQueue']['writers']
    server_metrics['connections.current'] = \
        server_status['connections']['current']
    server_metrics['connections.available'] = \
        server_status['connections']['available']
    server_metrics['mem.residentMb'] = \
        server_status['mem']['resident']
    server_metrics['mem.virtualMb'] = \
        server_status['mem']['virtual']
    server_metrics['mem.mapped'] = \
        server_status['mem']['mapped']
    server_metrics['mem.pageFaults'] = \
        server_status['extra_info']['page_faults']
    server_metrics['asserts.warnings'] = \
        server_status['asserts']['warning']
    server_metrics['asserts.errors'] = \
        server_status['asserts']['msg']
    server_metrics['opcounters.command'] = \
        server_status['opcounters']['command']
    server_metrics['opcounters.insert'] = \
        server_status['opcounters']['insert']
    server_metrics['opcounters.query'] = \
        server_status['opcounters']['query']
    server_metrics['opcounters.delete'] = \
        server_status['opcounters']['delete']
    server_metrics['opcounters.getmore'] = \
        server_status['opcounters']['getmore']
    server_metrics['opcounters.update'] = \
        server_status['opcounters']['update']
    server_metrics['network.bytesIn'] = \
        server_status['network']['bytesIn']
    server_metrics['network.bytesOut'] = \
        server_status['network']['bytesOut']
    server_metrics['network.numrequests'] = \
        server_status['network']['numRequests']
    server_metrics['metrics.queryExecutor.scanned'] = \
        server_status['metrics']['queryExecutor']['scanned']
    server_metrics['metrics.queryExecutor.scannedObjects'] = \
        server_status['metrics']['queryExecutor']['scannedObjects']

    # wireTiger engine stats

    __not_wire_tigger_info__ = "No wireTiger stats! " \
                               "%(host)s default engine is: %(engine)s" %\
                               {'host': __mongo_host__, 'engine': engine_type}

    try:
        server_metrics['wiredTiger.connection.memory_allocations'] = \
            server_status['wiredTiger']['connection']['memory allocations']
        server_metrics['wiredTiger.connection.memory_frees'] = \
            server_status['wiredTiger']['connection']['memory frees']
        server_metrics['wiredTiger.connection.total_read_IOs'] = \
            server_status['wiredTiger']['connection']['total read I/Os']
        server_metrics['wiredTiger.connection.total_write_IOs'] = \
            server_status['wiredTiger']['connection']['total write I/Os']
        server_metrics['wiredTiger.connection.pthread_mutex_wait_calls'] = \
            server_status['wiredTiger']['connection']['pthread mutex condition wait calls']
        server_metrics['wiredTiger.cache.bytes_read_into_cache'] = \
            server_status['wiredTiger']['cache']['bytes read into cache']
        server_metrics['wiredTiger.cache.bytes_written_cache'] = \
            server_status['wiredTiger']['cache']['bytes written from cache']
        server_metrics['wiredTiger.cache.bytes_currently_in_cache'] = \
            server_status['wiredTiger']['cache']['bytes currently in the cache']
        # WT Log Operation Stats
        server_metrics['wiredTiger.log.sync'] = \
            server_status['wiredTiger']['log']['log sync operations']
        server_metrics['wiredTiger.log.sync_dir'] = \
            server_status['wiredTiger']['log']['log sync_dir operations']
        server_metrics['wiredTiger.log.flush'] = \
            server_status['wiredTiger']['log']['log flush operations']
        server_metrics['wiredTiger.log.read'] = \
            server_status['wiredTiger']['log']['log read operations']
        server_metrics['wiredTiger.log.write'] = \
            server_status['wiredTiger']['log']['log write operations']
        server_metrics['wiredTiger.log.scan'] = \
            server_status['wiredTiger']['log']['log scan operations']
        server_metrics['wiredTiger.log.scan_double'] = \
            server_status['wiredTiger']['log']['log scan records requiring two reads']
        # WT Log Bytes
        server_metrics['wiredTiger.log.buff_size'] = \
            server_status['wiredTiger']['log']['total log buffer size']
        server_metrics['wiredTiger.log.payload'] = \
            server_status['wiredTiger']['log']['log bytes of payload data']
        server_metrics['wiredTiger.log.written'] = \
            server_status['wiredTiger']['log']['log bytes written']
        # WT Transactions
        server_metrics['wiredTiger.transaction.begins'] = \
            server_status['wiredTiger']['transaction']['transaction begins']
        server_metrics['wiredTiger.transaction.checkpoints'] = \
            server_status['wiredTiger']['transaction']['transaction checkpoints']
        server_metrics['wiredTiger.transaction.committed'] = \
            server_status['wiredTiger']['transaction']['transactions committed']
        server_metrics['wiredTiger.transaction.rolled_back'] = \
            server_status['wiredTiger']['transaction']['transactions rolled back']

    except:
        logging.info(__not_wire_tigger_info__)
    return server_metrics

metrics = dict()
metrics.update(gather_server_status_metrics())

try:
    metrics.update(gather_replication_metrics())
except:
    logging.info(__not_replset_info__)

upload_to_graphite(metrics)
