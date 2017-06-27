#!/usr/bin/python
# ElasticSearch delte index script
# 
# This scripts delete indexes named by date with a date range given by parameter

from elasticsearch import Elasticsearch
import datetime
import optparse

if __name__ == "__main__":
    PROGRAM_NAME = str(__file__)
    parser = optparse.OptionParser("usage: " + PROGRAM_NAME + " --host 127.0.0.1 --port 9200 --index example --number-of-days 1 --debug 0")
    parser.add_option("-H", "--host", dest="hostname", default="127.0.0.1", type="string", help="Hostname of elasticsearch")
    parser.add_option("-p", "--port", dest="portnum", default=9200, type="int", help="Port number of elasticsearch")
    parser.add_option("-i", "--index", dest="index", default="example", type="string", help="Elasticsearch index to reindex for")
    parser.add_option("-f", "--from-date", dest="fromdate", default="1970-01-01", type="string", help="timestamp to start search")
    parser.add_option("-t", "--to-date", dest="todate", default="1970-01-01", type="string", help="timestamp to end search")
    parser.add_option("-T", "--timeout", dest="timeout", default="30s", type="string", help="timeout to execute the deletion")
    parser.add_option("-d", "--debug", dest="debug", default=0, type="int", help="Enable or disable debug mode")
    
    (options, args) = parser.parse_args()

    HOSTNAME = options.hostname
    PORTNUM = options.portnum
    INDEX = options.index
    FROMDATE = options.fromdate
    TODATE = options.todate
    TIMEOUT = options.timeout
    DEBUG = options.debug
    
    es = Elasticsearch([{'host': HOSTNAME, 'port': PORTNUM}])

    dt_todate = datetime.datetime.strptime(TODATE,"%Y-%m-%d").date()
    dt_index = datetime.datetime.strptime(FROMDATE,"%Y-%m-%d").date()

    while dt_todate >= dt_index:
        index_to_delete = str(INDEX)+"-"+str(dt_index)
        if DEBUG:
            print "Index to delete: "+index_to_delete
        dt_index += datetime.timedelta(days=1)
        try:
            res = es.indices.delete(index = index_to_delete, timeout = TIMEOUT)
            print "Deleted index named "+index_to_delete+" with output: "+str(res)
        except Exception as inst:
            print type(inst)


