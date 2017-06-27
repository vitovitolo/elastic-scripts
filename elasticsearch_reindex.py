#!/usr/bin/python
# ElasticSearch reindex script
# 
# This scripts reindex the last N days documents from elastic to new indexes by day
# The timeframe has to be passed as a parameter

from elasticsearch import Elasticsearch
import datetime
import optparse

if __name__ == "__main__":
    PROGRAM_NAME = str(__file__)
    parser = optparse.OptionParser("usage: " + PROGRAM_NAME + " --host 127.0.0.1 --port 9200 --index example --number-of-days 1 --debug 0")
    parser.add_option("-H", "--host", dest="hostname", default="127.0.0.1", type="string", help="Hostname of elasticsearch")
    parser.add_option("-p", "--port", dest="portnum", default=9200, type="int", help="Port number of elasticsearch")
    parser.add_option("-i", "--index", dest="index", default="example", type="string", help="Elasticsearch index to reindex for")
    parser.add_option("-D", "--datefield", dest="datefield", default="@timestamp", type="string", help="elasticsearch datetime field name")
    parser.add_option("-f", "--from-date", dest="fromdate", default="1970-01-01", type="string", help="timestamp to start search")
    parser.add_option("-t", "--to-date", dest="todate", default="1970-01-01", type="string", help="timestamp to end search")
    parser.add_option("-d", "--debug", dest="debug", default=0, type="int", help="Enable or disable debug mode")
    
    (options, args) = parser.parse_args()

    HOSTNAME = options.hostname
    PORTNUM = options.portnum
    INDEX = options.index
    DATEFIELD = options.datefield
    FROMDATE = options.fromdate
    TODATE = options.todate
    DEBUG = options.debug
    
    es = Elasticsearch([{'host': HOSTNAME, 'port': PORTNUM}], timeout=300)

    dt_todate = datetime.datetime.strptime(TODATE,"%Y-%m-%d").date()
    dt_index = datetime.datetime.strptime(FROMDATE,"%Y-%m-%d").date()

    while dt_todate >= dt_index:
        index_to_create = str(INDEX)+"-"+str(dt_index)
        from_date = str(dt_index) + "||/d"
        to_date = str(dt_index) + "||/d"
        if DEBUG:
            print "New Index to create: "+index_to_create
            print "from date search: " + from_date
            print "to date search: " + to_date
        try:
            res = es.reindex(refresh='true', wait_for_completion=False, body={"source": { "index": INDEX, "size": 10000, "type": INDEX, "query": {"range": {DATEFIELD: {"gte": from_date , "lte": to_date}}}}, "dest": { "index": index_to_create}})
            print "Reindex doing with task ID: "+str(res)
        except Exception as inst:
            print type(inst)
            print "Error reindexing elastic: "+ str(inst)
        dt_index += datetime.timedelta(days=1)

