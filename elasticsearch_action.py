#!/usr/bin/python
# ElasticSearch operation script
# 
# Print: print document id from daterange
# Delete: delete documents from daterange

import optparse
from elasticsearch import Elasticsearch


def delete_docs(INDEX, response):
    for doc in response['hits']['hits']:
        es.delete(index=INDEX, doc_type='trapdata', id=doc['_id'], refresh='true')


def print_docs(response):
    for doc in response['hits']['hits']:
        print doc['host']


def do_action(action, INDEX, response):
    if action == "print":
        print_docs(response)
    elif action == "delete":
        delete_docs(INDEX, response)


if __name__ == "__main__":
    PROGRAM_NAME = str(__file__)
    parser = optparse.OptionParser("usage: " + PROGRAM_NAME + " --host 127.0.0.1 --port 9200 --index trapdata --datefiled '@timestamp' --from-date '2016-11-10' --to-date '2016-10-11' --action print")
    parser.add_option("-H", "--host", dest="hostname", default="127.0.0.1", type="string", help="specify hostname of elasticsearch")
    parser.add_option("-p", "--port", dest="portnum", default=9200, type="int", help="port number of elasticsearch")
    parser.add_option("-i", "--index", dest="index", default="elasticsearch", type="string", help="elasticsearch index to search for")
    parser.add_option("-d", "--datefield", dest="datefield", default="@timestamp", type="string", help="elasticsearch datetime field name")
    parser.add_option("-f", "--from-date", dest="fromdate", default="1970-01-01", type="string", help="timestamp to start search")
    parser.add_option("-t", "--to-date", dest="todate", default="1970-01-01", type="string", help="timestamp to end search")
    parser.add_option("-F", "--format", dest="d_format", default="yyyy-MM-dd", type="string", help="timestamp format")
    parser.add_option("-a", "--action", dest="action", default="print", type="string", help="elasticsearch api method: print / delete ")

    (options, args) = parser.parse_args()

    HOSTNAME = options.hostname
    PORTNUM = options.portnum
    INDEX = options.index
    DATEFIELD = options.datefield
    FROMDATE = options.fromdate
    TODATE = options.todate
    FORMAT = options.d_format
    ACTION = options.action


    # Elasticsearch connection
    es = Elasticsearch([{'host': HOSTNAME, 'port': PORTNUM}])

    # init some counters
    total_docs = 0
    index = 0

    # First search
    first = es.search(index=INDEX, scroll='1d', timeout="30s", filter_path=['hits.hits._id', '_scroll_id'], size=10000, body={"query": {"range": {DATEFIELD: {"gte": FROMDATE , "lte": TODATE, "format": FORMAT}}}})

    sid = first['_scroll_id']

    # check if response has documents
    if len(first) < 2:
        print "No docs found."
    else:
        scroll_size = len(first['hits']['hits'])
        total_docs += scroll_size
        do_action(ACTION, INDEX, first)

        # Loop until no docs will returned
        while (scroll_size > 0):
            print "documents processed: " + str(total_docs)
            try:
                page = es.scroll(scroll_id=sid, scroll='1d')
                scroll_size = len(page['hits']['hits'])
                total_docs += scroll_size
                do_action(ACTION, INDEX, page)
            except Exception as inst:
                print type(inst)
                print inst

    print "Total documents found: " + str(total_docs)
