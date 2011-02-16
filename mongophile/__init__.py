#!/usr/bin/env python

import pymongo
import re
from ops import *

import cli.log

try:
    import json
except:
    import simplejson as json


insert_re = re.compile("^insert\s((.*)\.([0-9a-zA-Z$._]*))")
query_re = re.compile("^query\s((.*)\.([0-9a-zA-Z$._]*))\s*(ntoreturn:(\d*))?(scanAndOrder\s)?\sreslen:(\d*)\snscanned:(\d*)\s*(query:\s({.*}))?\s*nreturned:(\d*)")
update_re = re.compile("^update\s((.*)\.([0-9a-zA-Z$._]*))\s*query:\s({.*})\snscanned:(\d*)\s*(fastmod|fastmodinsert|upsert|moved)")
cmd_re = re.compile("^query\s(.*)\.\\$cmd\sntoreturn:(\d*)\scommand:\s({.*})\sreslen:(\d*)")
# TODO getmore

def parseEntry(app, entry):
    app.log.debug("TS: %s Millis: %d", entry['ts'], entry['millis'])
    # parse the debug block
    debug = entry['info']
    if debug.startswith("insert"):
        app.log.debug("Insert Op")
        data = insert_re.search(debug)
        if data:
            db = data.group(2)
            coll = data.group(3)
            #app.log.debug("DB: %s Coll: %s", db, coll)
        else:
            app.log.info("Failed to match RegEx on insert '%s'", debug)
    elif debug.startswith("query"):
        app.log.debug("Query Op")
        if debug.find("$cmd") >= 0:
            app.log.debug("Command Op")
            data = cmd_re.search(debug)
            if data:
                db = data.group(1)
                ntoreturn = data.group(2)
                command = data.group(3)
                reslen = data.group(4)
                app.log.debug("[Command] DB: %s NToReturn: %s Command: %s ResLen:%s", db, ntoreturn, command, reslen)
                return MongoCommand(app.log, entry['ts'], entry['millis'], db, ntoreturn, command, reslen)
            else:
                app.log.info("Failed to match RegEx on command '%s'" % debug)
        else:
            data = query_re.search(debug)
            if data:
                db = data.group(2)
                coll = data.group(3)
                ntoreturn = data.group(5)
                scanAndOrder = data.group(6)
                reslen = data.group(7)
                nscanned = data.group(8)
                query = data.group(10)
                nreturned = data.group(11)
                app.log.debug("[Query] DB: %s Coll: %s Scan And Order?: %s NToReturn: %s ResLen:%s NScanned:%s Query:%s NReturned:%s", db, coll, scanAndOrder, ntoreturn, reslen, nscanned, query, nreturned)
                return MongoQuery(app.log, entry['ts'], entry['millis'], db, coll, ntoreturn, scanAndOrder, reslen, nscanned, query, nreturned)
            else:
                app.log.info("Failed to match RegEx on query '%s'" % debug)
    elif debug.startswith("update"):
        app.log.debug("Update Op")
        data = update_re.search(debug)
        if data:
            db = data.group(2)
            coll = data.group(3)
            query = data.group(4)
            nscanned = data.group(5)
            opType = data.group(6)
            app.log.debug("[Update] DB: %s Coll: %s NScanned: %s OpType: %s Query: %s", db, coll, nscanned, opType, query)
            return MongoUpdate(app.log, entry['ts'], entry['millis'], db, coll, query, nscanned, opType)
        else:
            app.log.info("Failed to match RegEx on update '%s'" % debug)
    elif debug.startswith("remove"):
        app.log.debug("remove op, ignored")
    else:
        app.log.warn("Unknown Op in %s", debug)

@cli.log.LoggingApp
def mongophile(app):
    if not app.params.file or app.params.explain:
        assert app.params.host, "MongoDB (--host) Hostname must be defined."
        assert app.params.port, "MongoDB (--port) Port must be defined."
        assert app.params.db, "MongoDB (--db) Database must be defined."
        mongo = pymongo.Connection(app.params.host, app.params.port)
        useServer = True

    if app.params.file:
        app.log.warn("File input mode.  Skipping server read, sourcing JSON from '%s'." % app.params.file)
        data = json.load(open(app.params.file, 'r'))
    else:
        app.log.warn("Will read data from server connection.")
        data = mongo['system.profile'].find()

    if app.params.explain:
        raise Exception("Explain mode not currently supported")

    #app.log.debug("Data: %s", data)
    app.log.info("Loaded %d profile entries.", len(data))

    ops = []
    for entry in data:
        obj = parseEntry(app, entry)
        if obj:
            ops.append(obj)

    print "Parsed %d ops." % len(ops)
    totalMS = reduce(lambda x, y: x + y.millis, ops, 0L)
    print "Total Milliseconds: %d" % totalMS
    sortedOps = sorted(ops, lambda x, y: cmp(y.millis, x.millis))
    print sortedOps[0:10]
    nonOptimalQueries = filter(lambda x: isinstance(x, MongoQuery) and x.scanRatio < 100, ops)
    print "Non Optimal Queries: %s" % [x.scanRatio for x in nonOptimalQueries]


mongophile.add_param("-x", "--host", help="MongoDB host to read from", default="localhost")
mongophile.add_param("-p", "--port", help="MongoDB port to read from",
default=27017)
mongophile.add_param("-d", "--db", help="MongoDB Database to read from")
mongophile.add_param("-f", "--file", help="File to read from (Optional, parses JSON instead of connecting to MongoDB)")
mongophile.add_param("-e", "--explain", help="Attempt to explain each query.  If using a file, specify host, port, db to use this", default=False, action="store_true")

if __name__ == "__main__":
    mongophile.run()

