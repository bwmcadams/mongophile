#!/usr/bin/env python

import pymongo
import re

import cli.log

try:
    import json
except:
    import simplejson as json


insert_re = re.compile("^insert\s((.*)\.([0-9a-zA-Z$._]*))")
query_re = re.compile("^query\s((.*)\.([0-9a-zA-Z$._]*))\s*(ntoreturn:(\d*))?(scanAndOrder\s)?\sreslen:(\d*)\snscanned:(\d*)\s*query:\s({.*})\s*nreturned:(\d*)")
update_re = re.compile("^update\s((.*)\.([0-9a-zA-Z$._]*))\s*query:\s({.*})\snscanned:(\d*)\s*(fastmod|fastmodinsert|upsert|moved)")
cmd_re = re.compile("^query\s(.*)\.\\$cmd\sntoreturn:(\d*)\scommand:\s({.*})\sreslen:(\d*)")
# TODO getmore

def parseEntry(app, entry):
    app.log.debug("TS: %s Millis: %d", entry['ts'], entry['millis'])
    # parse the info block
    info = entry['info']
    if info.startswith("insert"):
        app.log.debug("Insert Op")
        data = insert_re.search(info)
        if data:
            db = data.group(2)
            coll = data.group(3)
            #app.log.info("DB: %s Coll: %s", db, coll)
        else:
            app.log.error("Failed to match RegEx on insert '%s'", info)
    elif info.startswith("query"):
        app.log.debug("Query Op")
        if info.find("$cmd") >= 0:
            app.log.debug("Command Op")
            data = cmd_re.search(info)
            if data:
                db = data.group(1)
                ntoreturn = data.group(2)
                command = data.group(3)
                reslen = data.group(4)
                app.log.info("\n\n[Command] DB: %s NToReturn: %s Command: %s ResLen:%s", db, ntoreturn, command, reslen)
            else:
                app.log.error("Failed to match RegEx on command '%s'" % info)
        else:
            data = query_re.search(info)
            if data:
                db = data.group(2)
                coll = data.group(3)
                ntoreturn = data.group(5)
                scanAndOrder = data.group(6)
                reslen = data.group(7)
                nscanned = data.group(8)
                query = data.group(9)
                nreturned = data.group(10)
                app.log.info("\n\n[Query] DB: %s Coll: %s Scan And Order?: %s NToReturn: %s ResLen:%s NScanned:%s Query:%s NReturned:%s", db, coll, scanAndOrder, ntoreturn, reslen, nscanned, query, nreturned)
            else:
                app.log.error("Failed to match RegEx on query '%s'" % info)
    elif info.startswith("update"):
        app.log.debug("Update Op")
        data = update_re.search(info)
        if data:
            db = data.group(2)
            coll = data.group(3)
            query = data.group(4)
            nscanned = data.group(5)
            opType = data.group(6)
            app.log.info("\n\n[Update] DB: %s Coll: %s NScanned: %s OpType: %s Query: %s", db, coll, nscanned, opType, query) 
        else:
            app.log.error("Failed to match RegEx on update '%s'" % info)
    elif info.startswith("remove"):
        app.log.debug("remove op, ignored")
    else:
        app.log.warn("Unknown Op in %s", info)

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

    #app.log.debug("Data: %s", data)
    app.log.info("Loaded %d profile entries.", len(data))

    for entry in data:
        parseEntry(app, entry)

mongophile.add_param("-x", "--host", help="MongoDB host to read from", default="localhost")
mongophile.add_param("-p", "--port", help="MongoDB port to read from",
default=27017)
mongophile.add_param("-d", "--db", help="MongoDB Database to read from")
mongophile.add_param("-f", "--file", help="File to read from (Optional, parses JSON instead of connecting to MongoDB)")
mongophile.add_param("-e", "--explain", help="Attempt to explain each query.  If using a file, specify host, port, db to use this", default=False, action="store_true")

if __name__ == "__main__":
    mongophile.run()

