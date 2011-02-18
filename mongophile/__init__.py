#!/usr/bin/env python

import pymongo

import cli.log

try:
    import json
except:
    import simplejson as json


from parser import ProfilerParser
from ops import *

@cli.log.LoggingApp
def mongophile(app):
    if not app.params.file or app.params.explain:
        assert app.params.host, "MongoDB (--host) Hostname must be defined."
        assert app.params.port, "MongoDB (--port) Port must be defined."
        assert app.params.db, "MongoDB (--db) Database must be defined."
        mongo = pymongo.Connection(app.params.host, app.params.port)
        useServer = True

    if app.params.file:
        app.log.info("File input mode.  Skipping server read, sourcing JSON from '%s'." % app.params.file)
        fh = open(app.params.file, 'r')
        try:
            data = json.load(fh)
        except:
            app.log.warn("Parsing File as a JSON array failed.  Trying to parse it as multiple JSON entries, one per line.")
            data = []
            fh.seek(0)
            for line in fh:
                data.append(json.loads(line))
    else:
        app.log.warn("Will read data from server connection.")
        data = mongo['system.profile'].find()

    if app.params.explain:
        raise Exception("Explain mode not currently supported")

    app.log.debug("Data: %s", data)

    parser = ProfilerParser(app, data)
    #totalMS = reduce(lambda x, y: x + y.millis, parser.ops, 0L)
    #print "Total Milliseconds: %d" % totalMS
    #nonOptimalQueries = filter(lambda x: isinstance(x, MongoQuery) and x.scanRatio < 100, parser.ops)


mongophile.add_param("-x", "--host", help="MongoDB host to read from", default="localhost")
mongophile.add_param("-p", "--port", help="MongoDB port to read from",
default=27017)
mongophile.add_param("-d", "--db", help="MongoDB Database to read from")
mongophile.add_param("-f", "--file", help="File to read from (Optional, parses JSON instead of connecting to MongoDB)")
mongophile.add_param("-e", "--explain", help="Attempt to explain each query.  If using a file, specify host, port, db to use this", default=False, action="store_true")

if __name__ == "__main__":
    mongophile.run()

