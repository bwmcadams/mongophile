import pymongo
import re

import cli.log


try:
    import json
except:
    import simplejson as json


from itertools import *
from collections import defaultdict
from ops import *

insert_re = re.compile("^insert\s((.*)\.([0-9a-zA-Z$._]*))")
query_re = re.compile("^query\s((.*)\.([0-9a-zA-Z$._]*))\s*(ntoreturn:(\d*))?(scanAndOrder\s)?\sreslen:(\d*)\snscanned:(\d*)\s*(query:\s({.*}))?\s*nreturned:(\d*)")
update_re = re.compile("^update\s((.*)\.([0-9a-zA-Z$._]*))\s*query:\s({.*})\snscanned:(\d*)\s*(fastmod|fastmodinsert|upsert|moved)")
cmd_re = re.compile("^query\s(.*)\.\\$cmd\sntoreturn:(\d*)\scommand:\s({.*})\sreslen:(\d*)")
# TODO getmore

class ProfilerParser(object):

    op_list = []

    noops = 0 # Ops we don't care about(E.G. insert)
    badops = 0 # Ops we couldn't parse (even if they were noops)

    def __init__(self, app, data):
        """ Initialize the parser.
        DataHandle should be an iterable of Python dictionary
        representations of profiler log entries.

        A pymongo cursor to the profile collection works too.
        """
        assert hasattr(data, "__iter__"), "Data must be an iterable object."
        self.app = app
        self.input_data = data
        app.log.debug("Loaded %d profile entries.", len(data))
        for entry in data:
            obj = self.parseEntry(entry)
            if obj:
                self.op_list.append(obj)
        self.ordered_op_list = self.op_list
        self.op_list = sorted(self.op_list, lambda x, y: cmp(y.millis, x.millis))
        app.log.info("Read %d Ops, and parsed %d properly.  %d were NoOps (things we don't care about) and %d failed to parse." % (len(data), len(self.op_list), self.noops, self.badops))
        self.ops = defaultdict(list)
        for k, g in groupby(self.op_list, key=lambda x: x.opType):
            self.ops[k].append(list(g))


    def parseEntry(self, entry):
        self.app.log.debug("TS: %s Millis: %d", entry['ts'], entry['millis'])
        # parse the debug block
        info = entry['info']
        if info.startswith("insert"):
            return self.insertParse(info, entry['ts'], entry['millis'])
        elif info.startswith("query"):
            self.app.log.debug("Query Op")
            if info.find("$cmd") >= 0:
                return self.cmdParse(info, entry['ts'], entry['millis'])
            else:
                return self.queryParse(info, entry['ts'], entry['millis'])
        elif info.startswith("update"):
            return self.updateParse(info, entry['ts'], entry['millis'])
        elif info.startswith("remove"):
            self.app.log.debug("remove op, ignored")
            self.noops += 1
        else:
            self.app.log.warn("Unknown Op in %s", info)
            self.noops += 1

    def insertParse(self, info, ts, millis):
            self.app.log.debug("Insert Op")
            data = insert_re.search(info)
            if data:
                db = data.group(2)
                coll = data.group(3)
                #self.app.log.debug("DB: %s Coll: %s", db, coll)
                self.noops += 1
            else:
                self.app.log.info("Failed to match RegEx on insert '%s'", info)
                self.badops += 1

    def cmdParse(self, info, ts, millis):
        self.app.log.debug("Command Op")
        data = cmd_re.search(info)
        if data:
            db = data.group(1)
            ntoreturn = data.group(2)
            command = data.group(3)
            reslen = data.group(4)
            self.app.log.debug("[Command] DB: %s NToReturn: %s Command: %s ResLen:%s", db, ntoreturn, command, reslen)
            return MongoCommand(self.app.log, ts, millis, db, ntoreturn, command, reslen)
        else:
            self.app.log.info("Failed to match RegEx on command '%s'" % info)
            self.badops += 1

    def queryParse(self, info, ts, millis):
        data = query_re.search(info)
        if data:
            db = data.group(2)
            coll = data.group(3)
            ntoreturn = data.group(5)
            scanAndOrder = data.group(6)
            reslen = data.group(7)
            nscanned = data.group(8)
            query = data.group(10)
            nreturned = data.group(11)
            self.app.log.debug("[Query] DB: %s Coll: %s Scan And Order?: %s NToReturn: %s ResLen:%s NScanned:%s Query:%s NReturned:%s", db, coll, scanAndOrder, ntoreturn, reslen, nscanned, query, nreturned)
            return MongoQuery(self.app.log, ts, millis, db, coll, ntoreturn, scanAndOrder, reslen, nscanned, query, nreturned)
        else:
            self.app.log.info("Failed to match RegEx on query '%s'" % info)
            self.badops += 1

    def updateParse(self, info, ts, millis):
        self.app.log.debug("Update Op")
        data = update_re.search(info)
        if data:
            db = data.group(2)
            coll = data.group(3)
            query = data.group(4)
            nscanned = data.group(5)
            opType = data.group(6)
            self.app.log.debug("[Update] DB: %s Coll: %s NScanned: %s OpType: %s Query: %s", db, coll, nscanned, opType, query)
            return MongoUpdate(self.app.log, ts, millis, db, coll, query, nscanned, opType)
        else:
            self.app.log.info("Failed to match RegEx on update '%s'" % info)
            self.noobadops += 1


@cli.log.LoggingApp
def mongophile(app):
    def arg_assert(test, msg):
        if not test:
            app.argparser.error(msg)

    if not app.params.file or app.params.explain:
        arg_assert(app.params.host, "MongoDB (--host) Hostname must be defined with non-file or explain mode..")
        arg_assert(app.params.port, "MongoDB (--port) Port must be defined with non-file or explain mode..")
        arg_assert(app.params.db, "MongoDB (--db) Database must be defined with non-file or explain mode..")
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
    print "Read %d Ops, and parsed %d properly.  %d were NoOps (things we don't care about) and %d failed to parse." % (len(parser.input_data), len(parser.op_list), parser.noops, parser.badops)
    print "Top 10 Slowest Operations..."
    for op in parser.op_list[0:10]:
        print "\t%s\n" % op


mongophile.add_param("-x", "--host", help="MongoDB host to read from", default="localhost")
mongophile.add_param("-p", "--port", help="MongoDB port to read from",
default=27017)
mongophile.add_param("-d", "--db", help="MongoDB Database to read from")
mongophile.add_param("-f", "--file", help="File to read from (Optional, parses JSON instead of connecting to MongoDB)")
mongophile.add_param("-e", "--explain", help="Attempt to explain each query.  If using a file, specify host, port, db to use this", default=False, action="store_true")

def main():
    mongophile.run()

