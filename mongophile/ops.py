try:
    import json
except:
    import simplejson as json

class MongoOp(object):

    def __repr__(self):
        return "{ [MongoOp '%s'] %d ms }" % (self.opType, self.ms)

class MongoInsert(MongoOp):
    opType = "insert"

class MongoQuery(MongoOp):

    opType = "query"

    def __init__(self, log, ts, ms, db, coll, ntoreturn,
                 scanAndOrder, reslen, nscanned,
                 query, nreturned):
        self.log = log
        self.ts = ts
        self.ms = ms
        self.db = db
        self.coll = coll
        self.ntoreturn = ntoreturn
        self.scanAndOrder = scanAndOrder is None
        self.reslen = reslen
        self.nscanned = nscanned
        self.query = query
        self.nreturned = nreturned

        #try:
            #self.query = json.loads(self.rawQuery)
        #except Exception, e:
            #log.error("Cannot parse raw query '%s' into a dictionary. Error: %s" % (query, e))

class MongoCommand(MongoOp):

    opType = "command"

    def __init__(self, log, ts, ms, db, ntoreturn, command, reslen):
        self.log = log
        self.db = db
        self.ts = ts
        self.ms = ms
        self.ntoreturn = ntoreturn
        self.command = command
        self.reslen = reslen

        #try:
            #self.query = json.loads(self.rawCommand)
        #except Exception, e:
            #log.error("Cannot parse raw command '%s' into a dictionary. Error: %s" % (command, e))

class MongoUpdate(MongoOp):
    opType = "update"

    def __init__(self, log, ts, ms, db, coll, query,
                 nscanned, updateType):
        self.log = log
        self.db = db
        self.coll = coll
        self.ts = ts
        self.ms = ms
        self.query = query
        self.nscanned = nscanned
        self.updateType = updateType
        self.opType = self.opType + " {%s}" % updateType

        #try:
            #self.query = json.loads(self.rawQuery)
        #except Exception, e:
            #log.error("Cannot parse raw query '%s' into a dictionary. Error: %s" % (query, e))

