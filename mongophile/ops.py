try:
    import json
except:
    import simplejson as json

class MongoOp(object):
    pass

class MongoInsert(MongoOp):
    pass

class MongoQuery(MongoOp):

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
    def __init__(self, log, ts, ms, db, coll, query,
                 nscanned, opType):
        self.log = log
        self.db = db
        self.coll = coll
        self.ts = ts
        self.ms = ms
        self.query = query
        self.nscanned = nscanned
        self.opType = opType

        #try:
            #self.query = json.loads(self.rawQuery)
        #except Exception, e:
            #log.error("Cannot parse raw query '%s' into a dictionary. Error: %s" % (query, e))

