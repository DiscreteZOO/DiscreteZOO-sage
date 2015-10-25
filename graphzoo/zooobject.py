import graphzoo
from query import Count
from query import Table
from utility import lookup

class ZooObject:
    _spec = None
    _db = None
    _zooid = None
    _parent = None

    def __init__(self, db = None):
        if db == None:
            self._db = graphzoo.DEFAULT_DB
        else:
            self._db = db

    def setdb(self, db):
        self._db = db

    def initdb(self, db = None):
        _initdb(self.__class__, self._db)

    def _todict(self, r, skip = [], fields = None):
        if fields is None:
            fields = self._spec["fields"]
        return {k: self._db.from_db_type(r[k],
                                lookup(fields, k, type(r[k])))
                for k in r.keys() if k not in skip and r[k] is not None}

class ZooInfo:
    cl = None

    def __init__(self, cl, db = None):
        self.cl = cl
        self.updatedb(db)

    def updatedb(self, db = None):
        if db is not None:
            self.cl._db = db
        elif self.cl._db is None:
            self.cl._db = graphzoo.DEFAULT_DB
        return self.cl._db

    def initdb(self, db = None, commit = True):
        if db is None:
            db = self.updatedb()
        for base in self.cl.__bases__:
            if issubclass(base, ZooObject):
                ZooInfo(base).initdb(db = db, commit = False)
        if self.cl._spec is not None:
            db.init_table(self.cl._spec, commit = commit)

    def count(self, db = None, join = None, by = None, sub = {}, **kargs):
        if db is None:
            db = self.updatedb()
        t = Table(self.cl._spec["name"])
        if join is None:
            join = t
        else:
            t.join(join, by = by)
        if all(k in self.cl._spec["fields"] for k in kargs):
            cur = db.query([Count()], t, dict(kargs.items() + sub.items()))
            n = cur.fetchone()[0]
            cur.close()
            return n
        else:
            if self.cl._parent is None:
                raise KeyError
            return ZooInfo(self.cl._parent).count(db, join = t,
                                    by = {self.cl._spec["primary_key"]},
                                    sub = dict(sub.items() +
                                        [(k, v) for k, v in kargs.items()
                                        if k in self.cl._spec["fields"]]),
                                    **dict([(k, v) for k, v in kargs.items()
                                        if k not in self.cl._spec["fields"]]))
