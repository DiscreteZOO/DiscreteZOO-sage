import graphzoo
from query import All
from query import And
from query import Count
from query import Table
from utility import drop_none
from utility import lookup
from utility import tomultidict

class ZooObject:
    _spec = None
    _db = None
    _zooid = None
    _parent = None

    def __init__(self, db = None):
        if db is None:
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
                                lookup(fields, k, default = type(r[k])))
                for k in r.keys() if k in fields and k not in skip
                                     and r[k] is not None}

class ZooInfo:
    cl = None

    def __init__(self, cl, db = None):
        self.cl = cl

    def getdb(self):
        if self.cl._db is not None:
            return self.cl._db
        return graphzoo.DEFAULT_DB

    def initdb(self, db = None, commit = True):
        if db is None:
            db = self.getdb()
        for base in self.cl.__bases__:
            if issubclass(base, ZooObject):
                ZooInfo(base).initdb(db = db, commit = False)
        if self.cl._spec is not None:
            db.init_table(self.cl._spec, commit = commit)

    def count(self, db = None, groupby = set(), join = None, by = None,
              sub = {}, subgroup = set(), groupby_orig = None, *largs,
              **kargs):
        if db is None:
            db = self.getdb()
        if type(groupby) is not set:
            if type(groupby) is not list:
                groupby = [groupby]
            if groupby_orig is None:
                groupby_orig = groupby
            groupby = set(groupby)
        elif groupby_orig is None:
            groupby_orig = list(groupby)
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = t.join(join, by = by)
        outk = {k for k in kargs if k not in self.cl._spec["fields"]}
        outg = {k for k in groupby if k not in self.cl._spec["fields"]}
        if len(outk) == 0 and len(outg) == 0:
            grp = groupby.union(subgroup)
            cur = db.query(columns = [Count()] + list(grp), table = t,
                           cond = dict(kargs.items() + sub.items()),
                           groupby = grp)
            n = cur.fetchall()
            cur.close()
            return tomultidict(n, groupby_orig)
        else:
            if self.cl._parent is None:
                raise KeyError
            return ZooInfo(self.cl._parent).count(db,
                    groupby = set(outg), join = t,
                    by = {self.cl._spec["primary_key"]},
                    sub = dict(sub.items() + [(k, v) for k, v in kargs.items()
                                              if k not in outk]),
                    subgroup = subgroup.union(k for k in groupby
                                              if k not in outg),
                    groupby_orig = groupby_orig,
                    **dict([(k, kargs[k]) for k in outk]))

    def query(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        join = lookup(kargs, "join", default = None, destroy = True)
        by = lookup(kargs, "by", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = t.join(join, by = by)
        if self.cl._parent is None:
            cur = lookup(kargs, "cur", default = None, destroy = True)
            orderby = lookup(kargs, "orderby", default = [], destroy = True)
            limit = lookup(kargs, "limit", default = None, destroy = True)
            offset = lookup(kargs, "offset", default = None, destroy = True)
            return db.query(columns = [All()], table = t,
                            cond = And(*largs, **kargs), orderby = orderby,
                            limit = limit,  offset = offset, cur = cur)
        else:
            return ZooInfo(self.cl._parent).query(db = db, join = t,
                                        by = {self.cl._spec["primary_key"]},
                                        *largs, **kargs)

    def all(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (self.cl(drop_none(r), db = db) for r in cur)

    def one(self, *largs, **kargs):
        kargs["limit"] = 1
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db =db, *largs, **kargs)
        r = cur.fetchone()
        if r is None:
            raise KeyError(kargs)
        return self.cl(drop_none(r), db = db)
