import graphzoo
from query import Count
from query import Table
from utility import lookup
from utility import tomultidict

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

    def count(self, db = None, groupby = set(), join = None, by = None,
              sub = {}, subgroup = set(), groupby_orig = None, **kargs):
        if db is None:
            db = self.updatedb()
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
                           query = dict(kargs.items() + sub.items()),
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
