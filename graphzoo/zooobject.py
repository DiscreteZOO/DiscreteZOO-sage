import graphzoo
from query import A as All
from query import And
from query import Column
from query import Count
from query import Table
from utility import lookup
from utility import todict
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

    def _getprops(self, cl):
        return self.__getattribute__(cl._spec["dict"])

    def _setprops(self, cl, d):
        return self.__setattr__(cl._spec["dict"], d)

    def _init_props(self, cl, props):
        if props is not None:
            self._setprops(cl, self._todict(props, skip = cl._spec["skip"],
                                            fields = cl._spec["fields"]))
            props = {k: v for k, v in props.items()
                     if k not in cl._spec["fields"]}
        return props

    def _db_read(self, cl, join = None, query = None):
        if query is None:
            if self._zooid is None:
                raise IndexError("graph id not given")
            query = {"id": self._zooid}
        t = Table(cl._spec["name"])
        if join is None:
            join = t
        cur = self._db.query([t], join, query)
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(query)
        self._setprops(cl, self._todict(r, skip = cl._spec["skip"],
                                        fields = cl._spec["fields"]))
        return r

    def _db_write(self, cl, cur):
        id = None
        if cl._parent is None:
            id = cl._spec["primary_key"]
        self._db.insert_row(cl._spec["name"],
                            dict(self._getprops(cl).items() + \
                                 [(k, self.__getattribute__(k)())
                                  for k in cl._spec["skip"]]),
                            cur = cur, id = id)
        if id is not None:
            self._zooid = self._db.lastrowid(cur)

    def _todict(self, r, skip = [], fields = None):
        if fields is None:
            fields = self._spec["fields"]
        return {k: self._db.from_db_type(r[k],
                                lookup(fields, k, default = type(r[k])))
                for k in r.keys() if k in fields and k not in skip
                                     and r[k] is not None}

    def load_db_data(self):
        cl = self.__class__
        while cl is not None:
            cl._db_read(self)
            cl = cl._parent

    def id(self):
        return self._zooid

class ZooInfo:
    cl = None

    def __init__(self, cl, db = None):
        self.cl = cl

    def __repr__(self):
        return "<%s at 0x%08x>" % (str(self), id(self))

    def __str__(self):
        return "Info object for %s" % self.cl

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

    def count(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        join = lookup(kargs, "join", default = None, destroy = True)
        by = lookup(kargs, "by", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = t.join(join, by = by)
        if self.cl._parent is None:
            groupby = lookup(kargs, "groupby", default = [], destroy = True)
            if isinstance(groupby, set):
                groupby = list(groupby)
            elif not isinstance(groupby, list):
                groupby = [groupby]
            groupbycols = [Column(x, alias = True) for x in groupby]
            cur = db.query(columns = [Count(All)] + groupbycols, table = t,
                           cond = And(*largs, **kargs), groupby = groupby)
            n = cur.fetchall()
            cur.close()
            return tomultidict(n, groupbycols)
        else:
            return ZooInfo(self.cl._parent).count(db = db, join = t,
                                        by = {self.cl._spec["primary_key"]},
                                        *largs, **kargs)

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
            return db.query(columns = [All], table = t,
                            cond = And(*largs, **kargs), orderby = orderby,
                            limit = limit,  offset = offset, cur = cur)
        else:
            return ZooInfo(self.cl._parent).query(db = db, join = t,
                                        by = {self.cl._spec["primary_key"]},
                                        *largs, **kargs)

    def props(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (todict(r, db) for r in cur)

    def all(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (self.cl(todict(r, db), db = db) for r in cur)

    def one(self, *largs, **kargs):
        kargs["limit"] = 1
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db =db, *largs, **kargs)
        r = cur.fetchone()
        if r is None:
            raise KeyError(kargs)
        return self.cl(todict(r, db), db = db)
