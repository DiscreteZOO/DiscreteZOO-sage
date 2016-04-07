import discretezoo
from .. import zootypes
from ...db.query import A as All
from ...db.query import And
from ...db.query import Column
from ...db.query import Count
from ...db.query import In
from ...db.query import R as Random
from ...db.query import Subquery
from ...db.query import Table
from ...util.utility import default
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import todict
from ...util.utility import tomultidict

class ZooEntity(object):
    _baseprops = None
    _spec = None
    _db = None
    _dict = "_baseprops"
    _fields = None
    _parent = None
    _extra_classes = None

    def __init__(self, data = None, **kargs):
        self._init_(ZooEntity, kargs, setVal = {"data": data})

    def _init_(self, cl, d, defNone = [], defVal = {}, setVal = {},
               setProp = {}):
        self._extra_classes = set()
        cl._init_defaults(self, d)
        for k in defNone:
            default(d, k)
        for k, v in defVal.items():
            default(d, k, v)
        for k, v in setVal.items():
            d[k] = v
        default(d, "db")
        default(d, "cur")
        default(d, "commit")
        default(d, "store", discretezoo.WRITE_TO_DB)
        self._initdb(d["db"])
        if self.__class__ is cl:
            d["write"] = {}
        d["write"][cl] = d["store"]
        if d["store"] and d["cur"] is None:
            d["cur"] = self._db.cursor()
            if d["commit"] is None:
                d["commit"] = True
        if not cl._parse_params(self, d):
            self._init_params(d)
        self._init_props(cl)
        cl._init_object(self, cl, d, setProp)
        if self._zooid is not False and d["write"][cl]:
            self._compute_props(cl, d, setProp)
            self._db_write(cl, d["cur"])
        if self.__class__ is cl and d["commit"]:
            self._db.commit()

    def setdb(self, db):
        self._db = db

    def _initdb(self, db = None):
        if self._db is not None:
            return
        if db is None:
            self._db = discretezoo.DEFAULT_DB
        else:
            self._db = db

    def _getclass(self, attr, alias = False):
        c = self.__class__
        while c is not None:
            if attr in c._spec["fields"]:
                if alias:
                    return (c, attr)
                else:
                    return c
            if alias and attr in c._spec["aliases"]:
                return (c, c._spec["aliases"][attr])
            c = c._parent
        for c in self._extra_classes:
            if attr in c._spec["fields"]:
                if alias:
                    return (c, attr)
                else:
                    return c
            if alias and attr in c._spec["aliases"]:
                return (c, c._spec["aliases"][attr])
        raise KeyError(attr)

    def _getprops(self, cl):
        if isinstance(cl, type):
            return self.__getattribute__(cl._dict)
        c = self.__class__
        while c is not None:
            if cl in c._spec["fields"]:
                return self.__getattribute__(c._dict)
            c = c._parent
        for c in self._extra_classes:
            if cl in c._spec["fields"]:
                return self.__getattribute__(c._dict)
        raise KeyError(cl)

    def _setprops(self, cl, d):
        try:
            props = self.__getattribute__(cl._dict)
        except AttributeError:
            props = None
        if props is None:
            self.__setattr__(cl._dict, d)
        else:
            props.update(d)

    def _init_defaults(self, d):
        pass

    def _init_skip(self, d):
        pass

    def _parse_params(self, d):
        if isinteger(d["data"]):
            d["zooid"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _init_params(self, d):
        pass

    def _init_object(self, cl, d, setProp = {}):
        if self._zooid is None:
            self._zooid = d["zooid"]
        if self._zooid is None:
            try:
                r = self._db_read(cl, kargs = d)
                self._zooid = r["zooid"]
            except KeyError as ex:
                if not d["store"]:
                    raise ex

    def _init_props(self, cl):
        c = cl
        while c is not None:
            self._setprops(c, {})
            c = c._parent

    def _default_props(self, cl):
        for c, m in cl._spec["default"].items():
            self._getprops(c).update(m)

    def _apply_props(self, cl, d):
        if d["props"] is not None:
            self._init_skip(d)
            self._setprops(cl, self._todict(d["props"],
                                            skip = cl._spec["skip"],
                                            fields = cl._spec["fields"]))
            d["props"] = {k: v for k, v in d["props"].items()
                            if k not in cl._spec["fields"]
                                or k in cl._spec["skip"]}
            d["write"][cl] = False

    def _compute_props(self, cl, d, setProp = {}):
        for c, m in cl._spec["condition"].items():
            for k, v in m.items():
                assert self.__getattribute__(k)(store = (c is not cl)) == v, \
                    "Attribute %s does not have value %s" % (k, v)
        self._default_props(cl)
        p = self._getprops(cl)
        for k, v in setProp.items():
            p[k] = d[v]
        for c, s in cl._spec["compute"].items():
            for k in s:
                self.__getattribute__(k)(store = (c is not cl))

    def _db_read(self, cl, join = None, query = None, cur = None,
                 kargs = None):
        if query is None:
            if self._zooid is None:
                if not cl._db_read_nonprimary(self, cur = cur):
                    raise KeyError("object id not given")
            query = {"zooid": self._zooid}
        t = Table(cl._spec["name"])
        if join is None:
            join = t
        cur = self._db.query([t], join, query, cur = cur)
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(query)
        self._setprops(cl, self._todict(r, skip = cl._spec["skip"],
                                        fields = cl._spec["fields"]))
        if kargs is not None and "write" in kargs:
            kargs["write"][cl] = False
        return r

    def _db_read_nonprimary(self, cur = None):
        return False

    def _db_write(self, cl, cur):
        id = None
        if cl._parent is None:
            id = cl._spec["primary_key"]
        row = dict(self._getprops(cl).items() +
                [(k, self.__getattribute__(k)()) for k in cl._spec["skip"]])
        if self._zooid is False and "zooid" in row:
            del row["zooid"]
        if "zooid" not in row or not self._update_rows(cl, row,
                                    {cl._spec["primary_key"]: row["zooid"]},
                                    noupdate = cl._spec["noupdate"],
                                    cur = cur):
            self._db.insert_row(cl._spec["name"], row, cur = cur, id = id)
            if id is not None:
                objid = self._db.lastrowid(cur)
                if self._zooid is not False:
                    self._zooid = objid
                return objid
            cl._add_change(self, cl, cur)
        cl._db_write_nonprimary(self, cur)

    def _db_write_nonprimary(self, cur = None):
        pass

    def _update_rows(self, cl, row, cond, noupdate = [], cur = None,
                     commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        self._db.query([Column(c) for c in
                        {cl._spec["primary_key"]}.union(row.keys())],
                       cl._spec["name"], cond, distinct = True, cur = cur)
        chg = False
        skip = set()
        rows = cur.fetchall()
        if len(rows) == 0:
            return False
        if len(noupdate) > 0:
            for r in rows:
                for k, v in row.items():
                    if k in noupdate and r[k] is not None and v != r[k]:
                        skip.add(k)
                        continue
            row = {k: v for k, v in row.items() if k not in skip}
        from ..change import Change
        for r in rows:
            for k, v in row.items():
                if v != r[k]:
                    Change(r[cl._spec["primary_key"]], cl, column = k,
                           cur = cur, db = self._db)
                    chg = True
        if chg:
            self._db.update_rows(cl._spec["name"], row, cond, cur = cur,
                                 commit = commit)
        return True

    def _add_change(self, cl, cur):
        pass

    def _todict(self, r, skip = [], fields = None):
        if fields is None:
            fields = self._spec["fields"]
        return {k: self._db.from_db_type(r[k],
                                lookup(fields, k, default = type(r[k])))
                for k in r.keys() if k in fields and k not in skip
                                     and r[k] is not None}

    @staticmethod
    def _get_column(cl, name, table, join = None, by = None):
        raise AttributeError("_get_column")

    def load_db_data(self):
        cl = self.__class__
        while cl is not None:
            cl._db_read(self)
            cl = cl._parent

    def zooid(self):
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
        return discretezoo.DEFAULT_DB

    def initdb(self, db = None, commit = True):
        if db is None:
            db = self.getdb()
        for base in self.cl.__bases__:
            if issubclass(base, ZooEntity):
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
            t = join.join(t, by = by)
        if self.cl._parent is None:
            groupby = lookup(kargs, "groupby", default = [], destroy = True)
            if isinstance(groupby, set):
                groupby = list(groupby)
            elif not isinstance(groupby, list):
                groupby = [groupby]
            groupbycols = [Column(x, alias = True) for x in groupby]
            cond = And(*largs, **kargs)
            cols = t.getTables()
            for table, j, b in cond.getTables():
                if table not in cols:
                    t = t.join(table, by = b)
            cur = db.query(columns = [Count(Column(self.cl._spec["primary_key"],
                                                   self.cl._spec["name"]),
                                            distinct = True)] + groupbycols,
                           table = t, cond = cond, groupby = groupby)
            n = cur.fetchall()
            cur.close()
            return tomultidict(n, groupbycols)
        else:
            return ZooInfo(self.cl._parent).count(db = db, join = t,
                                by = frozenset([self.cl._spec["primary_key"]]),
                                *largs, **kargs)

    def query(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        join = lookup(kargs, "join", default = None, destroy = True)
        by = lookup(kargs, "by", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = join.join(t, by = by)
        if self.cl._parent is None:
            cur = lookup(kargs, "cur", default = None, destroy = True)
            orderby = lookup(kargs, "orderby", default = [], destroy = True)
            limit = lookup(kargs, "limit", default = None, destroy = True)
            offset = lookup(kargs, "offset", default = None, destroy = True)
            random = lookup(kargs, "random", default = False, destroy = True)
            cond = And(*largs, **kargs)
            ct = cond.getTables()
            cols = t.getTables()
            columns = [Table(table) for table in cols]
            if random:
                orderby = [Random]
                columns.append(Column(Random, alias = "_rand"))
            if cols.issuperset({tbl for tbl, j, b in ct}):
                return db.query(columns = columns, table = t, cond = cond,
                                orderby = orderby, limit = limit,
                                offset = offset, distinct = True, cur = cur)
            else:
                tt = Table(t)
                for tbl, j, b in ct:
                    if tbl not in cols:
                        t = t.join(tbl, by = b)
                c = Column(self.cl._spec["primary_key"], self.cl._spec["name"])
                return db.query(columns = columns, table = tt,
                                cond = In(c, Subquery(columns = [c], table = t,
                                                      cond = cond)),
                                orderby = orderby, limit = limit,
                                offset = offset, distinct = True, cur = cur)
        else:
            return ZooInfo(self.cl._parent).query(db = db, join = t,
                                by = frozenset([self.cl._spec["primary_key"]]),
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
        cur = self.query(db = db, *largs, **kargs)
        r = cur.fetchone()
        if r is None:
            raise KeyError(largs, kargs)
        return self.cl(todict(r, db), db = db)

def initdb(db = None, commit = True):
    if db is None:
        db = discretezoo.DEFAULT_DB
    for cl in zootypes.names.values():
        if type(cl) is type and issubclass(cl, ZooEntity):
            ZooInfo(cl).initdb(db = db, commit = False)
    if commit:
        db.commit()
