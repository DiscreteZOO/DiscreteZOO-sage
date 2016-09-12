r"""
A superclass for all DiscreteZOO entities

This module contains a class which all DiscreteZOO entities extend,
and also a class for making queries about entities of a given class.
"""

from sage.rings.integer import Integer
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
    r"""
    A class which all DiscreteZOO entities extend.

    Each entity is identified by a unique ID in the database.
    Both data and metadata can be entities.
    """
    _baseprops = None
    _spec = None
    _db = None
    _dict = "_baseprops"
    _fields = None
    _parent = None
    _extra_classes = None

    def __init__(self, data = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the entity from.

        - ``db`` - the database being used (must be a named parameter;
          default: ``None``).

        - ``store`` - whether to store the data in ``vals`` to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (must be a named parameter; default: ``None``).

        - other named parameters accepted by subclasses.
        """
        self._init_(ZooEntity, kargs, setVal = {"data": data})

    def _init_(self, cl, d, defNone = [], defVal = {}, setVal = {},
               setProp = {}):
        r"""
        Generic entity constructing method.

        This method should be called by the constructor of each class
        extending ``ZooEntity``. All other methods used in construction
        are then called (directly or indirectly) from this method.

        INPUT:

        - ``cl`` - the (super)class of the object currently being constructed.

        - ``d`` - a dictionary containing parameters passed from the
          constructor.

        - ``defNone`` - a list of parameters which default to ``None``
           (default: ``[]``).

        - ``defVal`` - a dictionary mapping parameters to their default
          values  (default: ``{}``).

        - ``setVal`` - a dictionary mapping parameters to the value they
          should take, overriding any previously set values  (default: ``{}``).

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from  (default: ``{}``).
        """
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
        r"""
        Set the database.

        INPUT:

        - ``db`` -- the database being used.
        """
        self._db = db

    def _initdb(self, db = None):
        r"""
        Initialize the database.

        If no database is specified, the default database is used.

        INPUT:

        - ``db`` (default: ``None``) -- the database being used.
        """
        if self._db is not None:
            return
        if db is None:
            self._db = discretezoo.DEFAULT_DB
        else:
            self._db = db

    def _getclass(self, attr, alias = False):
        r"""
        Return the class a field belongs to.

        Raise ``KeyError`` if the field is not found.

        INPUT:

        - ``attr`` - the name of the field to find the class for.

        - ``alias`` (default: ``False``) - if ``True``, also look for the
          field among aliases, and return a tuple containing the class and
          the canonical name of the field.
        """
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
        r"""
        Return the property dictionary of a class.

        If a class is given, its property dictionary is returned. If a field
        name is given, return a property dictionary of the class the field
        belongs to. If the field is not found, raise a ``KeyError``.

        INPUT:

        - ``cl`` - the dictionary or field name to return the property
          dictionary for.
        """
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
        r"""
        Set the given properties for a class.

        If ``cl`` does not yet have a property dictionary,
        initialize it with a copy of ``d``.
        Otherwise, update it with ``d``.

        INPUT:

        - ``cl`` - the class to set the properties for.

        - ``d`` - the dictionary of properties to set.
        """
        try:
            props = self.__getattribute__(cl._dict)
        except AttributeError:
            props = None
        if props is None:
            self.__setattr__(cl._dict, dict(d))
        else:
            props.update(d)

    def _init_defaults(self, d):
        r"""
        Initialize the default parameters.

        To be overridden.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        pass

    def _init_skip(self, d):
        r"""
        Initialize the properties to be stored separately.

        To be overridden.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        pass

    def _parse_params(self, d):
        r"""
        Parse the ``data`` parameter of the constructor.

        Tries to parse ``d["data"]`` as the ID of the entity, returning
        ``True`` on success. On failure, returns ``False``, indicating that
        class-specific parsing is necessary.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if isinteger(d["data"]):
            d["zooid"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _init_params(self, d):
        r"""
        Class-specific parsing of the ``data`` parameter of the constructor
        after a generic parsing fails.

        To be overridden.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        pass

    def _init_object(self, cl, d, setProp = {}):
        r"""
        Initialize the object being represented.

        Read the properties belonging to the given class from the database.
        If not found and the object is not going to be written to the database,
        raise a ``KeyError``.

        INPUT:

        - ``cl`` - the class to initialize the object for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
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
        r"""
        Initialize the property dictionary of ``cl`` and its superclasses.

        INPUT:

        - ``cl`` - the class to initialize the dictionaries for.
        """
        c = cl
        while c is not None:
            self._setprops(c, {})
            c = c._parent

    def _default_props(self, cl):
        r"""
        Set the default properties from the class specification.

        INPUT:

        - ``cl`` - the class to set the default properties for.
        """
        for c, m in cl._spec["default"].items():
            self._getprops(c).update(m)

    def _apply_props(self, cl, d):
        r"""
        Update the property dictionary of ``cl`` with the appropriate
        properties in ``d["props"]``.

        Used to set properties read from the database, so they won't be
        attempted to be written back. Before updating, initializes the
        properties to be stored separately.

        INPUT:

        - ``cl`` - the class to set the properties for.

        - ``d`` - the dictionary of parameters.
        """
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
        r"""
        Compute the properties required by the class specification.

        INPUT:

        - ``cl`` - the class to compute the properties for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        cl._check_conditions(self, cl, d)
        self._default_props(cl)
        p = self._getprops(cl)
        for k, v in setProp.items():
            p[k] = d[v]
        for c, s in cl._spec["compute"].items():
            for k in s:
                self.__getattribute__(k)(store = (c is not cl))

    def _check_conditions(self, cl, d):
        r"""
        Check the necessary conditions required by the class specification.

        Raise ``AssertionError`` on failure to meet the conditions.

        INPUT:

        - ``cl`` - the class to compute the properties for.

        - ``d`` - the dictionary of parameters.
        """
        for c, m in cl._spec["condition"].items():
            for k, v in m.items():
                assert self.__getattribute__(k)(store = (c is not cl)) == v, \
                    "Attribute %s does not have value %s" % (k, v)

    def _db_read(self, cl, join = None, query = None, cur = None,
                 kargs = None):
        r"""
        Read properties from the database.

        The row to be read is identified by the ID.
        If no ID is given or a corresponding row is not found,
        raise a ``KeyError``.

        INPUT:

        - ``cl`` - the class to read the properties for.

        - ``join`` - the table to read from. The default value of ``None``
          means that the table is determined by ``cl``.

        - ``query`` - the conditions that the row must satisfy. The default
          value of ``None`` means that the row will be identified by its ID.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``kargs`` - the dictionary of parameters (default: ``None``).
        """
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
        r"""
        Read properties from the database identified by something other than
        the ID of the entity.

        Returns whether the query was successful.
        This instance performs no query and always returns ``False``.

        INPUT:

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).
        """
        return False

    def _db_write(self, cl, cur):
        r"""
        Write properties to the database.

        The properties belonging to class ``cl`` are written to the database
        using the cursor ``cur``.

        INPUT:

        - ``cl`` - the class to write the properties for.

        - ``cur`` - the cursor to use for database interaction.
        """
        from ..zooproperty import ZooProperty
        id = None
        if cl._parent is None:
            id = cl._spec["primary_key"]
        row = dict(self._getprops(cl).items() +
                [(k, self.__getattribute__(k)(store = False))
                 for k in cl._spec["skip"]])
        row = {k: v for k, v in row.items()
               if not issubclass(cl._spec['fields'][k], ZooProperty)}
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

    def _db_write_nonprimary(self, cur):
        r"""
        Write additional properties to the database.

        To be overridden.

        INPUT:

        - ``cur`` - the cursor to use for database interaction.
        """
        pass

    def _update_rows(self, cl, row, cond, noupdate = [], cur = None,
                     commit = None):
        r"""
        Update rows satisfying the given conditions.

        INPUT:

        - ``cl`` - the class to update the rows for.

        - ``row`` - a dictionary mapping field names to new values of the
          corresponding properties.

        - ``cond`` - the condition that should hold for the rows to be updated.

        - ``noupdate`` - a list of field names which should not be updated
          even when they appear in ``row``.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (default: ``None``).
        """
        from ..change import Change
        from ..zooproperty import ZooProperty
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        row = {k: v for k, v in row.items()
               if not issubclass(cl._spec['fields'][k], ZooProperty)}
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
        r"""
        Record a change after adding the entity to the database.

        To be overriden.

        INPUT:

        - ``cl`` - the class to write the properties for.

        - ``cur`` - the cursor to use for database interaction.
        """
        pass

    def _todict(self, r, skip = [], fields = None):
        r"""
        Return a dictionary containing the relevant properties.

        INPUT:

        - ``r`` - a dictionary of properties.

        - ``skip`` - a list of fields to be skipped from the output
          (default: ``[]``).

        - ``fields`` - if specified, the list of fields to be included.
          The default value of ``None`` means that the list should be taken
          from the class specification.
        """
        if fields is None:
            fields = self._spec["fields"]
        return {k: self._db.from_db_type(r[k],
                                lookup(fields, k, default = type(r[k])))
                for k in r.keys() if k in fields and k not in skip
                                     and r[k] is not None}

    @staticmethod
    def _get_column(cl, name, table, join = None, by = None):
        r"""
        Return a ``Column`` object for a property of the given class.

        Not implemented for entities, so this method gets hidden by raising an
        ``AttributeError``.

        INPUT:

        - ``cl`` - the class to get the object for.

        - ``name`` - the name of the property.

        - ``table`` - the table containing the objects whose properties are
          represented by ``cl``.

        - ``join`` - a join of tables needed to determine the object
          (default: ``None``).

        - ``by`` - the criterion to join by (default: ``None``).
          See ``db.query.Table.join`` for more information.
        """
        raise AttributeError("_get_column")

    def load_db_data(self):
        r"""
        Load data from the database.
        """
        cl = self.__class__
        while cl is not None:
            cl._db_read(self)
            cl = cl._parent

    def zooid(self, **kargs):
        r"""
        Return the ID from the database.
        """
        return self._zooid

class ZooInfo:
    r"""
    A class for queries on DiscreteZOO objects.
    """
    cl = None

    def __init__(self, cl):
        r"""
        Object constructor.

        INPUT:

        - ``cl`` - the class to make queries on.
        """
        self.cl = cl

    def __repr__(self):
        return "<%s at 0x%08x>" % (str(self), id(self))

    def __str__(self):
        return "Info object for %s" % self.cl

    def getdb(self):
        r"""
        Return a database to be used.
        """
        if self.cl._db is not None:
            return self.cl._db
        return discretezoo.DEFAULT_DB

    def initdb(self, db = None, commit = True):
        r"""
        Initialize tables for the class.

        INPUT:

        - ``db`` - the database being used (default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (default: ``True``).
        """
        if db is None:
            db = self.getdb()
        for base in self.cl.__bases__:
            if issubclass(base, ZooEntity):
                ZooInfo(base).initdb(db = db, commit = False)
        if self.cl._spec is not None:
            db.init_table(self.cl._spec, commit = commit)

    def count(self, *largs, **kargs):
        r"""
        Count objects satisfying the conditions.

        INPUT:

        - ``db`` - the database being used (must be a named parameter;
          default: ``None``).

        - ``join`` - a join of tables needed to determine the object
          (must be a named parameter; default: ``None``).

        - ``by`` - the criterion to join by (must be a named parameter;
          default: ``None``). See ``db.query.Table.join`` for more information.

        - ``groupby`` - an expression or list of expressions to group by
          (must be a named parameter; default: ``[]``).

        - an unnamed attribute should be an expression representing a
          condition.

        - a named parameter specifies the condition that the property specified
          by the name takes the specified by the value.
        """
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
        r"""
        Make a query for objects satisfying the conditions.

        INPUT:

        - ``db`` - the database being used (must be a named parameter;
          default: ``None``).

        - ``join`` - a join of tables needed to determine the object
          (must be a named parameter; default: ``None``).

        - ``by`` - the criterion to join by (must be a named parameter;
          default: ``None``). See ``db.query.Table.join`` for more information.

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``orderby`` - an expression or list of expressions to order by
          (must be a named parameter; default: ``[]``).

        - ``limit`` - the maximal number of returned results (must be a named
          parameter; default: ``None``).

        - ``offset`` - the number of results to skip (must be a named
          parameter; default: ``None``).

        - ``random`` - whether to randomly shuffle the results (must be a named
          parameter; default: ``False``).

        - an unnamed attribute should be an expression representing a
          condition.

        - a named parameter specifies the condition that the property specified
          by the name takes the specified by the value.
        """
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
        r"""
        Return a generator yielding properties of objects satisfying the
        conditions.

        All parameters are passed to ``ZooInfo.query``.
        """
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (todict(r, db) for r in cur)

    def all(self, *largs, **kargs):
        r"""
        Return a generator yielding objects satisfying the conditions.

        All parameters are passed to ``ZooInfo.query``.
        """
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (self.cl(todict(r, db), db = db) for r in cur)

    def one(self, *largs, **kargs):
        r"""
        Return an object satisfying the conditions.

        All parameters are passed to ``ZooInfo.query``.
        """
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
    r"""
    Initialize tables for all DiscreteZOO entities.

    INPUT:

    - ``db`` - the database being used (default: ``None``).

    - ``commit`` - whether to commit the changes to the database
      (default: ``True``).
    """
    if db is None:
        db = discretezoo.DEFAULT_DB
    for cl in zootypes.names.values():
        if type(cl) is type and issubclass(cl, ZooEntity):
            ZooInfo(cl).initdb(db = db, commit = False)
    if commit:
        db.commit()
