r"""
A superclass for properties of DiscreteZOO objects

This module provides a superclass for multi-valued properties of DiscreteZOO
objects.
"""

import discretezoo
from .change import Change
from .zooentity import ZooEntity
from ..db.query import All
from ..db.query import Column
from ..db.query import Or
from ..db.query import Table
from ..db.query import Value
from ..util.utility import default

class ZooProperty(ZooEntity):
    r"""
    A superclass for properties of DiscreteZOO objects.
    """
    _deleted = None
    _foreign_key = None
    _foreign_obj = None
    _foreign_field = None

    def _init_(self, kargs):
        """
        Initialize the object.

        This method is called by the object constructor.

        INPUT:

        - ``kargs``: a dictionary of explicit and implicit parameters
          to the object constructor.
        """
        default(kargs, "store", discretezoo.WRITE_TO_DB)
        kargs["write"] = {}
        ZooEntity._init_(self, ZooEntity, kargs, defNone = ["data"])

    def _db_fetch(self, data, cur = None):
        """
        Fetch data from the database.

        INPUT:

        - ``data``: either the ID of an object whose properties will be
          fetched, or the ID of an entry of the property.
          In the latter case, the ``ZooProperty`` object will contain a single
          entry and will be read-only.

        - ``cur``: the cursor to use for database interaction
          (default: ``None``).
        """
        t = Table(self._spec["name"])
        c = self._db.query([t], t, {self._spec["primary_key"]: data},
                           cur = cur).fetchall()
        if len(c) == 0:
            self._objid = data
            c = self._db.query([t], t, {self._foreign_key: data,
                                        "deleted": False}, cur = cur)
        else:
            self._zooid = data
            self._objid = c[0][self._foreign_key]
            self._deleted = bool(c[0]["deleted"])
        return c

    def _insert_row(self, cl, row, cur = None, commit = None):
        r"""
        Insert a row into the database or replace an existing row.

        A matching row (possibly marked as deleted) is first queried for. If
        none exists, a new row is inserted. Otherwise, the old one is updated
        with the changed values. Returns the ID of the inserted or updated row.

        INPUT:

        - ``cl`` - the class determining the table to insert the row into.

        - ``row`` - a dictionary specifying the row to insert.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``commit`` - whether to commit after a new row is inserted. If
          ``None`` (default), commit only if ``cur`` is not specified.
        """
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        uidx = self._unique_index()
        row = dict(row)
        self._db.query([Column(k) for k in row if k not in uidx] +
                        [cl._spec["primary_key"], "deleted"], cl._spec["name"],
                       [Column(k) == Value(v) for k, v in row.items()
                        if k in uidx], cur = cur)
        r = cur.fetchone()
        if r is None:
            id = self._db_write(ZooEntity, cur)
            Change(id, cl, cur = cur, db = self._db)
            row[cl._spec["primary_key"]] = id
            row["deleted"] = False
            self._db.insert_row(cl._spec["name"], row, cur = cur,
                                commit = commit)
        else:
            id = r[cl._spec["primary_key"]]
            if r["deleted"]:
                Change(id, cl, column = "deleted", cur = cur, db = self._db)
            for k, v in row.items():
                if k not in uidx and v != r[k]:
                    Change(id, cl, column = k, cur = cur, db = self._db)
            row["deleted"] = False
            self._db.update_rows(cl._spec["name"], row,
                                 {cl._spec["primary_key"]: id}, cur = cur,
                                 commit = commit)
        return id

    def _delete_rows(self, cl, cond, cur = None, commit = None):
        r"""
        Delete rows from the database.

        Matching rows are first queried for. If found, they are marked as
        deleted.

        INPUT:

        - ``cl`` - the class determining the table to delete the rows from.

        - ``cond`` - an ``Expression`` specifying the condition necessary for
          the deletion to take place.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``commit`` - whether to commit after rows are deleted. If ``None``
          (default), commit only if ``cur`` is not specified.
        """
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        self._db.query([Column(cl._spec["primary_key"])], cl._spec["name"],
                       cond, distinct = True, cur = cur)
        for (id,) in cur.fetchall():
            Change(id, cl, column = "deleted", cur = cur, db = self._db)
        self._db.update_rows(cl._spec["name"], {"deleted": True}, cond,
                             cur = cur, commit = True)

    def _update_rows(self, cl, row, cond, cur = None, commit = None):
        r"""
        Update rows in the database.

        Matching rows are first queried for. If found, they are updated.

        INPUT:

        - ``cl`` - the class determining the table to delete the rows from.

        - ``row`` - a dictionary specifying the new values of the specified
          columns.

        - ``cond`` - an ``Expression`` specifying the condition necessary for
          the update to take place.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``commit`` - whether to commit after rows are deleted. If ``None``
          (default), commit only if ``cur`` is not specified.
        """
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        uidx = self._unique_index()
        cond = cond & (~Column("deleted"))
        self._db.query([Column(c) for c in {cl._spec["primary_key"]}
                                           .union(row.keys()).union(uidx)],
                       cl._spec["name"], cond, cur = cur)
        a = cur.fetchall()
        deleted = {}
        for r in a:
            self._db.query([Column(c) for c in
                            [cl._spec["primary_key"], "deleted"] + row.keys()],
                           cl._spec["name"],
                           {k: row[k] if k in row else r[k] for k in uidx},
                           cur = cur)
            s = cur.fetchone()
            if s is not None and s["deleted"]:
                id = s[cl._spec["primary_key"]]
                deleted[id] = r[cl._spec["primary_key"]]
                Change(id, cl, column = "deleted", cur = cur, db = self._db)
                Change(r[cl._spec["primary_key"]], cl, column = "deleted",
                       cur = cur, db = self._db)
            else:
                s = r
                id = r[cl._spec["primary_key"]]
            for k, v in row.items():
                if v != s[k]:
                    Change(id, cl, column = k, cur = cur, db = self._db)
        if len(deleted) > 0:
            col = Column(cl._spec["primary_key"])
            self._db.update_rows(cl._spec["name"], {"deleted": True},
                            Or([col == Value(id) for id in deleted.values()]),
                            cur = cur, commit = False)
            self._db.update_rows(cl._spec["name"],
                                 dict(row.items() + [("deleted", False)]),
                                 Or([col == Value(id) for id in deleted]),
                                 cur = cur, commit = False)
        if len(deleted) < len(a):
            self._db.update_rows(cl._spec["name"], row, cond, cur = cur,
                                 commit = commit)
        elif commit:
            self._db.commit()

    def _unique_index(self):
        r"""
        Return a list of columns uniquely determining a row in the database.

        Not implemented, to be overridden.
        """
        return NotImplementedError

    @staticmethod
    def _init_spec(cl, spec):
        r"""
        Initialize the class specification.

        INPUT:

        - ``cl`` - the class to initialize the specification for.

        - ``spec`` - a partial specification to be added to the generic
          specification.
        """
        if "indices" in spec:
            cl._spec["indices"] += spec["indices"]
        if "skip" in spec:
            cl._spec["skip"].update(spec["skip"])
        if "fieldparams" in spec:
            cl._spec["fieldparams"].update(spec["fieldparams"])
        if "compute" in spec:
            cl._spec["compute"].update(spec["compute"])
        if "condition" in spec:
            cl._spec["condition"].update(spec["condition"])
        if "default" in spec:
            cl._spec["default"].update(spec["default"])

    @staticmethod
    def _init_json_field():
        """
        Return an empty object suitable for conversion to JSON.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    @staticmethod
    def _update_json_field(field, data):
        """
        Update ``field`` with ``data``.

        Not implemented, to be overridden.

        INPUT:

        - ``field`` - field object (``list`` or ``dict``) to be updated.

        - ``data`` - data object to update with.
        """
        raise NotImplementedError

    def _update_json(self, obj):
        """
        Update the appropriate field of the object ``obj``
        to be converted to JSON.

        INPUT:

        - ``obj`` - object to be converted.
        """
        if self._foreign_field not in obj:
            obj[self._foreign_field] = self._init_json_field()
        self._update_json_field(obj[self._foreign_field], self._to_json())

    def write_json(self, location, folder = "objects", field = None,
                   link = True):
        r"""
        Write a JSON representation of the object the property belongs to
        to the appropriate file in the repository at ``location``.

        INPUT:

        - ``location`` - the location of the repository containing the
          objects.

        - ``folder`` - the path to the folder within the repository where
          the objects will be written (default: ``"objects"``).

        - ``field`` - which fields to export (ignored for ``ZooProperty``).
        """
        self._foreign_obj(self._objid).write_json(location, folder = folder,
                                                  field = self, link = link)
