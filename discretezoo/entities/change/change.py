r"""
A class representing changes to the database

This module contains a class for representing and logging
changes to the database.
"""

import discretezoo
from ..zooentity import ZooEntity
from ...db.query import Column
from ...db.query import Table
from ...db.query import Value
from ...util.utility import default


class Change(ZooEntity):
    r"""
    A change to the database

    Each time a row in the database is changed,
    this change is logged to a special table.
    These changes can then be gathered in a commit
    and submitted to the master database.
    """
    _parent = None
    _objid = None
    _chgid = None

    def __init__(self, id, table=None, column=None, commithash=None,
                 user=None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``id`` - the ID of the changed row. Alternatively, if ``table``
          is not given, the ID of an existing change.

        - ``table`` - the table to which the change was made
          (default: ``None``).

        - ``column`` - the column to which the change was made.
          The default value of ``None`` is used for a new row.

        - ``commithash`` - the hash of the commit containing the change.

        - ``user`` - the user who commited the change.

        - ``store`` - whether to store the change to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (must be a named parameter; default: ``None``).
        """
        self._zooid = False
        if table is None:
            self._chgid = id
        else:
            self._objid = id
            if issubclass(table, ZooEntity):
                table = table._spec["name"]
        default(kargs, "store", discretezoo.WRITE_TO_DB)
        kargs["write"] = {}
        ZooEntity._init_(self, ZooEntity, kargs, defNone=["data"])
        if kargs["store"]:
            cur = kargs["cur"]
            if self._db.track:
                if self._objid is None:
                    raise KeyError("table not given")
                row = {"zooid": self._objid, "table": table,
                       "column": "" if column is None else column,
                       "commit": "" if commithash is None else commithash}
                self._db.query([Column(self._spec["primary_key"])],
                               Table(self._spec["name"]),
                               [Column(k) == Value(v) for k, v in row.items()],
                               cur=cur)
                r = cur.fetchone()
                if r is None:
                    row["user"] = user
                    self._db.insert_row(self._spec["name"], row, cur=cur,
                                        id=self._spec["primary_key"])
                    self._chgid = self._db.lastrowid(cur)
                else:
                    self._chgid = r[0]
            self.table = table
            self.column = column
            self.commit = commithash
            self.user = user
            if kargs["commit"]:
                self._db.commit()
        else:
            if self._chgid is None:
                raise KeyError("change id not given")
            t = Table(self._spec["name"])
            cur = self._db.query([t], t,
                                 {self._spec["primary_key"]: self._chgid},
                                 cur=cur)
            r = cur.fetchone()
            if r is None:
                raise KeyError(self._chgid)
            self._objid = r["zooid"]
            self.table = r["table"]
            self.column = None if r["column"] == "" else r["column"]
            self.commit = None if r["commit"] == "" else r["commit"]
            self.user = r["user"]

    def __repr__(self):
        out = "table %s" % self.table
        if self.column is not None:
            out = "column %s of %s" % (self.column, out)
        if self.commit is not None:
            out = "%s at commit %s" % (out, self.commit)
        return "Change to object with ID %d in %s" % (self._objid, out)

    def commit(commithash, user, cur=None, commit=None):
        r"""
        Include the change in a commit.

        - ``commit`` - the hash of the commit containing the change.

        - ``user`` - the user who commited the change.

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (default: ``None``).
        """
        if self._chgid is None:
            raise KeyError
        if self.commit is not None:
            raise KeyError("change is already included in a commit")
        self._db.update_rows(self._spec["name"],
                             {"commit": commithash, "user": user},
                             Column("change_id") == Value(self._chgid),
                             cur=cur, commit=commit)
        self.commit = commithash
        self.user = user
