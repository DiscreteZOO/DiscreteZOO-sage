from ..zooentity import ZooEntity
from ...db.query import Column
from ...db.query import Table
from ...db.query import Value

class Change(ZooEntity):
    _parent = None
    _objid = None
    _chgid = None

    def __init__(self, id, table = None, column = None, commit = None,
                 user = None, cur = None, db = None):
        self._zooid = False
        if table is None:
            self._chgid = id
        else:
            self._objid = id
            if issubclass(table, ZooEntity):
                table = table._spec["name"]
        ZooEntity.__init__(self, db = db)
        if cur is not None:
            if self._db.track:
                if self._objid is None:
                    raise KeyError("table not given")
                row = {"zooid": self._objid, "table": table,
                       "column": "" if column is None else column,
                       "commit": "" if commit is None else commit}
                self._db.query([Column(self._spec["primary_key"])],
                               Table(self._spec["name"]),
                               [Column(k) == Value(v) for k, v in row.items()],
                               cur = cur)
                r = cur.fetchone()
                if r is None:
                    row["user"] = user
                    self._db.insert_row(self._spec["name"], row, cur = cur,
                                        id = self._spec["primary_key"])
                    self._chgid = self._db.lastrowid(cur)
                else:
                    self._chgid = r[0]
            self.table =  table
            self.column = column
            self.commit = commit
            self.user = user
        else:
            if self._chgid is None:
                raise KeyError("change id not given")
            t = Table(self._spec["name"])
            cur = self._db.query([t], t,
                                 {self._spec["primary_key"]: self._chgid},
                                 cur = cur)
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

    def commit(commit, user, cur = None):
        if self._chgid is None:
            raise KeyError
        if self.commit is not None:
            raise KeyError("change is already included in a commit")
        self._db.update_rows(self._spec["name"],
                             {"commit" : commit, "user": user},
                             Column("change_id") == Value(self._chgid))
        self.commit = commit
        self.user = user
