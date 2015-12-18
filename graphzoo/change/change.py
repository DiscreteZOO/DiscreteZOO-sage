from ..query import Column
from ..query import Table
from ..query import Value
from ..zooentity import ZooEntity

class Change(ZooEntity):
    _parent = None
    _objid = None
    _chgid = None

    def __init__(self, id, table = None, column = None, commit = None,
                 cur = None, db = None):
        self._zooid = False
        if table is None:
            self._chgid = id
        else:
            self._objid = id
        ZooEntity.__init__(self, db = db)
        if cur is not None:
            if self._objid is None:
                raise KeyError("table not given")
            cond = {"zooid": self._objid, "table": table,
                    "column": "" if column is None else column,
                    "commit": "" if commit is None else commit}
            try:
                self._db.insert_row(self._spec["name"], cond, cur = cur,
                                    id = self._spec["primary_key"],
                                    canfail = True)
                self._chgid = self._db.lastrowid(cur)
            except ValueError:
                self._db.query([self._spec["primary_key"]],
                                Table(self._spec["name"]), cond, cur = cur)
                self._chgid = cur.fetchone()[0]
            self.table =  table
            self.column = column
            self.commit = commit
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

    def __repr__(self):
        out = "table %s" % self.table
        if self.column is not None:
            out = "column %s of %s" % (self.column, out)
        if self.commit is not None:
            out = "%s at commit %s" % (out, self.commit)
        return "Change to object with ID %d in %s" % (self._objid, out)

    def commit(commit, cur = None):
        if self.commit is not None:
            raise KeyError("change is already included in a commit")
        self._db.update_rows(self._spec["name"], {"commit" : commit},
                             Column("commit") == Value(commit))
        self.commit = commit
