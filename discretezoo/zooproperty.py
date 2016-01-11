from change import Change
from query import Column
from query import Or
from query import Value
from zooentity import ZooEntity

class ZooProperty(ZooEntity):
    def _insert_row(self, cl, row, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        uidx = self._unique_index()
        row = dict(row)
        self._db.query([k for k in row.keys() if k not in uidx] +
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
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        self._db.query([cl._spec["primary_key"]], cl._spec["name"], cond,
                       distinct = True, cur = cur)
        for (id,) in cur.fetchall():
            Change(id, cl, column = "deleted", cur = cur, db = self._db)
        self._db.update_rows(cl._spec["name"], {"deleted": True}, cond,
                             cur = cur, commit = True)

    def _update_rows(self, cl, row, cond, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        uidx = self._unique_index()
        cond = cond & (~Column("deleted"))
        self._db.query([cl._spec["primary_key"]] +
                        list(set(row.keys()).union(uidx)), cl._spec["name"],
                        cond, cur = cur)
        a = cur.fetchall()
        deleted = {}
        for r in a:
            self._db.query([cl._spec["primary_key"], "deleted"] + row.keys(),
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
        return NotImplementedError
