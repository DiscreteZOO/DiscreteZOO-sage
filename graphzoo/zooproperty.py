from change import Change
from query import Column
from query import Value
from zooentity import ZooEntity

class ZooProperty(ZooEntity):
    def _insert_row(self, cl, row, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        id = self._db_write(ZooEntity, cur)
        row = dict(row.items() +
                    [(cl._spec["primary_key"], id), ("deleted", False)])
        try:
            Change(id, cl, cur = cur, db = self._db)
            self._db.insert_row(cl._spec["name"], row, cur = cur,
                                commit = False, canfail = True)
        except ValueError:
            uidx = self._unique_index()
            cond = [Column(k) == Value(v) for k, v in row.items() if k in uidx]
            self._db.query([cl._spec["primary_key"]], cl._spec["name"], cond,
                            cur = cur)
            id = cur.fetchone()[0]
            Change(id, cl, column = "deleted", cur = cur, db = self._db)
            del row[cl._spec["primary_key"]]
            self._db.update_rows(cl._spec["name"], row,
                                 {cl._spec["primary_key"]: id}, cur = cur,
                                 commit = False)
        if commit:
            self._db.commit()

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
                             cur = cur, commit = False)
        if commit:
            self._db.commit()

    def _update_rows(self, cl, row, cond, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        self._db.query([cl._spec["primary_key"]] + row.keys(),
                       cl._spec["name"], cond, distinct = True, cur = cur)
        for r in cur.fetchall():
            for c in row:
                if r[c] != row[c]:
                    Change(r[cl._spec["primary_key"]], cl, column = c,
                           cur = cur, db = self._db)
        self._db.update_rows(cl._spec["name"], row, cond, cur = cur,
                             commit = False)
        if commit:
            self._db.commit()

    def _unique_index(self):
        return NotImplementedError
