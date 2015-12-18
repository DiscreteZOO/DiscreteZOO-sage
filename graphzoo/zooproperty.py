from change import Change
from zooentity import ZooEntity

class ZooProperty(ZooEntity):
    def _insert_row(self, cl, row, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        id = self._db_write(ZooEntity, cur)
        row = dict(row.items() + [(cl._spec["primary_key"], id)])
        self._db.insert_row(cl._spec["name"], row, cur = cur, commit = False)
        Change(id, cl, cur = cur, db = self._db)
        if commit:
            self._db.commit()
