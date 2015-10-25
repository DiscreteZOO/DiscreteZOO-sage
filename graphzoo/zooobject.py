import graphzoo
from utility import lookup

def _initdb(cl, db = None, commit = True):
    if db is None:
        db = graphzoo.DEFAULT_DB
    for base in cl.__bases__:
        if issubclass(base, ZooObject):
            _initdb(base, db, commit = False)
    if cl._spec is not None:
        db.init_table(cl._spec, commit = commit)

class ZooObject:
    _spec = None
    _db = None
    _zooid = None

    def __init__(self, db = None):
        if db == None:
            self._db = graphzoo.DEFAULT_DB
        else:
            self._db = db

    def setdb(self, db):
        self._db = db

    def initdb(self, db = None):
        _initdb(self.__class__, self._db)

    def _todict(self, r, skip = []):
        return {k: self._db.from_db_type(r[k],
                                lookup(self._spec["fields"], k, type(r[k])))
                for k in r.keys() if k not in skip}

