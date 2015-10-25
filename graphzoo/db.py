from zooobject import ZooObject

class DB:
    convert_to = None
    convert_from = None

    def __init__(self, **kargs):
        self.connect(**kargs)

    def connect(self, **kargs):
        raise NotImplementedError

    def cursor(self, **kargs):
        raise NotImplementedError

    def commit(self, **kargs):
        raise NotImplementedError

    def rollback(self, **kargs):
        raise NotImplementedError

    def init_table(self, **kargs):
        raise NotImplementedError

    def insert_row(self, **kargs):
        raise NotImplementedError

    def query(self, **kargs):
        raise NotImplementedError

    def to_db_type(self, x):
        if isinstance(x, ZooObject):
            return self.convert_to[ZooObject](x._zooid)
        elif type(x) in self.convert_to:
            return self.convert_to[type(x)](x)
        else:
            return x

    def from_db_type(self, x, t):
        if isinstance(t, tuple):
            t = t[0]
        if issubclass(t, ZooObject):
            return self.convert_from[ZooObject](x)
        elif t in self.convert_from:
            return self.convert_from[t](x)
        else:
            return x

class Table:
    tables = []

    def __init__(self, *args, **kargs):
        self.tables = [{"table": t,
                        "alias": t,
                        "left": False,
                        "by": set()} for t in args] \
                    + [{"table": t,
                        "alias": a,
                        "left": False,
                        "by": set()} for a, t in kargs]

    def join(self, by = {}, left = False, alias = None, *args, **kargs):
        if len(kargs) == 0:
            table = args[0]
        elif len(kargs) == 1:
            alias, table = kargs.items()[0]
        else:
            raise NotImplementedError
        self.tables.append({"table": table,
                            "alias": alias,
                            "left": left,
                            "by": by})
        return self
