from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from query import Table
from utility import isinteger
from utility import lookup
from zoograph import ZooGraph
from zooobject import ZooInfo
from zooobject import ZooObject

_objspec = {
    "name": "graph_cvt",
    "primary_key": "id",
    "indices": {"cvt_index"},
    "skip": {"id"},
    "fields" : {
        "cvt_index": Integer,
        "id": (ZooGraph, {"primary_key"})
    }
}

class CVTGraph(ZooGraph):
    _cvtprops = None
    _spec = _objspec
    _parent = ZooGraph

    def __init__(self, data = None, index = None, vertices = None,
                 zooid = None, props = None, graph = None, name = None,
                 cur = None, db = None, **kargs):
        kargs["loops"] = False
        kargs["multiedges"] = False
        if isinteger(data):
            if index is None:
                zooid = Integer(data)
            else:
                vertices = Integer(data)
            data = None
        elif isinstance(data, GenericGraph):
            graph = data
            data = None
        elif isinstance(data, dict):
            props = data
            data = None
        if props is not None:
            if "id" in props:
                zooid = props["id"]
            if "data" in props:
                data = props["data"]
            props = {k: v for k, v in props.items() if k not in ["id", "data"]}

        if graph is not None:
            if not isinstance(graph, GenericGraph):
                raise TypeError("not a graph")
            if name is None:
                name = graph.name()
            if isinstance(graph, ZooGraph):
                zooid = graph._zooid
                self._props = graph._props
            if isinstance(graph, CVTGraph):
                self._cvtprops = graph._cvtprops
            if cur is not None:
                if self._props is None:
                    self._props = {}
                if self._cvtprops is None:
                    self._cvtprops = {}
                self._cvtprops["cvt_index"] = index
                self._props["average_degree"] = 3
                self._props["is_regular"] = True
                self._props["is_tree"] = False
                self._props["is_vertex_transitive"] = True
                if vertices is not None:
                    self._props["num_edges"] = 3*vertices/2
                self._props["number_of_loops"] = 0
            elif zooid is None:
                raise IndexError("graph id not given")
            data = None
            vertices = None
            index = None
        else:
            cur = None
            if props is not None:
                self._cvtprops = self._todict(props,
                                            skip = CVTGraph._spec["skip"],
                                            fields = CVTGraph._spec["fields"])
                props = {k: v for k, v in props.items()
                         if k not in self._spec["fields"]}

        if vertices is not None and index is not None:
            join = Table(self._spec["name"]).join(Table(self._parent._spec["name"]),
                         by = {self._spec["primary_key"]})
            ZooObject.__init__(self, db);
            r = self._db_read(join, {"order": vertices, "cvt_index": index})
            ZooGraph.__init__(self, zooid = r["id"], data = r["data"],
                              props = props, name = name, db = db, **kargs)
        else:
            ZooGraph.__init__(self, zooid = zooid, data = data, graph = graph,
                              props = props, name = name, cur = cur, db = db,
                              **kargs)
        if vertices is not None:
            assert(vertices == self._props["order"])
        if self._cvtprops is None:
            self._db_read_cvt()
        if cur is not None:
            self._db_write_cvt(cur)

    def _repr_(self):
        name = "Cubic vertex-transitive graph on %d vertices, number %d" \
                                            % (self.order(), self.cvt_index())
        if self.name() != '':
            name = self.name() + ": " + name
        return name

    def _db_read_cvt(self, join = None, query = None):
        if query is None:
            if self._zooid is None:
                raise IndexError("graph id not given")
            query = {"id": self._zooid}
        t = Table(CVTGraph._spec["name"])
        if join is None:
            join = t
        cur = self._db.query([t], join, query)
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(query)
        self._cvtprops = self._todict(r, skip = CVTGraph._spec["skip"],
                                      fields = CVTGraph._spec["fields"])

    def _db_write_cvt(self, cur):
        self._db.insert_row(CVTGraph._spec["name"],
                            dict(self._cvtprops.items() + \
                                 [("id", self._zooid)]), cur = cur)

    def load_db_data(self):
        ZooGraph.load_db_data(self)
        self._db_read_cvt()

    def cvt_index(self):
        return lookup(self._cvtprops, "cvt_index")

info = ZooInfo(CVTGraph)

def import_cvt(file, db = None, format = "sparse6", canonical = False,
               verbose = False):
    if db is None:
        db = info.updatedb()
    info.initdb(db = db, commit = False)
    previous = 0
    i = 0
    cur = db.cursor()
    with open(file) as f:
        for line in f:
            data = line.strip()
            if format not in ["graph6", "sparse6"]:
                data = eval(data)
            g = Graph(data)
            if canonical:
                g = g.canonical_label()
            n = g.order()
            if n > previous:
                if verbose:
                    print "Imported %d graphs of order %d" % (i, previous)
                previous = n
                i = 0
            i += 1
            CVTGraph(graph = g, vertices = n, index = i, cur = cur)
        if verbose:
            print "Imported %d graphs of order %d" % (i, n)
        f.close()
    cur.close()
    db.commit()