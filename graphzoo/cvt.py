from sage.rings.integer import Integer
from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from zoograph import ZooGraph
from utility import lookup
from utility import todict
from utility import isinteger
import sqlite

class CVTGraph(ZooGraph):
    _cvtprops = {}

    def __init__(self, data = None, index = None, vertices = None,
                 zooid = None, graph = None, name = None, **kargs):
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
        cvtprops = None
        if graph is not None:
            if not isinstance(graph, GenericGraph):
                raise TypeError("not a graph")
            if name is None:
                name = graph.name()
            if isinstance(graph, ZooGraph):
                zooid = graph._zooid
            elif zooid is None:
                raise IndexError("graph id not given")
            if isinstance(graph, CVTGraph):
                cvtprops = graph._cvtprops
            vertices = None
            index = None

        if vertices is not None and index is not None:
            cur = sqlite.db.cursor()
            cur.execute("""
                SELECT graph.* FROM graph NATURAL JOIN graph_cvt
                WHERE vertices = ? AND cvtid = ?
            """, [int(vertices), int(index)])
            r = cur.fetchone()
            cur.close()
            if r is None:
                raise KeyError((vertices, index))
            props = todict(r, skip = ["id", "data"])
            zooid = r["id"]
            ZooGraph.__init__(self, zooid = zooid, data = r["data"],
                              props = props, name = name, **kargs)
        else:
            ZooGraph.__init__(self, zooid = zooid, graph = graph, name = name,
                              **kargs)
        if cvtprops is None:
            self._db_read_cvt()
        else:
            self._cvtprops = cvtprops
        if not name:
            self.name("Cubic vertex-transitive graph on %d vertices, number %d"
                      % (self.order(), self.cvt_index()))

    def _db_read_cvt(self):
        cur = sqlite.db.cursor()
        cur.execute("SELECT * FROM graph_cvt WHERE id = ?", [int(self._zooid)])
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(self._zooid)
        self._cvtprops = todict(r, skip = ["id"])

    def load_db_data(self):
        ZooGraph.load_db_data(self)
        self._db_read_cvt()

    def cvt_index(self):
        return lookup(self._cvtprops, "cvtid")

def initdb():
    sqlite.initdb()
    sqlite.db.execute("""
        CREATE TABLE IF NOT EXISTS graph_cvt (
            id          INTEGER PRIMARY KEY REFERENCES graph(id),
            cvtid       INTEGER
        )
    """)
    sqlite.db.commit()

def import_cvt(file):
    initdb()
    previous = 0
    i = 0
    cur = sqlite.db.cursor()
    with open(file) as f:
        for line in f:
            g = Graph(line.strip())
            n = g.order()
            if n > previous:
                previous = n
                i = 0
            i += 1
            cur.execute("""
                INSERT INTO graph(data, average_degree, diameter, girth,
                                  is_regular, vertices)
                VALUES (?, 3, ?, ?, 1, ?)
            """, (g.sparse6_string(), int(g.diameter()), int(g.girth()),
                  int(n)))
            id = cur.lastrowid
            cur.execute("""
                INSERT INTO graph_cvt(id, cvtid) VALUES (?, ?)
            """, (id, i))
        f.close()
    cur.close()
    sqlite.db.commit()
