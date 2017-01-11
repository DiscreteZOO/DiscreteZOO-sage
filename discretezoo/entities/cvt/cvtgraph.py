# -*- coding: utf-8 -*-
r"""
A class representing cubic vertex-transitive graphs

This module contains a representing vertex-transitive graphs
and a function for importing such graphs.
"""
from sage.graphs.digraph import DiGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
import discretezoo
from ..spx import check_spx
from ..spx import SPXGraph
from ..vt import VTGraph
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import import_graphs
from ..zoograph import override
from ..zooobject import ZooObject
from ...db.query import Table
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import update

class CVTGraph(VTGraph):
    r"""
    A cubic vertex-transitive graph.

    A graph is cubic if each of its vertices lies on precisely three arcs.
    In the case of simple graphs, this is equivalent to each vertex
    having precisely three neighbours.
    """
    _cvtprops = None
    _parent = VTGraph
    _spec = None
    _dict = "_cvtprops"

    def __init__(self, data = None, cvt_index = None, symcubic_index = None,
                 **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the graph from (anything accepted
          by ``ZooObject`` or Sage's ``Graph``), or the order of the graph.

        - ``cvt_index`` - the index of the graph in the census by Potočnik,
          Spiga and Verret (default: ``None``).

        - ``symcubic_index`` - the index of the graph in the extended Foster
          census of cubic symmetric graphs by Conder (default: ``None``).

        - ``db`` - the database being used (must be a named parameter;
          default: ``None``).

        - ``store`` - whether to store the graph to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (must be a named parameter; default: ``None``).

        - named parameters accepted by or Sage's ``Graph`` class.
          Other named parameters are silently ignored.
        """
        ZooObject._init_(self, CVTGraph, kargs,
                         defNone = ["order", "vt_index"],
                         setVal = {"data": data, "cvt_index": cvt_index,
                                   "symcubic_index": symcubic_index},
                         setProp = {"cvt_index": "cvt_index",
                                    "symcubic_index": "symcubic_index"})

    def _parse_params(self, d):
        r"""
        Parse the ``data`` parameter of the constructor.

        First checks whether both ``data`` and either ``index`` or
        ``symcubic_index`` are given. Otherwise, tries the
        ``VTGraph._parse_params`` method.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if isinteger(d["data"]) and \
                not (d["cvt_index"] is None and d["symcubic_index"] is None):
            d["order"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return VTGraph._parse_params(self, d)

    def _construct_object(self, cl, d):
        if d["order"] is not None:
            cond = {"order": d["order"]}
            if d["cvt_index"] is not None:
                cond["cvt_index"] = d["cvt_index"]
            elif d["symcubic_index"] is not None:
                cond["symcubic_index"] = d["symcubic_index"]
            if len(cond) > 1:
                join = Table(cl._spec["name"]).join(
                                    Table(ZooGraph._spec["name"]),
                                    by = frozenset([cl._spec["primary_key"]]))
                try:
                    r = self._db_read(ZooGraph, join, cond, kargs = d)
                    d["zooid"] = r["zooid"]
                    d["graph"] = None
                except KeyError:
                    pass
        VTGraph.__init__(self, **d)

        if d["order"] is not None:
            assert(d["order"] == self._graphprops["order"])
        if len(self._cvtprops) == 0:
            try:
                self._db_read(cl, kargs = d)
            except KeyError as ex:
                if not d["store"]:
                    raise ex

    def _repr_generic(self):
        r"""
        Return an uncapitalized string representation.
        """
        if lookup(self._graphprops, "connected_components_number",
                  default = 1) > 1:
            out = "disconnected "
        else:
            out = ""
        index = self.cvt_index()
        tr = "vertex-transitive"
        if index is None:
            index = self.symcubic_index()
            if index is not None or \
                    lookup(self._graphprops, "is_arc_transitive",
                           default = False):
                tr = "symmetric"
        out += "cubic %s %s" % (tr, ZooGraph._repr_generic(self))
        if index is not None:
            out = "%s, number %s" % (out, index)
        return out

    def cvt_id(self):
        r"""
        Return the order and index of the graph in the census by P. Potočnik,
        P. Spiga and G. Verret.

        If the graph is not in the census, returns ``None``.
        """
        index = self.cvt_index()
        if index is None:
            return None
        return (self.order(), index)

    def cvt_index(self):
        r"""
        Return the index of the graph among the graphs of the same order
        in the census by P. Potočnik, P. Spiga and G. Verret.
        """
        return lookup(self._cvtprops, "cvt_index", default = None)

    @override.computed
    def is_moebius_ladder(self, **kargs):
        r"""
        Return whether the graph is a Möbius ladder.
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not self.is_connected(store = store, cur = cur):
            return False
        o = self.order(store = store, cur = cur)
        g = self.girth(store = store, cur = cur)
        if o <= 6:
            return o+2 == 2*g
        if g != 4:
            return False
        b = self.is_bipartite(store = store, cur = cur)
        d = self.diameter(store = store, cur = cur)
        og = self.odd_girth(store = store, cur = cur)
        return ((o % 4 == 0 and 4*d == o and og == 2*d+1) or
                    (o % 4 == 2 and 4*d == o+2 and b)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

    @override.computed
    def is_prism(self, **kargs):
        r"""
        Return whether the graph is a prism.
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not self.is_connected(store = store, cur = cur):
            return False
        o = self.order(store = store, cur = cur)
        g = self.girth(store = store, cur = cur)
        if o <= 6:
            return o == 2*g
        if g != 4:
            return False
        b = self.is_bipartite(store = store, cur = cur)
        if o == 8:
            return b
        d = self.diameter(store = store, cur = cur)
        og = self.odd_girth(store = store, cur = cur)
        return ((o % 4 == 0 and 4*d == o+4 and b) or
                    (o % 4 == 2 and 4*d == o+2 and og == 2*d-1)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

    @override.computed
    def is_spx(self, parameters = False, **kargs):
        r"""
        Return whether the graph is an SPX graph.

        INPUT:

        - ``parameters`` - if ``True``, return a tuple ``(r, s)``
          with the parameters ``r`` and ``s`` of the SPX graph
          (or ``None`` if the graph is not an SPX graph);
          otherwise (default), return a boolean.
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        try:
            if store:
                G = SPXGraph(self, store = store, cur = cur)
                if parameters:
                    return (G.spx_r(), G.spx_s())
            else:
                assert lookup(self._cvtprops, "is_spx", default = True)
                pars = check_spx(self)
                if parameters:
                    return pars
        except AssertionError:
            return None if parameters else False
        except KeyError as ex:
            if parameters:
                raise ex
        return True

    def symcubic_id(self):
        r"""
        Return the order and index of the graph in the extended Foster census
        of cubic symmetric graphs by M. Conder.

        If the graph is not in the census, returns ``None``.
        """
        index = self.symcubic_index()
        if index is None:
            return None
        return (self.order(), index)

    def symcubic_index(self):
        r"""
        Return the index of the graph among the graphs of the same order
        in the extended Foster census of cubic symmetric graphs by M. Conder.
        """
        return lookup(self._cvtprops, "symcubic_index", default = None)

    def truncation(self, name = None, **kargs):
        r"""
        Return a truncated graph.

        The truncated graph of a cubic graph ``G`` is a graph ``T`` whose
        vertices are arcs of ``G``, and two such arcs are adjacent if either
        they belong to the same edge, or they have a common source.

        INPUT:

        - ``name`` - the name to be given to the truncated graph. If ``None``,
          (default), the name will be derived from the name of the graph
          if it exists and the truncated graph does not yet have a name in the
          database.

        - ``store`` - whether to store the truncated graph
          and its name to the database (must be a named parameter;
          default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        commit = False
        if lookup(self._graphprops, "is_arc_transitive", default = False):
            cl = CVTGraph
        else:
            cl = ZooGraph
        try:
            t = lookup(self._cvtprops, "truncation")
            if isinteger(t):
                t = cl(zooid = t, db = self._db)
                update(self._cvtprops, "truncation", t)
        except KeyError:
            G = Graph([sum([[(n, e, 0), (n, e, 1)] for n, e
                            in enumerate(self.edge_iterator(labels = False))],
                           []),
                       lambda (n, e, i), (m, f, j): n == m or e[i] == f[j]],
                      loops = False)
            if not store:
                cur = None
            elif cur is None:
                cur = self._db.cursor()
                commit = True
            try:
                t = cl(G, store = store, db = self._db, cur = cur)
            except KeyError:
                t = G
            if store:
                self._update_rows(CVTGraph, {"truncation": t._zooid},
                                  {self._spec["primary_key"]: self._zooid},
                                  cur = cur)
        if name is None:
            nm = self.name()
            if nm and not t.name():
                name = "Truncated %s" % nm
        if name is not None:
            if isinstance(t, ZooGraph):
                t.name(new = name, store = store, cur = cur)
            else:
                t.name(new = name)
        if commit:
            self._db.commit()
        update(self._cvtprops, "truncation", t)
        return t

def import_cvt(file, db = None, format = "sparse6", index = "cvt_index",
               verbose = False):
    r"""
    Import cubic vertex-transitive graphs from ``file`` into the database.

    This function has been used to import the census of cubic vertex-transitive
    graphs by P. Potočnik, P. Spiga and G. Verret and the extended Foster
    census of cubic symmetric graphs by M. Conder and is not meant to be used
    by users of DiscreteZOO.

    To properly import the graphs, all graphs of the same order must be
    together in the file, and no graph of this order must be present in the
    database.

    INPUT:

    - ``file`` - the filename containing a graph in each line.

    - ``db`` - the database to import into. The default value of ``None`` means
      that the default database should be used.

    - ``format`` - the format the graphs are given in. If ``format`` is
      ``'graph6'`` or ``'sparse6'`` (default), then the graphs are read as
      strings, otherwised they are evaluated as Python expressions before
      being passed to Sage's ``Graph``.

    - ``index``: the name of the parameter of the constructor of ``cl``
      to which the serial number of the graph of a given order is passed.

    - ``verbose``: whether to print information about the progress of importing
      (default: ``False``).
    """
    import_graphs(file, cl = CVTGraph, db = db, format = format, index = index,
                  verbose = verbose)

info = ZooInfo(CVTGraph)
