r"""
A class representing vertex-transitive graphs

This module contains a representing vertex-transitive graphs
and a function for importing such graphs.
"""
from sage.rings.integer import Integer
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import import_graphs
from ..zooobject import ZooObject
from ...db.query import Table
from ...util.utility import isinteger
from ...util.utility import lookup

class VTGraph(ZooGraph):
    r"""
    A vertex-transitive graph.

    A graph is vertex-transitive if for any pair of its vertices ``(u, v)``,
    there exists an automorphism of the graph mapping ``u`` to ``v``.
    """
    _vtprops = None
    _parent = ZooGraph
    _spec = None
    _dict = "_vtprops"

    def __init__(self, data = None, vt_index = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the graph from (anything accepted
          by ``ZooObject`` or Sage's ``Graph``), or the order of the graph.

        - ``vt_index`` - the index of the graph in the census by Royle
          (default: ``None``).

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
        ZooObject._init_(self, VTGraph, kargs,
                         defNone = ["order"],
                         setVal = {"data": data, "vt_index": vt_index},
                         setProp = {"vt_index": "vt_index"})

    def _parse_params(self, d):
        r"""
        Parse the ``data`` parameter of the constructor.

        First checks whether both ``data`` and ``vt_index`` are given, and
        interprets ``data`` as the ID otherwise. If ``data`` is not an
        integer, tries the ``ZooGraph._parse_params`` method.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if isinteger(d["data"]):
            if d["vt_index"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["order"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return ZooGraph._parse_params(self, d)

    def _construct_object(self, cl, d):
        r"""
        Prepare all necessary data and construct the graph.

        INPUT:

        - ``cl`` - the class to construct the graph for.

        - ``d`` - the dictionary of parameters.
        """
        if d["order"] is not None:
            if d["vt_index"] is not None:
                join = Table(cl._spec["name"]).join(
                                    Table(ZooGraph._spec["name"]),
                                    by = frozenset([cl._spec["primary_key"]]))
                try:
                    r = self._db_read(ZooGraph, join,
                                      {"order": d["order"],
                                       "vt_index": d["vt_index"]}, kargs = d)
                    d["zooid"] = r["zooid"]
                    d["graph"] = None
                except KeyError:
                    pass
        ZooGraph.__init__(self, **d)

        if d["order"] is not None:
            assert(d["order"] == self._graphprops["order"])
        if len(self._vtprops) == 0:
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
        out += "vertex-transitive %s" % ZooGraph._repr_generic(self)
        index = self.vt_index()
        if index is not None:
            out = "%s, number %s" % (out, index)
        return out

    def vt_id(self):
        r"""
        Return the order and index of the graph in the census by G. Royle.

        If the graph is not in the census, returns ``None``.
        """
        index = self.vt_index()
        if index is None:
            return None
        return (self.order(), index)

    def vt_index(self):
        r"""
        Return the index of the graph among the graphs of the same order
        in the census by G. Royle.
        """
        return lookup(self._vtprops, "vt_index", default = None)

def import_vt(file, db = None, format = "sparse6", index = "vt_index",
              verbose = False):
    r"""
    Import vertex-transitive graphs from ``file`` into the database.

    This function has been used to import the census of vertex-transitive
    graphs by G. Royle and is not meant to be used by users of DiscreteZOO.

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
    import_graphs(file, cl = VTGraph, db = db, format = format, index = index,
                  verbose = verbose)

info = ZooInfo(VTGraph)
