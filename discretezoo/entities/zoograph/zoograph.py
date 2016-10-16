r"""
A class representing DiscreteZOO graphs

This module contains a class extending Sage's ``Graph`` class
and some helper functions.
"""

from sage.categories.sets_cat import EmptySetError
from sage.graphs.graph import DiGraph
from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.graphs.graph_coloring import edge_coloring
from sage.misc.package import is_package_installed
from sage.rings.infinity import PlusInfinity
from sage.rings.integer import Integer
from hashlib import sha256
from inspect import getargspec
import discretezoo
from ..zooentity import ZooInfo
from ..zooobject import ZooObject
from ...db.query import Column
from ...db.query import Value
from ...util.decorators import ZooDecorator
from ...util.utility import construct
from ...util.utility import default
from ...util.utility import lookup
from ...util.utility import update

override = ZooDecorator(Graph)

class ZooGraph(Graph, ZooObject):
    r"""
    A graph in DiscreteZOO.

    Extends Sage's ``Graph`` class.
    """
    _graphprops = None
    _spec = None
    _parent = ZooObject
    _dict = "_graphprops"
    _override = override
    _initialized = False

    def __init__(self, data = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the graph from (anything accepted
          by ``ZooObject`` or Sage's ``Graph``).

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
        ZooObject._init_(self, ZooGraph, kargs, defNone = ["vertex_labels"],
                         setVal = {"data": data,
                                   "immutable": True,
                                   "data_structure": "static_sparse"})

    def _init_defaults(self, d):
        r"""
        Initialize the default parameters.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        default(d, "zooid")
        default(d, "unique_id")
        default(d, "props")
        default(d, "graph")
        default(d, "name")
        default(d, "cur")
        default(d, "loops")
        default(d, "multiedges")

    def _init_params(self, d):
        r"""
        Class-specific parsing of the ``data`` parameter of the constructor
        after a generic parsing fails.

        If ``data`` is a graph, then its copy will be constructed. If ``data``
        is a dictionary, then it is taken as the dictionary of properties.
        Otherwise, ``data`` is passed to Sage's ``Graph``.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if isinstance(d["data"], GenericGraph):
            d["graph"] = d["data"]
        elif isinstance(d["data"], dict):
            d["props"] = d["data"]
        elif d["data"] is not None:
            self._construct_graph(d)
        d["data"] = None

    def _construct_graph(self, d):
        r"""
        Construct a graph from the given data.
        """
        args = getargspec(Graph.__init__)[0][1:]
        d["graph"] = Graph(**{k: v for k, v in d.items() if k in args})
        d["vertex_labels"] = None

    def _init_skip(self, d):
        r"""
        Initialize the properties to be stored separately.

        The ``zooid`` and ``data`` entries are not considered properties
        and are not stored as such.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if d["props"] is not None:
            if "zooid" in d["props"]:
                d["zooid"] = d["props"]["zooid"]
                del d["props"]["zooid"]
            if "data" in d["props"]:
                d["data"] = d["props"]["data"]
                del d["props"]["data"]

    def _init_object(self, cl, d, setProp = {}):
        r"""
        Initialize the object being represented.

        If a graph has been given, the object is initialized to its copy.
        Otherwise, properties are stored in the appropriate dictionary.

        INPUT:

        - ``cl`` - the class to initialize the object for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        if d["graph"] is not None:
            self._init_graph(cl, d, setProp)
        else:
            self._apply_props(cl, d)
        cl._construct_object(self, cl, d)

    def _init_graph(self, cl, d, setProp = {}):
        r"""
        Initialize the propreties from the given graph.

        INPUT:

        - ``cl`` - the class to initialize the properties for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        if not isinstance(d["graph"], GenericGraph):
            raise TypeError("not a graph")
        if d["name"] is None:
            d["name"] = d["graph"].name()
        if isinstance(d["graph"], ZooGraph):
            d["zooid"] = d["graph"]._zooid
            d["unique_id"] = d["graph"]._unique_id
            d["unique_id_algorithm"] = d["graph"]._unique_id_algorithm
            self._copy_props(cl, d["graph"])
        else:
            uid_done = False
            for algo in AVAILABLE_ALGORITHMS:
                try:
                    d["unique_id"] = unique_id(d["graph"], algorithm = algo,
                                               store = False)
                    d["unique_id_algorithm"] = algo
                    uid_done = True
                    break
                except NotImplementedError:
                    pass
            if not uid_done:
                raise NotImplementedError("no suitable unique ID algorithm found")
        try:
            if d["zooid"] is None:
                d["props"] = next(ZooInfo(cl).props(cl._fields.unique_id == \
                                                        Value(d["unique_id"]),
                                                    cur = d["cur"]))
                for k, v in setProp.items():
                    if k not in d["props"]:
                        d["props"][k] = d[v]
        except StopIteration:
            if not d["write"][cl]:
                raise KeyError("graph not found in database")

        self._apply_props(cl, d)
        d["data"] = d["graph"]
        d["graph"] = None
        self._graphprops["number_of_loops"] = d["data"].number_of_loops()
        self._graphprops["has_multiple_edges"] = d["data"].has_multiple_edges()

    def _compute_props(self, cl, d, setProp = {}):
        r"""
        Compute the properties required by the class specification.

        INPUT:

        - ``cl`` - the class to compute the properties for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        ZooObject._compute_props(self, cl, d, setProp)
        if cl is ZooGraph:
            for k in ["diameter", "girth"]:
                if k in self._graphprops and \
                        self._graphprops[k] == PlusInfinity():
                    del self._graphprops[k]

    def _construct_object(self, cl, d):
        r"""
        Prepare all necessary data and construct the graph.

        INPUT:

        - ``cl`` - the class to construct the graph for.

        - ``d`` - the dictionary of parameters.
        """
        ZooObject.__init__(self, **d)
        if d["data"] is None:
            try:
                d["data"] = self._db_read(cl, kargs = d)["data"]
            except KeyError as ex:
                if not d["store"]:
                    raise ex
        propname = lookup(self._graphprops, "name", default = None)
        if d["name"]:
            self._graphprops["name"] = d["name"]
        elif propname:
            d["name"] = propname
        if propname == '':
            del self._graphprops["name"]
        if d["vertex_labels"] is not None:
            d["data"] = Graph(d["data"]).relabel(d["vertex_labels"],
                                                 inplace = False)
        if d["loops"] is None:
            d["loops"] = self._graphprops["number_of_loops"] > 0
        elif not d["loops"] and self._graphprops["number_of_loops"] > 0:
            raise ValueError("the requested graph has loops")
        if d["multiedges"] is None:
            d["multiedges"] = self._graphprops["has_multiple_edges"]
        elif not d["multiedges"] and self._graphprops["has_multiple_edges"]:
            raise ValueError("the requested graph has multiple edges")
        construct(Graph, self, d)
        self._initialized = True

    def _db_write_nonprimary(self, cur):
        r"""
        Write the unique IDs for all available algorithms to the database.

        INPUT:

        - ``cur`` - the cursor to use for database interaction.
        """
        uid = self.unique_id()
        for algo in AVAILABLE_ALGORITHMS:
            if algo not in uid:
                try:
                    uid.__setitem__(algo, unique_id(self, algorithm = algo),
                                    store = True, cur = cur)
                except NotImplementedError:
                    pass

    def _repr_generic(self):
        r"""
        Return an uncapitalized string representation.
        """
        name = ""
        if self.allows_loops():
            name += "looped "
        if self.allows_multiple_edges():
            name += "multi-"
        if self._directed:
            name += "di"
        name += "graph on %d vert"%self.order()
        if self.order() == 1:
            name += "ex"
        else:
            name += "ices"
        return name

    @override.documented
    def _repr_(self):
        r""
        name = self._repr_generic()
        if self.name() != '':
            name = self.name() + ": " + name
        else:
            name = name[0].capitalize() + name[1:]
        return name

    def __getattribute__(self, name):
        return ZooObject.__getattribute__(self, "_getattr")(name, Graph)

    @override.documented
    def copy(self, weighted = None, implementation = 'c_graph',
             data_structure = None, sparse = None, immutable = None):
        r"""
        This method has been overridden by DiscreteZOO to ensure that a mutable
        copy will have type ``Graph``.
        """
        if immutable is False or (data_structure is not None
                                  and data_structure is not 'static_sparse'):
            return Graph(self).copy(weighted = weighted,
                                    implementation = implementation,
                                    data_structure = data_structure,
                                    sparse = sparse,
                                    immutable = immutable)
        else:
            return Graph.copy(self, weighted = weighted,
                                    implementation = implementation,
                                    data_structure = data_structure,
                                    sparse = sparse,
                                    immutable = immutable)

    @override.documented
    def relabel(self, perm = None, inplace = True, return_map = False,
                check_input = True, complete_partial_function = True,
                immutable = True):
        r"""
        This method has been overridden by DiscreteZOO to ensure that a mutable
        copy will have type ``Graph``.
        """
        if inplace:
            raise ValueError("To relabel an immutable graph use inplace=False")
        G = Graph(self, immutable = False)
        perm = G.relabel(perm, return_map = True, check_input = check_input,
                         complete_partial_function = complete_partial_function)
        if immutable is not False:
            G = self.__class__(self, vertex_labels = perm)
        if return_map:
            return G, perm
        else:
            return G

    @override.documented
    def _subgraph_by_adding(self, vertices = None, edges = None,
                            edge_property = None, immutable = None, *largs,
                            **kargs):
        r"""
        This method has been overridden by DiscreteZOO to ensure that the
        subgraph will have type ``Graph``.
        """
        if immutable is None:
            immutable = True
        return Graph(self)._subgraph_by_adding(vertices = vertices,
                                               edges = edges,
                                               edge_property = edge_property,
                                               immutable = immutable,
                                               *largs, **kargs)

    def data(self, **kargs):
        r"""
        Return graph data.

        INPUT:

        - ``store`` - whether to store the graph to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - other named parameters accepted by the ``data`` function.
        """
        try:
            lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                   destroy = True)
            lookup(kargs, "cur", default = None, destroy = True)
            if len(kargs) > 0:
                raise NotImplementedError
            return lookup(self._graphprops, "data")
        except (KeyError, TypeError, NotImplementedError):
            return data(self, **kargs)

    @override.documented
    def average_degree(self, *largs, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        default = len(largs) + len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            lookup(self._graphprops, "average_degree")
            return 2*self.size(store = store,
                               cur = cur)/self.order(store = store, cur = cur)
        except (KeyError, NotImplementedError):
            a = Graph.average_degree(self, *largs, **kargs)
            if default:
                if store:
                    self._update_rows(ZooGraph, {"average_degree": a},
                                      {self._spec["primary_key"]: self._zooid},
                                      cur = cur)
                update(self._graphprops, "average_degree", a)
            return a

    @override.computed
    def chromatic_index(self, **kargs):
        r"""
        Return the minimal number of colors needed to color the edges of the
        graph.
        """
        return edge_coloring(self, value_only = True)

    @override.derived
    def density(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        o = self.order(store = store, cur = cur)
        if o <= 1:
            return Integer(0)
        return 2*self.size(store = store, cur = cur)/(o*(o-1))

    @override.determined((Column("connected_components_number") != Integer(1),
                          PlusInfinity()))
    def diameter(self, value, attrs, **kargs):
        return (value != PlusInfinity(), attrs)

    @override.determined(is_planar = Integer(0))
    def genus(self, value, attrs, **kargs):
        return (True, attrs)

    @override.determined(is_forest = PlusInfinity())
    def girth(self, value, attrs, **kargs):
        return (value != PlusInfinity(), attrs)

    @override.documented
    def hamiltonian_cycle(self, algorithm = "tsp", *largs, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        default = len(largs) + len(kargs) == 0 and \
            algorithm in ["tsp", "backtrack"]
        if default:
            if algorithm == "tsp":
                try:
                    out = Graph.hamiltonian_cycle(self, algorithm, *largs,
                                                  **kargs)
                    h = True
                except EmptySetError as out:
                    h = False
            elif algorithm == "backtrack":
                out = Graph.hamiltonian_cycle(self, algorithm, *largs, **kargs)
                h = out[0]
            try:
                lookup(self._graphprops, "is_hamiltonian")
            except KeyError:
                if store:
                    self._update_rows(ZooGraph, {"is_hamiltonian": h},
                                      {self._spec["primary_key"]: self._zooid},
                                      cur = cur)
                update(self._graphprops, "is_hamiltonian", h)
            if isinstance(out, BaseException):
                raise out
            else:
                return out
        else:
            return Graph.hamiltonian_cycle(self, algorithm, *largs, **kargs)

    @override.derived
    def has_loops(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        return self.number_of_loops(store = store, cur = cur) > 0

    @override.derived
    def is_connected(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        return self.connected_components_number(store = store, cur = cur) <= 1

    @override.derived
    def is_half_transitive(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        return (self.is_edge_transitive(store = store, cur = cur) and
            self.is_vertex_transitive(store = store, cur = cur) and
            not self.is_arc_transitive(store = store, cur = cur))

    @override.documented
    def is_regular(self, k = None, *largs, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        default = len(largs) + len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            r = lookup(self._graphprops, "is_regular")
            return r and (True if k is None
                          else k == self.average_degree(store = store,
                                                        cur = cur))
        except (KeyError, NotImplementedError):
            r = Graph.is_regular(self, k, *largs, **kargs)
            if default:
                if store:
                    row = {"is_regular": r}
                    if r and k is not None:
                        row["average_degree"] = k
                    self._update_rows(ZooGraph, row,
                                      {self._spec["primary_key"]: self._zooid},
                                      cur = cur)
                update(self._graphprops, "is_regular", r)
                if r and k is not None:
                    update(self._graphprops, "average_degree", k)
            return r

    @override.derived
    def is_semi_symmetric(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not self.is_bipartite(store = store, cur = cur):
            return False
        return (self.is_regular(store = store, cur = cur) and
                self.is_edge_transitive(store = store, cur = cur) and
                not self.is_vertex_transitive(store = store, cur = cur))

    @override.derived
    def is_triangle_free(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        return self.triangles_count(store = store, cur = cur) == 0

    @override.derived
    def is_weakly_chordal(self, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        return self.is_long_hole_free(store = store, cur = cur) \
            and self.is_long_antihole_free(store = store, cur = cur)

    @override.documented
    def name(self, new = None, *largs, **kargs):
        store = lookup(kargs, "store",
                       default = self._initialized and discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        default = len(largs) + len(kargs) == 0
        if default:
            old = lookup(self._graphprops, "name", default = "")
            if new is None:
                return old
            elif new != old:
                if new == "":
                    new = None
                if store:
                    self._update_rows(ZooGraph, {"name": new},
                                      {self._spec["primary_key"]: self._zooid},
                                      cur = cur)
                update(self._graphprops, "name", new)
        else:
            return Graph.name(self, new, *largs, **kargs)

    @override.determined(is_bipartite = PlusInfinity(),
                         is_forest = PlusInfinity())
    def odd_girth(self, value, attrs, **kargs):
        inf = value == PlusInfinity()
        if inf:
            del attrs["is_forest"]
        return (not inf, attrs)

AVAILABLE_ALGORITHMS = ["sage"]
if is_package_installed("bliss"):
    AVAILABLE_ALGORITHMS.insert(0, "bliss")

def canonical_label(graph, **kargs):
    r"""
    Return the canonical labeling of ``graph``.

    INPUT:

    - ``graph`` - the graph to compute the canonical labelling for.

    - ``algorithm`` - the algorithm to use to compute the canonical labelling.
      The default value ``None`` means that ``'bliss'`` will be used if
      available, and ``'sage'`` otherwise.
    """
    algorithm = lookup(kargs, "algorithm", default = None)
    return graph.canonical_label(algorithm = algorithm)

def data(graph, **kargs):
    r"""
    Return the data for the canonical labeling of ``graph``.

    Currently, it returns the sparse6 string.

    INPUT:

    - ``graph`` - the graph to get the data for.

    - ``algorithm`` - the algorithm to use to compute the canonical labelling.
      The default value ``None`` means that ``'bliss'`` will be used if
      available, and ``'sage'`` otherwise.
    """
    # TODO: determine the most appropriate way of representing the graph
    algorithm = lookup(kargs, "algorithm", default = None)
    return canonical_label(graph, algorithm = algorithm).sparse6_string()

def unique_id(graph, **kargs):
    r"""
    Return the unique ID of ``graph``.

    The unique ID is the SHA256 hash of the string returned by ``data``.
    If ``graph`` is an instance of ``ZooGraph`` and ``store`` is set to
    ``True``, the computed unique ID is stored to the database.

    INPUT:

    - ``graph`` - the graph to compute the unique ID for.

    - ``algorithm`` - the algorithm to use to compute the canonical labelling.
      The default value ``None`` means that ``'bliss'`` will be used if
      available, and ``'sage'`` otherwise.

    - ``store`` - whether to store the computed unique ID to the database
      (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

    - ``cur`` - the cursor to use for database interaction
      (must be a named parameter; default: ``None``).
    """
    algorithm = lookup(kargs, "algorithm", default = None)
    store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
    cur = lookup(kargs, "cur", default = None)
    uid = sha256(data(graph, algorithm = algorithm)).hexdigest()
    if isinstance(graph, ZooGraph):
        graph.unique_id().__setitem__(algorithm, uid, store = store,
                                      cur = cur)
    return uid

def import_graphs(file, cl = ZooGraph, db = None, format = "sparse6",
                  index = "index", verbose = False):
    r"""
    Import graphs from ``file`` into the database.

    This function is used to import new censuses of graphs and is not meant
    to be used by users of DiscreteZOO.

    To properly import the graphs, all graphs of the same order must be
    together in the file, and no graph of this order must be present in the
    database.

    INPUT:

    - ``file`` - the filename containing a graph in each line.

    - ``cl`` - the class to be used for imported graphs
      (default: ``ZooGraph``).

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
    info = ZooInfo(cl)
    if db is None:
        db = info.getdb()
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
            n = g.order()
            if n > previous:
                if verbose and previous > 0:
                    print "Imported %d graphs of order %d" % (i, previous)
                previous = n
                i = 0
            i += 1
            cl(graph = g, order = n, cur = cur, db = db, **{index: i})
        if verbose:
            print "Imported %d graphs of order %d" % (i, n)
        f.close()
    cur.close()
    db.commit()

info = ZooInfo(ZooGraph)
