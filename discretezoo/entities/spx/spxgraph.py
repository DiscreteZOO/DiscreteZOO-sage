r"""
A class representing split Praeger-Xu graphs

This module contains a representing SPX graphs
and a function defining adjacency in such graphs.
"""

from sage.categories.cartesian_product import cartesian_product
from sage.graphs.graph import Graph
from sage.rings.finite_rings.integer_mod_ring import Integers
from sage.rings.integer import Integer
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zooobject import ZooObject
from ...util.utility import isinteger
from ...util.utility import lookup

class SPXGraph(ZooGraph):
    r"""
    A split Praeger-Xu (2, r, s) graph.

    A SPX(2, r, s) graph is a graph whose vertices are tuples ``(v, n, a)``,
    where ``v`` is a binary string of length ``s``, ``n`` is an integer
    modulo ``n``, and ``a`` is ``+`` or ``-``. The vertices ``(v, n, +)``
    and ``(w, m, -)`` are adjacent if either ``(v, n) == (w, m)``,
    or ``m == n+1`` and ``v[1:] == w[:-1]``.
    """
    _spxprops = None
    _parent = ZooGraph
    _spec = None
    _dict = "_spxprops"

    def __init__(self, data = None, s = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the graph from (anything accepted
          by ``ZooObject`` or Sage's ``Graph``), or the parameter ``r``.

        - ``r`` - the parameter ``r`` if not given as ``data``
          (must be a named parameter; default: ``None``).

        - ``s`` - the parameter ``s`` (default: ``None``).

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
        ZooObject._init_(self, SPXGraph, kargs, defNone = ["r"],
                         setVal = {"data": data, "s": s},
                         setProp = {"spx_r": "r", "spx_s": "s"})

    def _parse_params(self, d):
        r"""
        Parse the ``data`` parameter of the constructor.

        First checks whether both ``r`` and ``s`` are given, and interprets
        ``data`` as the ID otherwise. If ``data`` is not an integer,
        tries the ``ZooGraph._parse_params`` method.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if isinteger(d["data"]):
            if d["s"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["r"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return ZooGraph._parse_params(self, d)

    @staticmethod
    def _construct_spx(r, s, multiedges = None, **kargs):
        r"""
        Construct a SPX(2, r, s) graph.

        Return a tuple containing the data that can be used to construct
        the requested SPX graph, and a boolean indicating whether the
        constructed graph should be considered a multigraph.

        INPUT:

        - ``r`` - the ``r`` parameter indicating the range of the counter.

        - ``s`` - the ``s`` parameter indicating the length of the string.

        - ``multiedges`` - whether the constructed graph should be considered
          a multigraph. If ``None`` (default), the second element of the output
          will be set to ``True`` when ``r = 1``. If ``False`` and ``r = 1``,
          a ``ValueError`` is raised. Otherwise, the second element of the
          output will be ``multiedges``.

        - any other named parameters are silently ignored.
        """
        c = [tuple(x) for x
             in cartesian_product([[tuple(y) for y
                                    in Integers(2)**Integer(s)], Integers(r),
                                   [Integer(1), Integer(-1)]])]
        if r == 1:
            if multiedges is False:
                raise ValueError("A SPX graph with r = 1 has multiple edges")
            data = sum([[((v, n, t), (v, n, -t)),
                         ((v, n, t), (v[1:] + (Integer(0),), n, -t)),
                         ((v, n, t), (v[1:] + (Integer(1),), n, -t))]
                        for v, n, t in c if t == 1], [])
            multiedges = True
        else:
            data = [c, spx_adj]
        return (data, multiedges)

    def _init_object(self, cl, d, setProp = {}):
        r"""
        Initialize the object being represented.

        If ``r`` and ``s`` have been given, tries fetching the graph from the
        database, or construct it if unavailable. The ``ZooGraph._init_object``
        method is then called.

        INPUT:

        - ``cl`` - the class to initialize the object for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        if d["graph"] is None and d["r"] is not None and d["s"] is not None:
            try:
                r = self._db_read(cl, query = {"spx_r": d["r"],
                                               "spx_s": d["s"]}, kargs = d)
                d["zooid"] = r["zooid"]
            except KeyError as ex:
                if not d["store"]:
                    raise ex
                if not isinteger(d["r"]) or not isinteger(d["s"]):
                    raise TypeError("r and s must be positive integers")
                if d["r"] < 1 or d["s"] < 1:
                    raise ValueError("r and s must be positive integers")
                d["data"], d["multiedges"] = self._construct_spx(**d)
                self._construct_graph(d)
                d["data"] = None
        ZooGraph._init_object(self, cl, d, setProp = setProp)

    def _construct_object(self, cl, d):
        r"""
        Prepare all necessary data and construct the graph.

        INPUT:

        - ``cl`` - the class to construct the graph for.

        - ``d`` - the dictionary of parameters.
        """
        ZooGraph.__init__(self, **d)

        if d["r"] is not None and d["s"] is not None:
            assert(d["r"] * 2**(d["s"]+1) == self._graphprops["order"])
        if len(self._spxprops) == 0:
            try:
                self._db_read(cl, kargs = d)
            except KeyError as ex:
                if not d["store"]:
                    raise ex

    def _check_conditions(self, cl, d):
        r"""
        Check the necessary conditions required by the class specification.

        Raise ``AssertionError`` on failure to meet the conditions.
        Also set the parameters ``r`` and ``s``.

        INPUT:

        - ``cl`` - the class to compute the properties for.

        - ``d`` - the dictionary of parameters.
        """
        ZooGraph._check_conditions(self, cl, d)
        d["r"], d["s"] = self._check_spx()

    def _check_spx(self):
        r"""
        Check whether the graph is an SPX graph.

        Raise ``AssertionError`` if the graph is found not to be an SPX graph.
        Otherwise, return a tuple containing the parameters ``r`` and ``s``.
        """
        n = self.order()
        assert n % 4 == 0, "A SPX graph has order divisible by 4"
        A = self.automorphism_group()
        a = A.order()
        O = A.orbits()
        p = len(O)
        q = n//p
        assert q % 2 == 0, "The vertex orbits of an SPX graph have even size"
        t = 0
        while p > 1:
            assert p % 2 == 0, \
                "The number of vertex orbits of an SPX graph is a power of 2"
            p //= 2
            t += 1
        assert all(len(o) == q for o in O), \
               "All vertex orbits of an SPX graph have the same size"
        if t == 0:
            if n == 4:
                return (1, 1)
            elif n == 8:
                return (2, 1)
            elif n == 16:
                assert self.girth() == 4, \
                       "A SPX graph with 16 vertices has girth 4"
                return (2, 2) if a == 32 else (4, 1)
            q = a
        q //= 2
        r = 0
        while q > r:
            assert q % 2 == 0, \
                "The size of the automorphism group of an SPX graph is of form r*2^r"
            q //= 2
            r += 1
        assert q == r, \
            "The size of the automorphism group of an SPX graph is of form r*2^r"
        if t == 0:
            assert n % (2*r) == 0, \
                "The order of an SPX graph is a multiple of 2r"
            p = n//(2*r)
            s = 0
            while p > 1:
                assert p % 2 == 0, \
                    "The order of an SPX graph is a r times a power of 2"
                p //= 2
                s += 1
        else:
            s = r + t
        data, multiedges = self._construct_spx(r, s)
        G = Graph(data, multiedges = multiedges)
        assert self.is_isomorphic(G), "The given graph is not an SPX graph"
        return (r, s)

    def _repr_generic(self):
        r"""
        Return an uncapitalized string representation.
        """
        return "split Praeger-Xu(2, %d, %d) graph on %d vertices" \
                                % (self.spx_r(), self.spx_s(), self.order())

    def spx_r(self):
        r"""
        Return the ``r`` parameter of the SPX(2, r, s) graph.
        """
        return lookup(self._spxprops, "spx_r")

    def spx_s(self):
        r"""
        Return the ``s`` parameter of the SPX(2, r, s) graph.
        """
        return lookup(self._spxprops, "spx_s")

def spx_adj(x, y):
    r"""
    Returns whether ``x`` and ``y`` are adjacent in a SPX graph.

    INPUT:

    - ``x``, ``y`` - tuples containing a tuple of zeros and ones,
      a ring element, and 1 or -1.
    """
    xv, xn, xs = x
    yv, yn, ys = y
    if xs == ys:
        return False
    if xn == yn and xv == yv:
        return True
    if xn + xs != yn:
        return False
    if xs == -1:
        xv, yv = yv, xv
    return xv[1:] == yv[:-1]

info = ZooInfo(SPXGraph)
