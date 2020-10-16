r"""
Method decorators

This module provides some decorators for class methods.
"""

import re
from functools import partial
from functools import wraps
from operator import eq
import discretezoo
from .context import DBParams
from .utility import isinteger
from .utility import lookup
from .utility import update
from ..entities.zooobject import ZooObject


class ZooDecorator(object):
    r"""
    A class providing decorators for overriding methods of a given class.
    """
    cl = None

    def __init__(this, cl):
        r"""
        Object constructor.

        INPUT:

        - ``cl`` - the class whose methods will be overridden.
        """
        this.cl = cl

    def documented(this, fun, attr=None):
        r"""
        Embed the overridden function's documentation.

        If the overriding function has a docstring of its own, it is inserted
        after the INPUT section of the overridden functions's docstring.
        Otherwise, the inserted documentation describes the parameters
        ``store`` and ``cur``.

        INPUT:

        - ``attr`` - if specified, the docstring of ``attr`` will be used
          instead of the docstring of the overridden function.
        """
        doc = []
        basedoc = None
        end = None
        try:
            if attr is None:
                attr = getattr(this.cl, fun.__name__)
            basedoc = attr.__doc__
        except AttributeError:
            pass
        if basedoc is not None:
            m = re.search(r'^\s*(OUTPUT|ALGORITHM|EXAMPLES?|TESTS?):', basedoc,
                          re.MULTILINE)
            if m is None:
                doc.append(basedoc)
            else:
                st = m.start()
                doc.append(basedoc[:st])
                end = basedoc[st:]
        if fun.__doc__ is None:
            s = ''
            if basedoc is not None:
                m = re.search(r'^ +', basedoc, re.MULTILINE)
                if m is not None:
                    s = m.group()
            doc.append(r"""
{0}DiscreteZOO-specific parameters:

{0}- ``store`` - whether to store the computed results back to the database
{0}  (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

{0}- ``cur`` - the cursor to use for database interaction (must be a named
{0}  parameter; default: ``None``).
{0}""".format(s))
        else:
            doc.append(fun.__doc__)
        if end is not None:
            doc.append(end)
        fun.__doc__ = "\n".join(doc)
        return fun

    def computed(this, acceptArgs=None):
        r"""
        Wrap the computing function with database interaction.

        The decorated function should compute the desired property. It should
        accept the following parameters:

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).

        INPUT:

        - ``acceptArgs`` - if specified, the decorated function should
          return a pair containing the value to be stored in the database
          and the actual output. The decorated function will be called only
          if all of the specified arguments are present in ``acceptArgs``.
        """
        def _computed(fun):
            @wraps(fun, assigned=('__module__', '__name__'))
            def decorated(self, *largs, **kargs):
                cl = self._getclass(fun.__name__)
                return self._call(cl, fun.__name__, fun, largs, kargs,
                                  db_params=True, acceptArgs=acceptArgs)
            return this.documented(decorated, fun)
        return _computed

    def derived(this, fun):
        r"""
        Derive a property from other properties.

        The decorated function should derive the desired property. It should
        accept the following parameters:

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).
        """
        @wraps(fun)
        def decorated(self, *largs, **kargs):
            store, cur = DBParams.get(kargs, destroy=True)
            with DBParams(locals(), store, cur):
                if len(largs) + len(kargs) == 0:
                    return fun(self, store=store, cur=cur)
                else:
                    return getattr(this.cl, fun.__name__)(self, *largs,
                                                           **kargs)
        return this.documented(decorated)

    def determined(this, *lattrs, **attrs):
        r"""
        Determine the value of a property if a condition is satisfied.

        The decorated function should return a pair ``(upd, ats)``, where
        ``upd`` is a boolean signifying whether the computed value should be
        updated, and ``ats`` is a dictionary whose keys are names of the
        properties that should be updated to whether the corresponding value
        matches the computed value of the attribute (note that any non-string
        keys will be ignored). The function should accept the following
        parameters:

        - ``value`` - the value computed by the overridden function.

        - ``attrs`` - a dictionary mapping boolean atributes or expressions
          to the values of the sought attribute that they imply if true.

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).

        INPUT:

        - an unnamed attribute should be a tuple whose first element is an
          attribute name or an expressions, and the second element is the value
          of the sought attribute that is implied when the value of the
          attribute or expression represented by the first element is true.

        - a named attribute is equivalent to a tuple containing its name and
          value.
        """
        attrs.update(lattrs)
        attrs = {k: partial(eq, v) for k, v in attrs.items()}

        def _determined(fun):
            @wraps(fun)
            def decorated(self, *largs, **kargs):
                cl = self._getclass(fun.__name__)
                attr = getattr(this.cl, fun.__name__)
                return self._call(cl, fun.__name__, attr, largs, kargs,
                                  determiner=fun, attrs=attrs)
            return this.documented(decorated)
        return _determined
