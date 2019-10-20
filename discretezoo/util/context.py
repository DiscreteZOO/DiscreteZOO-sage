r"""
Context managers

This module provides a context manager providing a stack of database
parameters.
"""

import sys
import discretezoo
from .utility import lookup

PARAMS_STACK = {}


class DBParams(object):
    r"""
    A context manager class for database parameter stack access.
    """

    def __init__(self, locals, store, cur):
        r"""
        Object constructor.

        INPUT:

        - ``locals`` - the dictionary of local variables.

        - ``store`` -- whether to store the computed results back to the
          database.

        - ``cur`` -- the cursor to use for database interaction.
        """
        self.lid = id(locals)
        self.store = store
        self.cur = cur

    def __enter__(self):
        r"""
        Push the database parameters to the settings stack.
        """
        PARAMS_STACK[self.lid] = (self.store, self.cur)

    def __exit__(self, exc_type, exc_value, tb):
        r"""
        Pop the database parameters from the settings stack.
        """
        PARAMS_STACK.pop(self.lid, None)

    @staticmethod
    def get(kargs, destroy=False, initialized=True):
        r"""
        Extract database parameters from a dictionary of named parameters.

        If one or both parameters is not found in the dictionary, a parent
        frame with a corresponding entry in the PARAMS stack is sought. Failing
        this, the default values are used.

        INPUT:

        - ``kargs`` -- a dictionary of named parameters.

        - ``destroy`` (default ``False``) -- whether to delete the relavant
          keys if they exist.
        """
        try:
            store = lookup(kargs, "store", destroy=destroy)
            has_store = True
        except KeyError:
            has_store = False
        try:
            cur = lookup(kargs, "cur", destroy=destroy)
            has_cur = True
        except KeyError:
            has_cur = False
        if not (has_store and has_cur):
            i = 0
            try:
                while True:
                    lid = id(sys._getframe(i).f_locals)
                    if lid in PARAMS_STACK:
                        s_store, s_cur = PARAMS_STACK[lid]
                        if not has_store:
                            store = s_store
                        if not has_cur:
                            cur = s_cur
                        break
                    i += 1
            except (ValueError, AttributeError):
                if not has_store:
                    store = initialized and discretezoo.WRITE_TO_DB
                if not has_cur:
                    cur = None
            if not destroy:
                if not has_store:
                    kargs["store"] = store
                if not has_cur:
                    kargs["cur"] = cur
        return (store, cur)
