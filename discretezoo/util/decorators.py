import discretezoo
from .utility import lookup
from .utility import update

class ZooDecorator:
    cl = None

    def __init__(this, cl):
        this.cl = cl

    def documented(this, fun):
        try:
            fun.__doc__ = type.__getattribute__(this.cl, fun.func_name).__doc__
        except AttributeError:
            pass
        return fun

    def computed(this, fun):
        def decorated(self, *largs, **kargs):
            store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                           destroy = True)
            cur = lookup(kargs, "cur", default = None, destroy = True)
            default = len(largs) + len(kargs) == 0
            d = self._getprops(fun.func_name)
            try:
                if not default:
                    raise NotImplementedError
                return lookup(d, fun.func_name)
            except (KeyError, NotImplementedError):
                a = fun(self, store = store, cur = cur, *largs, **kargs)
                if default:
                    if store:
                        self._update_rows(self._getclass(fun.func_name),
                                    {fun.func_name: a},
                                    {self._spec["primary_key"]: self._zooid},
                                    cur = cur)
                    update(d, fun.func_name, a)
                return a
        decorated.func_name = fun.func_name
        return this.documented(decorated)

    def derived(this, fun):
        def decorated(self, *largs, **kargs):
            store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                           destroy = True)
            cur = lookup(kargs, "cur", default = None, destroy = True)
            if len(largs) + len(kargs) == 0:
                return fun(self, store = store, cur = cur)
            else:
                return type.__getattribute__(this.cl,
                                             fun.func_name)(self,
                                                            *largs, **kargs)
        decorated.func_name = fun.func_name
        return this.documented(decorated)

    def implied(this, value = True, **attrs):
        def _implied(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store",
                               default = discretezoo.WRITE_TO_DB,
                               destroy = True)
                cur = lookup(kargs, "cur", default = None, destroy = True)
                default = len(largs) + len(kargs) == 0
                d = self._getprops(fun.func_name)
                try:
                    if not default:
                        raise NotImplementedError
                    return all(lookup(self._getprops(k), k) == v
                               for k, v in attrs.items()) == value
                except (KeyError, NotImplementedError):
                    a = type.__getattribute__(this.cl,
                                              fun.func_name)(self,
                                                             *largs, **kargs)
                    if default and fun(self, a, store = store, cur = cur):
                        if store:
                            t = {}
                            for k, v in attrs.items():
                                cl = self._getclass(k)
                                if cl not in t:
                                    t[cl] = {}
                                t[cl][k] = v
                            for cl, at in t.items():
                                self._update_rows(cl, at,
                                    {self._spec["primary_key"]: self._zooid},
                                    cur = cur)
                        for k, v in attrs.items():
                            update(self._getprops(k), k, v)
                    return a
            decorated.func_name = fun.func_name
            return this.documented(decorated)
        return _implied

    def determined(this, **attrs):
        def _determined(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store",
                               default = discretezoo.WRITE_TO_DB,
                               destroy = True)
                cur = lookup(kargs, "cur", default = None, destroy = True)
                default = len(largs) + len(kargs) == 0
                d = self._getprops(fun.func_name)
                try:
                    if not default:
                        raise NotImplementedError
                    for a, v in attrs.items():
                        try:
                            if lookup(self._getprops(a), a):
                                return v
                        except KeyError:
                            pass
                    return lookup(d, fun.func_name)
                except (KeyError, NotImplementedError):
                    a = type.__getattribute__(this.cl,
                                              fun.func_name)(self,
                                                             *largs, **kargs)
                    if default and fun(self, a, store = store, cur = cur):
                        if store:
                            self._update_rows(self._getclass(fun.func_name),
                                    {fun.func_name: a},
                                    {self._spec["primary_key"]: self._zooid},
                                    cur = cur)
                        update(d, fun.func_name, a)
                    return a
            decorated.func_name = fun.func_name
            return this.documented(decorated)
        return _determined
