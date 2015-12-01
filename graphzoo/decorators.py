from utility import lookup

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

    def derived(this, fun):
        def decorated(self, *largs, **kargs):
            store = lookup(kargs, "store", default = False, destroy = True)
            if len(largs) + len(kargs) == 0:
                return fun(self, store = store)
            else:
                return type.__getattribute__(this.cl, fun.func_name)(self, *largs, **kargs)
        decorated.func_name = fun.func_name
        return this.documented(decorated)

    def implied(this, props, attrs):
        def _implied(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store", default = False, destroy = True)
                default = len(largs) + len(kargs) == 0
                d = self.__getattribute__(props)
                try:
                    if not default:
                        raise NotImplementedError
                    return all(lookup(d, k) == v for k, v in attrs.items())
                except (KeyError, NotImplementedError):
                    a = type.__getattribute__(this.cl, fun.func_name)(self, *largs, **kargs)
                    if a and default and store:
                        for k, v in attrs.items():
                            update(d, k, v)
                    return a
            decorated.func_name = fun.func_name
            return this.documented(decorated)
        return _implied

    def determined(this, props, attrs):
        def _determined(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store", default = False, destroy = True)
                default = len(largs) + len(kargs) == 0
                d = self.__getattribute__(props)
                try:
                    if not default:
                        raise NotImplementedError
                    for a, v in attrs.items():
                        try:
                            if lookup(d, a):
                                return v
                        except KeyError:
                            pass
                    return lookup(d, fun.func_name)
                except (KeyError, NotImplementedError):
                    a = type.__getattribute__(this.cl, fun.func_name)(self, *largs, **kargs)
                    if default and store:
                        update(d, fun.func_name, a)
                    return a
            decorated.func_name = fun.func_name
            return this.documented(decorated)
        return _determined
