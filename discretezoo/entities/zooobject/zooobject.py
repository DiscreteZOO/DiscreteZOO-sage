r"""
A superclass for all DiscreteZOO objects

This module contains a class which all DiscreteZOO objects extend.
"""

import json
import os
import re
from types import BuiltinFunctionType
from types import MethodType
from warnings import warn
import discretezoo
from ..change import Change
from ..zooentity import ZooEntity
from ..zooentity import ZooInfo
from ..zooproperty import ZooProperty
from ...db.query import Column
from ...db.query import ColumnSet
from ...db.query import Table
from ...util.utility import default
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import to_json
from ...util.utility import update

class ZooObject(ZooEntity):
    r"""
    A class which all DiscreteZOO objects extend.

    Each object is an entity representing mathematical objects.
    """
    _zooprops = None
    _spec = None
    _zooid = None
    _unique_id = None
    _unique_id_algorithm = None
    _parent = ZooEntity
    _dict = "_zooprops"
    _override = None
    _fields = None

    def __init__(self, data = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - the data to construct the entity from (ID or unique ID).

        - ``db`` - the database being used (must be a named parameter;
          default: ``None``).

        - ``store`` - whether to store the object to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (must be a named parameter; default: ``None``).

        - other named parameters are silently ignored.
        """
        self._init_(ZooObject, kargs, setVal = {"data": data})

    def _init_defaults(self, d):
        r"""
        Initialize the default parameters.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        default(d, "zooid")
        default(d, "unique_id")
        default(d, "unique_id_algorithm")

    def _parse_params(self, d):
        r"""
        Parse the ``data`` parameter of the constructor.

        First tries the ``ZooEntity._parse_params`` method, then tries to parse
        ``d["data"]`` as a unique ID, returning ``True`` on success.
        On failure, returns ``False``, indicating that class-specific parsing
        is necessary.

        INPUT:

        - ``d`` - the dictionary of parameters.
        """
        if ZooEntity._parse_params(self, d):
            return True
        elif isinstance(d["data"], basestring) \
                and re.match(r'^[0-9A-Fa-f]{64}$', d["data"]):
            d["unique_id"] = d["data"]
            d["data"] = None
            return True
        else:
            return False

    def _init_object(self, cl, d, setProp = {}):
        r"""
        Initialize the object being represented.

        If either of the ID or the unique ID is missing,
        tries to read it from the database.
        Regardless of whether this is successful,
        continues initializing the object as a ``ZooEntity``.

        INPUT:

        - ``cl`` - the class to initialize the object for.

        - ``d`` - the dictionary of parameters.

        - ``setProp`` - a dictionary mapping field names to names of the
          parameters they should take their value from (default: ``{}``).
        """
        if self._zooid is None:
            self._zooid = d["zooid"]
        if self._unique_id is None:
            self._unique_id = d["unique_id"]
            self._unique_id_algorithm = d["unique_id_algorithm"]
        if self._zooid is None or self._unique_id is None:
            try:
                r = self._db_read(cl, kargs = d)
                self._zooid = r["zooid"]
                if self._unique_id is None:
                    uid = self._fields.unique_id
                    cur = self._db.query([uid.algorithm, uid], uid.getJoin(),
                                         {uid.foreign: self._zooid},
                                          limit = 1, cur = d["cur"])
                    r = cur.fetchone()
                    if r is not None:
                        self._unique_id_algorithm, self._unique_id = r
            except KeyError:
                pass
        ZooEntity.__init__(self, **d)

    def _copy_props(self, cl, obj):
        r"""
        Copy properties and methods from ``obj``.

        INPUT:

        - ``cl`` - the lowest class of ``self`` to copy.

        - ``obj`` - the object to copy from.
        """
        c = cl
        while c is not None:
            if isinstance(obj, c):
                self._setprops(c, obj._getprops(c))
            c = c._parent
        c = obj.__class__
        cl = self.__class__
        while c is not None and not issubclass(cl, c):
            self.__setattr__(c._dict, obj._getprops(c))
            self._extra_classes.add(c)
            c = c._parent
        for c in obj._extra_classes:
            try:
                self.__getattribute__(c._dict)
            except AttributeError:
                self.__setattr__(c._dict, obj.__getattribute__(c._dict))
                self._extra_classes.add(c)
        for a in dir(obj):
            if a not in dir(self):
                attr = obj.__getattribute__(a)
                if isinstance(attr, MethodType):
                    self.__setattr__(a, MethodType(attr.im_func, self, cl))

    def _db_read_nonprimary(self, cur = None):
        r"""
        Read properties from the database identified by the unique ID
        of the object.

        Returns whether the query was successful.

        INPUT:

        - ``cur`` - the cursor to use for database interaction
          (default: ``None``).
        """
        if self._unique_id is not None:
            uid = self._fields.unique_id
            query = {uid.column: self._unique_id}
            cur = self._db.query([Column(ZooObject._spec["primary_key"],
                                         table = ZooObject._spec["name"]),
                                  uid.algorithm.column],
                                 uid.getJoin(), query, cur = cur)
            r = cur.fetchone()
            if r is None:
                raise KeyError(query)
            self._zooid, self._unique_id_algorithm = r
            return True
        return False

    def _db_write_nonprimary(self, cur):
        r"""
        Write the unique ID to the database.

        INPUT:

        - ``cur`` - the cursor to use for database interaction.
        """
        if self._unique_id is None:
            raise ValueError("Insufficient data to construct the object")
        uid = self.unique_id()
        uid.__setitem__(self._unique_id_algorithm, self._unique_id,
                        store = True, cur = cur)

    def _add_change(self, cl, cur):
        r"""
        Record a change after adding the entity to the database.

        INPUT:

        - ``cl`` - the class to write the properties for.

        - ``cur`` - the cursor to use for database interaction.
        """
        Change(self._zooid, cl, cur = cur)

    def _getattr(self, name, parent):
        r"""
        Provide a wrapper for Sage's methods.

        When a method that corresponds to an attribute of the object is called,
        DiscreteZOO checks whether it already has a result. Since most such
        methods consist simply of calling Sage's method if this is not the
        case, this methods provides a generic way of calling them.
        If the desired Sage's method has been overridden by DiscreteZOO, then
        the overriding method is used. Otherwise, a function is constructed
        to take care of checking for the availability of the result,
        calling the appropriate Sage's method if needed, and finally
        recording the result back to the database. This function is then
        returned with the appropriate documentation.

        INPUT:

        - ``name`` - the name of the attribute being requested.

        - ``parent`` - Sage's class being extended.
        """
        try:
            attr = parent.__getattribute__(self, name)
            error = False
        except AttributeError as ex:
            attr = None
            error = True
        if error or (isinstance(attr, MethodType) and
                (isinstance(attr.im_func, BuiltinFunctionType) or
                    (attr.func_globals["__package__"] is not None and
                     attr.func_globals["__package__"].startswith("sage.")) or
                    (attr.func_globals["__name__"] is not None and
                     attr.func_globals["__name__"].startswith("sage.")))):
            try:
                cl, name = self._getclass(name, alias = True)
            except KeyError:
                if error:
                    raise ex
                return attr
            def _attr(*largs, **kargs):
                store = lookup(kargs, "store",
                               default = discretezoo.WRITE_TO_DB,
                               destroy = True)
                cur = lookup(kargs, "cur", default = None, destroy = True)
                default = len(largs) + len(kargs) == 0
                props = self._getprops(cl)
                try:
                    if not default:
                        raise NotImplementedError
                    a = lookup(props, name)
                    if issubclass(cl._spec["fields"][name], ZooObject) \
                            and isinteger(a):
                        a = cl._spec["fields"][name](zooid = a)
                        update(props, name, a)
                    return a
                except (KeyError, NotImplementedError):
                    if error:
                        raise NotImplementedError
                    a = attr(*largs, **kargs)
                    if default:
                        if store:
                            self._update_rows(cl, {name: a},
                                    {self._spec["primary_key"]: self._zooid},
                                    cur = cur)
                        update(props, name, a)
                    return a
            _attr.func_name = name
            if self._override is None:
                return _attr
            else:
                return self._override.documented(_attr, attr)
        return attr

    @staticmethod
    def _get_column(cl, name, table, join = None, by = None):
        r"""
        Return a ``Column`` object for a property of the given class.

        INPUT:

        - ``cl`` - the class to get the object for.

        - ``name`` - the name of the property. Ignored in this context.

        - ``table`` - the table containing the objects whose properties are
          represented by ``cl``.

        - ``join`` - a join of tables needed to determine the object
          (default: ``None``).

        - ``by`` - the criterion to join by (default: ``None``).
          See ``db.query.Table.join`` for more information.
        """
        if not isinstance(table, Table):
            table = Table(table)
        if name == cl._spec["primary_key"]:
            return Column(name, table = table, join = join, by = by)
        else:
            if join is None:
                join = table
            else:
                join = join.join(table, by = by)
            return ColumnSet(cl, cl._spec["primary_key"], join = join,
                             by = ((cl._spec["primary_key"],
                                    Column(name, table = table)), ))

    def _to_json(self):
        r"""
        Return an object suitable for conversion to JSON.

        Returns a tuple containing an algorithm and the unique ID
        produced by the former.
        """
        return next(self.unique_id().iteritems())

    def _to_json_dict(self):
        r"""
        Return the complete description of the object as a ``dict``
        suitable for conversion to JSON.
        """
        c = self.__class__
        d = {}
        while c is not ZooEntity:
            d[c._spec["name"]] = self._to_json_field(c)
            c = c._parent
        for c in self._extra_classes:
            d[c._spec["name"]] = self._to_json_field(c)
        return d

    def _to_json_field(self, cl = None, field = None):
        r"""
        Return a dictionary with fields corresponding to the class ``cl``.

        INPUT:

        - ``cl`` - the class to get the fields for.
          If ``None`` (default), the class of the object is used.

        - ``field`` - if ``None`` (default), then the returned dictionary
          will contain all existing fields corresponding to ``cl``
          (i.e., those whose values are not ``None``);
          otherwise, the dictionary will only contain the value ``field``
          with its value (which could also be ``None``).
        """
        if cl is None:
            cl = self.__class__
        elif not issubclass(cl, ZooObject):
            return ZooEntity._to_json_field(self, cl = cl)
        if field is None:
            d = self.__getattribute__(cl._dict)
            for f, t in cl._spec["fields"].items():
                if f != cl._spec["primary_key"] and f not in d and \
                        issubclass(t, ZooEntity) and \
                        not issubclass(t, ZooObject):
                    self.__getattribute__(f)()
            d = dict(d)
            self._to_json_field_extra(cl, d)
            for f in cl._spec["skip"]:
                if f != cl._spec["primary_key"]:
                    d[f] = self.__getattribute__(f)()
        else:
            d = {field: self.__getattribute__(field)()}
            self._to_json_field_extra(cl, d, field)

        return {f: to_json(v, cl._spec["fields"][f]) for f, v in d.items()
                if v is not None}

    def _to_json_field_extra(self, cl, d, field = None):
        r"""
        Perform extra tweaking of the dictionary for the JSON encoding.

        To be overridden.

        INPUT:

        - ``cl`` - the class to get the fields for.

        - ``d`` - the dictionary to tweak.

        - ``field`` - the field to tweak; if ``None`` (default), all fields
          may be tweaked.
        """
        pass

    def alias(self):
        r"""
        Return the set of aliases of the object.
        """
        try:
            return lookup(self._zooprops, "alias")
        except KeyError:
            self._zooprops["alias"] = ZooObject._spec["fields"]["alias"](self._zooid)
            return self._zooprops["alias"]

    def unique_id(self):
        r"""
        Return the dictionary of unique IDs of the object.
        """
        try:
            return lookup(self._zooprops, "unique_id")
        except KeyError:
            self._zooprops["unique_id"] = ZooObject._spec["fields"]["unique_id"](self._zooid)
            return self._zooprops["unique_id"]

    def write_json(self, location, folder = "objects", field = None,
                   link = True):
        r"""
        Write a JSON representation of the object to the appropriate file
        in the repository at ``location``.

        INPUT:

        - ``location`` - the location of the repository containing the
          objects.

        - ``folder`` - the path to the folder within the repository where
          the objects will be written (default: ``"objects"``).

        - ``field`` - which fields to export: if ``None`` (default), then
          all fields belonging to all classes will be exported;
          if ``field`` is an instance of ``ZooProperty``,
          then the corresponding field will be updated with its values;
          otherwise ``field`` must be a pair ``(cl, column)``,
          where ``cl`` is the class for which fields will be exported,
          and ``column`` is ``None`` if all its fields should be exported,
          or a string specifying a field.
        """
        targets = {}
        target = None
        links = []
        found = False
        obj = os.path.join(location, folder)
        for a, u in self.unique_id().items():
            auu = (a, u[:2], u[2:])
            path = os.path.join(obj, *auu)
            if os.path.exists(path):
                realpath = os.path.realpath(path)
                targets[realpath] = auu
                if target is None and os.path.abspath(path) == realpath:
                    target = auu
            else:
                links.append(auu)
                if os.path.islink(path):
                    os.remove(path)
        if len(targets) > 0:
            if target is None:
                path, target = next(targets.iteritems())
                warn("Existing symlinks point to target "
                     "not corresponding to a known unique ID")
            else:
                path = os.path.join(obj, *target)
            if len(targets) > 1:
                warn("More than one target found, using %s" % path)
            with open(path) as f:
                d = json.load(f)
        else:
            if len(links) == 0:
                raise ValueError("No unique ID found")
            target = links.pop(0)
            dir = os.path.join(obj, *target[:-1])
            path = os.path.join(dir, target[-1])
            if not os.path.exists(dir):
                os.makedirs(dir)
            d = {}
        if field is None:
            for k, v in self._to_json_dict().items():
                if k in d:
                    d[k].update(v)
                else:
                    d[k] = v
        elif isinstance(field, ZooProperty):
            table = field._foreign_obj._spec["name"]
            if table not in d:
                d[table] = {}
            op = "_add" if field._deleted is False else "_delete"
            if op not in d[table]:
                d[table][op] = {}
            field._update_json(d[table][op])
        else:
            cl, column = field
            if cl._spec["name"] not in d:
                d[cl._spec["name"]] = {}
            d[cl._spec["name"]].update(self._to_json_field(cl, column))
        with open(path, "w") as f:
            json.dump(d, f, indent = 4, sort_keys = True)
        if link:
            rel = os.path.join("..", "..", *target)
            for l in links:
                dir = os.path.join(obj, *l[:-1])
                if not os.path.exists(dir):
                    os.makedirs(dir)
                os.symlink(rel, os.path.join(dir, l[-1]))

info = ZooInfo(ZooObject)
