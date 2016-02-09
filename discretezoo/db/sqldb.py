import os
import sqlite3
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from sage.rings.real_mpfr import create_RealNumber
from . import query
from .db import DB
from .query import enlist
from ..util.utility import int_or_real
from ..entities.zooentity import ZooEntity
from ..entities.zooproperty import ZooProperty

class SQLDB(DB):
    db = None
    
    data_string = None
    ident_quote = None
    exceptions = ()

    none_val = 'NULL'
    random = 'RANDOM()'

    convert_to = {
        Integer: int,
        Rational: float,
        RealNumber: float,
        str: str,
        bool: int,
        ZooEntity: int
    }

    convert_from = {
        int: Integer,
        float: create_RealNumber,
        Integer: Integer,
        Rational: int_or_real,
        RealNumber: create_RealNumber,
        str: str,
        bool: bool,
        ZooEntity: Integer
    }
    
    types = {
        Integer: 'INTEGER',
        Rational: 'REAL',
        RealNumber: 'REAL',
        str: 'TEXT',
        bool: 'INTEGER',
        ZooEntity: 'INTEGER'
    }

    constraints = {
        'not_null': 'NOT NULL',
        'primary_key': 'PRIMARY KEY',
        'unique': 'UNIQUE'
    }

    binaryops = {
        query.LessThan: '<',
        query.LessEqual: '<=',
        query.Equal: '=',
        query.NotEqual: '<>',
        query.GreaterThan: '>',
        query.GreaterEqual: '>=',
        query.Plus: '+',
        query.Minus: '-',
        query.Times: '*',
        query.Divide: '/',
        query.FloorDivide: '/',
        query.Modulo: '%%',
        query.LeftShift: '<<',
        query.RightShift: '>>',
        query.BitwiseAnd: '&',
        query.BitwiseOr: '|',
        query.Concatenate: '||',
        query.Like: 'LIKE',
        query.In: 'IN'
    }

    unaryops = {
        query.Not: ('NOT', True),
        query.Absolute: ('abs', None),
        query.Negate: ('-', True),
        query.Invert: ('~', True),
        query.IsNull: ('IS NULL', False),
        query.IsNotNull: ('IS NOT NULL', False)
    }

    logicalexps = {
        query.And: ' AND ',
        query.Or: ' OR '
    }

    logicalconsts = {
        query.And: '1',
        query.Or: '0'
    }

    def quoteIdent(self, ident):
        return '%s%s%s' % (self.ident_quote, ident, self.ident_quote)

    def tableAlias(self, table):
        return table.tables[0]['alias'] if isinstance(table, query.Table) \
               else table

    def binaryOp(self, op, left, right):
        if isinstance(op, query.Divide):
            left = 'CAST(%s AS %s)' % (left, self.types[Rational])
        return '(%s) %s (%s)' % (left, self.binaryops[op.__class__], right)

    def unaryOp(self, op, exp):
        k, p = self.unaryops[op.__class__]
        if p is True:
            return '%s (%s)' % (k, exp)
        elif p is False:
            return '(%s) %s' % (exp, k)
        else:
            return '%s(%s)' % (k, exp)

    def makeType(self, t, c):
        cons = ''.join([' ' + self.constraints[x] for x in c])
        if issubclass(t, ZooEntity):
            return self.types[ZooEntity] + cons + ' REFERENCES %s(%s)' \
                                % (self.quoteIdent(t._spec['name']),
                                    self.quoteIdent(t._spec['primary_key']))
        else:
            return self.types[t] + cons

    def makeTable(self, t, alias = True):
        if isinstance(t, query.Table):
            if not alias and len(t.tables) == 1:
                return self.makeTable(t.tables[0]['table'], alias = False)
            tdata = [self.makeTable(x['table']) if x['alias'] is None
                     else self.makeTable(x['table'], alias = False)
                     for x in t.tables]
            tables = ['%s' % tdata[i][0] if x['alias'] is None
                      else '%s AS %s' % (tdata[i][0],
                                         self.quoteIdent(x['alias']))
                        for i, x in enumerate(t.tables)]
            aliases = [query.Table.name(x) for x in t.tables]
            joins = [' %sJOIN ' % ('LEFT ' if x['left'] else '')
                        for x in t.tables]
            bydata = [{} if not isinstance(x['by'], tuple) else
                      {k: self.makeExpression(v) for k, v in x['by']}
                      for x in t.tables]
            bykeys = [d.keys() for d in bydata]
            using = ['' if x['by'] is None or len(x['by']) == 0 else
                     ((' USING (%s)' % ', '.join([self.quoteIdent(c)
                                                  for c in x['by']]))
                        if isinstance(x['by'], frozenset)
                        else ' ON %s' %
                            ' AND '.join(['%s.%s = %s' %
                                            (self.quoteIdent(aliases[i]),
                                             self.quoteIdent(k),
                                             bydata[i][k][0])
                                          for k in bykeys[i]]))
                     for i, x in enumerate(t.tables)]
            out = tables[0] + ''.join([joins[i] + tables[i] + using[i]
                                         for i in range(1, len(t.tables))])
            data = sum([tdata[i][1] + sum([bydata[i][k][1] for k in keys], [])
                        for i, keys in enumerate(bykeys)], [])
            if len(tables) > 1:
                out = "(%s)" % out
            return out, data
        else:
            return self.quoteIdent(t), []

    def makeExpression(self, exp, alias = False):
        if exp is None:
            return (self.none_val, [])
        elif isinstance(exp, query.All):
            return ('*', [])
        elif isinstance(exp, query.Table):
            return ('%s.*' % self.quoteIdent(exp.tables[0]['alias']), [])
        else:
            exp = query.makeExpression(exp)
        if isinstance(exp, query.Value):
            return (self.data_string, [self.to_db_type(exp.value)])
        elif isinstance(exp, query.Column):
            if isinstance(exp.column, basestring):
                sql = self.quoteIdent(exp.column)
                data = []
                if exp.table is not None:
                    sql = "%s.%s" % \
                            (self.quoteIdent(self.tableAlias(exp.table)), sql)
            else:
                sql, data = self.makeExpression(exp.column)
            if alias and exp.colalias is not None:
                sql += ' AS %s' % self.quoteIdent(exp.colalias)
            return (sql, data)
        elif isinstance(exp, query.LogicalExpression):
            word = self.logicalexps[exp.__class__]
            const = self.logicalconsts[exp.__class__]
            if len(exp.terms) == 0:
                return (const, [])
            else:
                q = [self.makeExpression(x) for x in exp.terms]
                return (word.join(['(%s)' % x[0] for x in q]),
                        sum([x[1] for x in q], []))
        elif isinstance(exp, query.BinaryOp):
            lq, ld = self.makeExpression(exp.left)
            rq, rd = self.makeExpression(exp.right)
            return (self.binaryOp(exp, lq, rq), ld + rd)
        elif isinstance(exp, query.UnaryOp):
            q, d = self.makeExpression(exp.exp)
            return (self.unaryOp(exp, q), d)
        elif isinstance(exp, query.Random):
            return (self.random, [])
        elif isinstance(exp, query.Count):
            sql, data = self.makeExpression(exp.column)
            if exp.distinct:
                sql = 'COUNT(DISTINCT %s)' % sql
            else:
                sql = 'COUNT(%s)' % sql
            return (sql, data)
        elif isinstance(exp, query.Subquery):
            return self.query(exp.columns, exp.table, cond = exp.cond,
                              groupby = exp.groupby, orderby = exp.orderby,
                              limit = exp.limit, offset = exp.offset,
                              subquery = True)
        else:
            raise NotImplementedError

    def cursor(self, **kargs):
        return self.db.cursor(**kargs)

    def commit(self, **kargs):
        self.db.commit(**kargs)

    def rollback(self, **kargs):
        self.db.rollback(**kargs)

    def handle_exception(self, ex):
        self.rollback()
        raise ex

    def createIndex(self, cur, name, idx):
        raise NotImplementedError

    def init_table(self, spec, commit = False):
        try:
            pkey = enlist(spec['primary_key'])
            idxs = [k[0] if isinstance(k, tuple) else k
                    for k in spec['indices']]
            idxs = [enlist(k) for k in idxs]
            if isinstance(spec['indices'], set):
                idxs = sorted(idxs)
            idxs = sum(idxs, [])
            ext = {k: v for k, v in spec['fields'].items()
                    if issubclass(v, ZooProperty)}
            cols = pkey[:]
            cols += [idxs[i] for i in range(len(idxs))
                     if idxs[i] not in (cols + idxs[:i])]
            cols += sorted([k for k in spec['fields'] if k not in ext
                            and k in spec['fieldparams'] and k not in cols])
            cols += sorted([k for k in spec['fields']
                            if k not in ext and k not in cols])
            colspec = ['%s %s' % (self.quoteIdent(k),
                                  self.makeType(spec['fields'][k],
                                                spec['fieldparams'][k]
                                                    if k in spec['fieldparams']
                                                    else set()))
                       for k in cols]
            if len(pkey) == 1 and pkey[0] in spec['fieldparams'] and \
                    "autoincrement" in spec['fieldparams'][pkey[0]]:
                pkey = []
            if len(pkey) > 0:
                colspec += ["PRIMARY KEY (%s)" % ', '.join(pkey)]
            cur = self.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS %s (%s)' %
                                                (self.quoteIdent(spec['name']),
                                                 ', '.join(colspec)))
            for idx in spec['indices']:
                self.createIndex(cur, spec['name'], idx)
            cur.close()
            for c in ext.values():
                self.init_table(c._spec, commit = False)
            if commit:
                self.db.commit()
        except self.exceptions as ex:
            self.handle_exception(ex)

    def returning(self, id):
        return ''

    def limit(self, limit = None, offset = None):
        out = ''
        if limit is not None:
            out += ' LIMIT %d' % limit
            if offset is not None:
                out += ' OFFSET %d' % offset
        return out

    def insert_row(self, table, row, cur = None, commit = None, id = None):
        try:
            cols = [c for c in row if row[c] is not None]
            if cur is False:
                cur = None
                ret = False
            else:
                ret = True
            if cur is None:
                cur = self.cursor()
            if len(cols) == 0:
                sql = 'INSERT INTO %s DEFAULT VALUES%s' % \
                            (self.quoteIdent(table), self.returning(id))
                data = []
            else:
                sql = 'INSERT INTO %s (%s) VALUES (%s)%s' % \
                            (self.quoteIdent(table),
                                ', '.join([self.quoteIdent(c) for c in cols]),
                                ', '.join([self.data_string] * len(cols)),
                                self.returning(id))
                data = [self.to_db_type(row[c]) for c in cols]
            cur.execute(sql, data)
            if ret:
                if commit:
                    self.db.commit()
                return cur
            else:
                cur.close()
                if commit is not False:
                    self.db.commit()
        except self.exceptions as ex:
            self.handle_exception(ex)

    def lastrowid(self, cur):
        return cur.lastrowid

    def update_rows(self, table, row, cond = False, cur = None, commit = None):
        if cond is False:
            raise UserWarning("false condition given; to change all rows specify cond = None")
        if cur is False:
            cur = None
            ret = False
        else:
            ret = True
        if len(row) == 0:
            return cur if ret else None
        try:
            t, data = self.makeTable(table)
            cols = row.keys()
            s = ', '.join(['%s = %s' % (self.quoteIdent(c), self.data_string)
                           for c in cols])
            data += [self.to_db_type(row[c]) for c in cols]
            w = ''
            if cond is not None:
                w, d = self.makeExpression(cond)
                w = ' WHERE %s' % w
                data += d
            sql = 'UPDATE %s SET %s%s' % (t, s, w)
            if cur is None:
                cur = self.cursor()
            cur.execute(sql, data)
            if ret:
                if commit:
                    self.db.commit()
                return cur
            else:
                cur.close()
                if commit is not False:
                    self.db.commit()
        except self.exceptions as ex:
            self.handle_exception(ex)

    def delete_rows(self, table, cond = False, cur = None, commit = None):
        if cond is False:
            raise UserWarning("false condition given; to delete all rows specify cond = None")
        if cur is False:
            cur = None
            ret = False
        else:
            ret = True
        try:
            t, data = self.makeTable(table)
            w = ''
            if cond is not None:
                w, d = self.makeExpression(cond)
                w = ' WHERE %s' % w
                data += d
            sql = 'DELETE FROM %s%s' % (t, w)
            if cur is None:
                cur = self.cursor()
            cur.execute(sql, data)
            if ret:
                if commit:
                    self.db.commit()
                return cur
            else:
                cur.close()
                if commit is not False:
                    self.db.commit()
        except self.exceptions as ex:
            self.handle_exception(ex)

    def query(self, columns, table, cond = None, groupby = None,
              orderby = None, limit = None, offset = None, distinct = False,
              cur = None, subquery = False):
        try:
            dist = 'DISTINCT ' if distinct else ''
            cols = [self.makeExpression(col, alias = True) for col in columns]
            c = ', '.join([x[0] for x in cols])
            data = sum([x[1] for x in cols], [])
            t, d = self.makeTable(table)
            data += d
            w = ''
            if cond is not None:
                w, d = self.makeExpression(cond)
                w = ' WHERE %s' % w
                data += d
            g = ''
            if groupby is not None:
                if not isinstance(groupby, (set, list)):
                    groupby = [groupby]
                if len(groupby) > 0:
                    groups = [self.makeExpression(grp) for grp in groupby]
                    g = ' GROUP BY %s' % ', '.join([x[0] for x in groups])
                    data += sum([x[1] for x in groups], [])
            o = ''
            if orderby is not None:
                if isinstance(orderby, dict):
                    orderby = orderby.items()
                elif not isinstance(orderby, (list, set)):
                    orderby = [orderby]
                if len(orderby) > 0:
                    orderby = [query.Order(x) for x in orderby]
                    orderby = [(self.makeExpression(v.exp), v.order)
                                for v in orderby]
                    o = ' ORDER BY %s' % \
                            ', '.join('%s %s' % (k, 'ASC' if v else 'DESC')
                                        for (k, _), v in orderby)
                    data += sum([x[0][1] for x in orderby], [])
            l = self.limit(limit, offset)
            sql = 'SELECT %s%s FROM %s%s%s%s%s' % (dist, c, t, w, g, o, l)
            if subquery:
                return (sql, data)
            if cur is None:
                cur = self.cursor()
            cur.execute(sql, data)
            return cur
        except self.exceptions as ex:
            self.handle_exception(ex)
