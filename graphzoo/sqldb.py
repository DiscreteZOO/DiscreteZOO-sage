import os
import sqlite3
import query
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from sage.rings.real_mpfr import create_RealNumber
from db import DB
from utility import int_or_real
from zooobject import ZooObject

class SQLDB(DB):
    db = None
    
    data_string = None
    ident_quote = None
    exceptions = ()

    none_val = 'NULL'

    convert_to = {
        Integer: int,
        Rational: float,
        RealNumber: float,
        str: str,
        bool: int,
        ZooObject: int
    }

    convert_from = {
        int: Integer,
        float: create_RealNumber,
        Integer: Integer,
        Rational: int_or_real,
        RealNumber: create_RealNumber,
        str: str,
        bool: bool,
        ZooObject: Integer
    }
    
    types = {
        Integer: 'INTEGER',
        Rational: 'REAL',
        RealNumber: 'REAL',
        str: 'TEXT',
        bool: 'INTEGER'
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
        query.Modulo: '%',
        query.LeftShift: '<<',
        query.RightShift: '>>',
        query.BitwiseAnd: '&',
        query.BitwiseOr: '|',
        query.Concatenate: '||',
        query.Like: 'LIKE'
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

    def binaryOp(self, op, left, right):
        return '(%s) %s (%s)' % (left, self.binaryops[op.__class__], right)

    def unaryOp(self, op, exp):
        k, p = self.unaryops[op.__class__]
        if p is True:
            return '%s (%s)' % (k, exp)
        elif p is False:
            return '(%s) %s' % (exp, k)
        else:
            return '%s(%s)' % (k, exp)

    def makeType(self, t):
        if isinstance(t, tuple):
            t, c = t
        else:
            c = set()
        cons = ''.join([' ' + self.constraints[x] for x in c])
        if issubclass(t, ZooObject):
            return 'INTEGER' + cons + ' REFERENCES %s(%s)' \
                                % (self.quoteIdent(t._spec['name']),
                                    self.quoteIdent(t._spec['primary_key']))
        else:
            return self.types[t] + cons

    def makeTable(self, t):
        if isinstance(t, query.Table):
            aliases = ['%s' % self.makeTable(x['table'])
                        if x['alias'] is None else '%s AS %s' %
                                                (self.makeTable(x['table']),
                                                self.quoteIdent(x['alias']))
                        for x in t.tables]
            joins = [' %sJOIN ' % ('LEFT ' if x['left'] else '')
                        for x in t.tables]
            using = ['' if len(x['by']) == 0
                     else ' USING (%s)' % ', '.join([self.quoteIdent(c)
                                        for c in x['by']]) for x in t.tables]
            out = aliases[0] + ''.join([joins[i] + aliases[i] + using[i]
                                         for i in range(1, len(t.tables))])
            if len(aliases) > 1:
                out = "(%s)" % out
            return out
        else:
            return self.quoteIdent(t)

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
            else:
                sql, data = self.makeExpression(exp.column)
            if alias and exp.alias is not None:
                sql += ' AS %s' % self.quoteIdent(exp.alias)
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
        elif isinstance(exp, query.Count):
            sql, data = self.makeExpression(exp.column)
            if exp.distinct:
                sql = 'COUNT(DISTINCT %s)' % sql
            else:
                sql = 'COUNT(%s)' % sql
            return (sql, data)
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

    def createIndex(self, cur, name, col):
        raise NotImplementedError

    def init_table(self, spec, commit = False):
        try:
            cur = self.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS %s (%s)' %
                            (self.quoteIdent(spec['name']),
                            ', '.join(['%s %s' % (self.quoteIdent(k),
                                                    self.makeType(v))
                                        for k, v in spec['fields'].items()])))
            for col in spec['indices']:
                self.createIndex(cur, spec['name'], col)
            cur.close()
            if commit:
                self.db.commit()
        except self.exceptions as ex:
            self.handle_exception(ex)

    def returning(self, id):
        return ''

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
            print 'INSERT INTO %s (%s) VALUES (%s)%s' % \
                            (self.quoteIdent(table),
                                ', '.join([self.quoteIdent(c) for c in cols]),
                                ', '.join([self.data_string] * len(cols)),
                                self.returning(id))
            print [self.to_db_type(row[c]) for c in cols]
            cur.execute('INSERT INTO %s (%s) VALUES (%s)%s' %
                            (self.quoteIdent(table),
                                ', '.join([self.quoteIdent(c) for c in cols]),
                                ', '.join([self.data_string] * len(cols)),
                                self.returning(id)),
                        [self.to_db_type(row[c]) for c in cols])
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

    def query(self, columns, table, cond = None, groupby = None,
              orderby = None, limit = None, offset = None, cur = None):
        try:
            t = self.makeTable(table)
            cols = [self.makeExpression(col, alias = True) for col in columns]
            c = ', '.join([x[0] for x in cols])
            data = sum([x[1] for x in cols], [])
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
            if limit is None:
                l = ''
            else:
                l = ' LIMIT %d' % limit
                if offset is not None:
                    l += ' OFFSET %d' % offset
            if cur is None:
                cur = self.cursor()
            cur.execute('SELECT %s FROM %s%s%s%s%s' % (c, t, w, g, o, l), data)
            return cur
        except self.exceptions as ex:
            self.handle_exception(ex)
