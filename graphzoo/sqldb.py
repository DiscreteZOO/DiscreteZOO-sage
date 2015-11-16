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
        query.Is: 'IS',
        query.IsNot: 'IS NOT',
        query.Like: 'LIKE'
    }

    unaryops = {
        query.Not: 'NOT',
        query.Negate: '-',
        query.Invert: '~'
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
        if op == query.Absolute:
            return 'abs(%s)' % exp
        return '%s (%s)' % (self.unaryops[op.__class__], exp)

    def makeType(self, t):
        if type(t) == tuple:
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

    def createIndex(self, cur, name, col):
        raise NotImplementedError

    def init_table(self, spec, commit = False):
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

    def returning(self, id):
        return ''

    def insert_row(self, table, row, cur = None, commit = None, id = None):
        cols = [c for c in row if row[c] is not None]
        if cur is False:
            cur = None
            ret = False
        else:
            ret = True
        if cur is None:
            cur = self.cursor()
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

    def lastrowid(self, cur):
        return cur.lastrowid

    def query(self, columns, table, cond = None, groupby = None,
              orderby = None, limit = None, offset = None, cur = None):
        t = self.makeTable(table)
        cols = [self.makeExpression(col, alias = True) for col in columns]
        c = ', '.join([x[0] for x in cols])
        data = sum([x[1] for x in cols], [])
        if cond is not None:
            w, d = self.makeExpression(cond)
            w = ' WHERE %s' % w
            data += d
        if groupby is None or len(groupby) == 0:
            g = ''
        else:
            if type(groupby) not in (set, list):
                groupby = [groupby]
            groups = [self.makeExpression(grp) for grp in groupby]
            g = ' GROUP BY %s' % ', '.join([x[0] for x in groups])
            data += sum([x[1] for x in groups], [])
        if orderby is None or len(orderby) == 0:
            o = ''
        else:
            if type(orderby) is set:
                orderby = [(k, True) for k in orderby]
            elif type(orderby) is dict:
                orderby = orderby.items()
            if type(orderby) is tuple:
                orderby = [orderby]
            elif type(orderby) is not list:
                orderby = [(orderby, True)]
            else:
                orderby = [k if type(k) is tuple else (k, True)
                           for k in orderby]
                orderby = [(self.makeExpression(k),
                            False if isinstance(v, basestring)
                                and v[0].upper() == 'D' else v)
                            for k, v in orderby]
            o = ' ORDER BY %s' % ', '.join('%s %s' % (self.quoteIdent(k),
                            'ASC' if v else 'DESC') for (k, _), v in orderby)
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
