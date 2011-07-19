# -*- coding: utf-8 -*-
"""
    sqlalchemy_django_query
    ~~~~~~~~~~~~~~~~~~~~~~~

    A module that implements a more Django like interface for SQLAlchemy
    query objects.  It's still API compatible with the regular one but
    extends it with Djangoisms.

    Example queries::

        Post.query.filter_by(pub_date__year=2008)
        Post.query.exclude_by(id=42)
        User.query.filter_by(name__istartswith='e')
        Post.query.filter_by(blog__name__exact='something')
        Post.query.order_by('-blog__name')

    :copyright: 2011 by Armin Ronacher, Mike Bayer.
    license: BSD, see LICENSE for more details.
"""
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.util import _entity_descriptor
from sqlalchemy.util import to_list
from sqlalchemy.sql import operators, extract


class DjangoQueryMixin(object):
    """Can be mixed into any Query class of SQLAlchemy and extends it to
    implements more Django like behavior:

    -   `filter_by` supports implicit joining and subitem accessing with
        double underscores.
    -   `exclude_by` works like `filter_by` just that every expression is
        automatically negated.
    -   `order_by` supports ordering by field name with an optional `-`
        in front.
    """
    _underscore_operators = {
        'gt':           operators.gt,
        'lte':          operators.lt,
        'gte':          operators.ge,
        'le':           operators.le,
        'contains':     operators.contains_op,
        'in':           operators.in_op,
        'exact':        operators.eq,
        'iexact':       operators.ilike_op,
        'startswith':   operators.startswith_op,
        'istartswith':  lambda c, x: c.ilike(x.replace('%', '%%') + '%'),
        'iendswith':    lambda c, x: c.ilike('%' + x.replace('%', '%%')),
        'endswith':     operators.endswith_op,
        'isnull':       lambda c, x: x and c != None or c == None,
        'range':        operators.between_op,
        'year':         lambda c, x: extract('year', c) == x,
        'month':        lambda c, x: extract('month', c) == x,
        'day':          lambda c, x: extract('day', c) == x
    }

    def filter_by(self, **kwargs):
        return self._filter_or_exclude(False, kwargs)

    def exclude_by(self, **kwargs):
        return self._filter_or_exclude(True, kwargs)

    def order_by(self, *args):
        args = list(args)
        joins_needed = []
        for idx, arg in enumerate(args):
            q = self
            if not isinstance(arg, basestring):
                continue
            if arg[0] in '+-':
                desc = arg[0] == '-'
                arg = arg[1:]
            else:
                desc = False
            q = self
            column = None
            for token in arg.split('__'):
                column = _entity_descriptor(q._joinpoint_zero(), token)
                if column.impl.uses_objects:
                    q = q.join(column)
                    joins_needed.append(column)
                    column = None
            if column is None:
                raise ValueError('Tried to order by table, column expected')
            if desc:
                column = column.desc()
            args[idx] = column

        q = super(DjangoQueryMixin, self).order_by(*args)
        for join in joins_needed:
            q = q.join(join)
        return q

    def _filter_or_exclude(self, negate, kwargs):
        q = self
        negate_if = lambda expr: expr if not negate else ~expr
        column = None

        for arg, value in kwargs.iteritems():
            for token in arg.split('__'):
                if column is None:
                    column = _entity_descriptor(q._joinpoint_zero(), token)
                    if column.impl.uses_objects:
                        q = q.join(column)
                        column = None
                elif token in self._underscore_operators:
                    op = self._underscore_operators[token]
                    q = q.filter(negate_if(op(column, *to_list(value))))
                    column = None
                else:
                    raise ValueError('No idea what to do with %r' % token)
            if column is not None:
                q = q.filter(negate_if(column == value))
                column = None
            q = q.reset_joinpoint()
        return q


class DjangoQuery(DjangoQueryMixin, Query):
    pass
