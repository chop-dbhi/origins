import re
from ..dal import recordtuple
from .. import logger
from . import base


class Client(base.Client):
    """Client specific for relational database backends that conform to
    the Python DB API.

    The `connect` method must set the `connection` property which is a
    connection to the database.
    """
    STRING_TYPES = set()
    NUMERIC_TYPES = set()
    TIME_TYPES = set()
    BOOL_TYPES = set()

    def disconnect(self):
        self.connection.close()

    def qn(self, name):
        return '"{}"'.format(name)

    def fetchall(self, *args, **kwargs):
        "Returns all rows."
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def fetchone(self, *args, **kwargs):
        "Returns the fist row"
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()

    def fetchvalue(self, *args, **kwargs):
        "Returns the first value from the first row."
        row = self.fetchone(*args, **kwargs)
        if row:
            return row[0]

    def _select_query(self, select, columns, table, orderby=''):
        return '{select} {columns} FROM {table} {orderby}'\
               .format(select=select, columns=columns, table=table,
                       orderby=orderby).strip()

    def _results_iter(self, names, query, unpack):
        # Not used below if unpack is true
        if unpack:
            record = None
        else:
            record = recordtuple(names)

        c = self.connection.cursor()
        c.execute(query)
        batch = c.fetchmany()

        while batch:
            for row in batch:
                if unpack:
                    yield row[0]
                else:
                    yield record(*row)

            batch = c.fetchmany()

    def _parse_dtype(self, dtype):
        match = re.match(r'^([a-z]+)', dtype, re.I)
        if match:
            return match.group()

    def is_string_type(self, dtype):
        "Returns true if the type is a string type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.STRING_TYPES

    def is_time_type(self, dtype):
        "Returns true if the type is a date or time type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.TIME_TYPES

    def is_numeric_type(self, dtype):
        "Returns true if the type is a numeric type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.NUMERIC_TYPES

    def is_bool_type(self, dtype):
        "Returns true if the type is a boolean type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.BOOL_TYPES

    def select(self, table_name, column_names, distinct=False,
               sort=None, unpack=False, iterator=False):

        # Handle sort direction for single column
        if len(column_names) == 1 and isinstance(sort, str):
            sort = [(column_names[0], sort)]

        # Handle single sort, e.g. ('foo', 'desc')
        elif sort and not isinstance(sort[0], (list, tuple)):
            sort = [sort]

        select = 'SELECT DISTINCT' if distinct else 'SELECT'
        if column_names:
            columns = ', '.join([self.qn(c) for c in column_names])
        else:
            columns = '*'
        orderby = 'ORDER BY ' + ', '.join([' '.join([self.qn(c), d])
                  for c, d in sort]) if sort else ''

        table = self.qn(table_name)
        query = self._select_query(select, columns, table, orderby)

        logger.debug(query)

        _iterator = self._results_iter(column_names, query, unpack)

        if iterator:
            return _iterator

        results = []
        for record in _iterator:
            results.append(record)
        return tuple(results)

    def count(self, table_name, column_names=None, distinct=False):
        subquery = False

        if column_names:
            columns = ', '.join([self.qn(c) for c in column_names])
            if len(column_names) > 1:
                subquery = True
        else:
            columns = '*'
            if distinct:
                subquery = True

        table = self.qn(table_name)

        if subquery:
            select = 'SELECT DISTINCT' if distinct else 'SELECT'
            query = '''
                SELECT COUNT(*) FROM ({subquery}) T
            '''.format(subquery=self._select_query(select, columns, table))
        else:
            distinct = 'DISTINCT ' if distinct else ''
            query = '''
                SELECT COUNT({distinct}{columns}) FROM {table}
            '''.format(distinct=distinct, columns=columns,
                       table=self.qn(table_name))

        logger.debug(query)
        return self.fetchvalue(query)

    def _aggregate(self, function, table_name, column_name):
        table = self.qn(table_name)
        column = self.qn(column_name)

        query = '''
            SELECT {function}({column})
            FROM {table}
        '''.format(function=function, column=column, table=table)

        logger.debug(query)

        return self.fetchvalue(query)

    def min(self, table_name, column_name, dtype):
        "Returns the minimum (lexicographic) value in the values."
        if (self.is_numeric_type(dtype) or self.is_time_type(dtype) or
                self.is_string_type(dtype)):
            return self._aggregate('MIN', table_name, column_name)

    def max(self, table_name, column_name, dtype):
        "Returns the maximum (lexicographic) value in the values."
        if (self.is_numeric_type(dtype) or self.is_time_type(dtype) or
                self.is_string_type(dtype)):
            return self._aggregate('MAX', table_name, column_name)

    def avg(self, table_name, column_name, dtype):
        "Returns the average of the values."
        if self.is_numeric_type(dtype) or self.is_time_type(dtype):
            return self._aggregate('AVG', table_name, column_name)

    def sum(self, table_name, column_name, dtype):
        "Returns the sum of values."
        if self.is_numeric_type(dtype) or self.is_time_type(dtype):
            return self._aggregate('SUM', table_name, column_name)

    def stddev(self, table_name, column_name, dtype):
        "Returns the standard deviation of values."
        if self.is_numeric_type(dtype) or self.is_time_type(dtype):
            return self._aggregate('STDDEV', table_name, column_name)

    def variance(self, table_name, column_name, dtype):
        "Returns the variance of values."
        if self.is_numeric_type(dtype) or self.is_time_type(dtype):
            return self._aggregate('VARIANCE', table_name, column_name)

    def longest(self, table_name, column_name, dtype):
        "Returns the longest string."
        if self.is_string_type(dtype):
            table = self.qn(table_name)
            column = self.qn(column_name)

            query = '''
                SELECT {column}, MAX(length({column}))
                FROM {table}
                GROUP BY {column}
                ORDER BY MAX(length({column})) DESC
            '''.format(column=column, table=table)

            return self.fetchvalue(query)

    def shortest(self, table_name, column_name, dtype):
        "Returns the shortest string."
        if self.is_string_type(dtype):
            table = self.qn(table_name)
            column = self.qn(column_name)

            query = '''
                SELECT {column}, MIN(length({column}))
                FROM {table}
                GROUP BY {column}
                ORDER BY MIN(length({column})) ASC
            '''.format(column=column, table=table)

            return self.fetchvalue(query)


class Database(base.Node):
    def sync(self):
        self.update(self.client.database())
        self._contains(self.client.tables(), Table)

    @property
    def tables(self):
        return self._containers('table')


class Table(base.Node):
    def sync(self):
        self._contains(self.client.columns(self['name']), Column)

    @property
    def columns(self):
        return self._containers('column')

    def count(self, names=None, distinct=False):
        """Returns a count of all records. If `distinct` is true, duplicate
        records will not be counted. If `names` is not falsy, perform a
        distinct count on only those data elements.
        """
        if not names:
            names = [c['name'] for c in self.columns]
        return self.client.count(self['name'], names, distinct=distinct)

    def select(self, names=None, distinct=False, sort=None, iterator=True):
        """Returns records from this table. `names` can be a list of element
        names to select a subset of fields. `sort` can be a list of sort order
        pairs of (name, direction), a single pair, or just the direction for
        single column selects.
        """
        if not names:
            names = [c['name'] for c in self.columns]
        return self.client.select(self['name'], names, distinct=distinct,
                                  sort=sort, iterator=iterator)


class Column(base.Node):
    def sync(self):
        self._foreign_keys_synced = False

    @property
    def foreign_keys(self):
        if not self._foreign_keys_synced:
            root = self.root
            table_name = self.parent['name']

            for attrs in self.client.foreign_keys(table_name, self['name']):
                # Get referenced node
                node = root.tables[attrs['table']].columns[attrs['column']]

                self.relate(node, 'RELATES', {
                    'name': attrs['name'],
                    'type': 'foreignkey',
                })

            self._foreign_keys_synced = True
        return self.rels(type='RELATES').filter('type', 'foreignkey').nodes()

    def count(self, distinct=True):
        """Returns the count of the values in this column. By default this
        is a distinct count, but this can be toggled using the flag.
        """
        return self.client.count(self.parent['name'], [self['name']],
                                 distinct=distinct)

    def select(self, distinct=True, sort=None, iterator=True, unpack=False):
        """Returns records for this column. The sort order can be specified by
        setting `sort` to 'desc' or 'asc'. For convenience, the `unpack`
        option can be set to true to return the value in the iterator rather
        than within a record.
        """
        return self.client.select(self.parent['name'], [self['name']],
                                  distinct=distinct, sort=sort,
                                  iterator=iterator, unpack=unpack)

    def min(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.min(table, column, dtype)

    def max(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.max(table, column, dtype)

    def sum(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.sum(table, column, dtype)

    def avg(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.avg(table, column, dtype)

    def stddev(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.stddev(table, column, dtype)

    def variance(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.variance(table, column, dtype)

    def longest(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.longest(table, column, dtype)

    def shortest(self):
        table, column, dtype = self.parent['name'], self['name'], self['type']
        return self.client.shortest(table, column, dtype)
