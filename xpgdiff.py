#!/usr/bin/env python3
"""
Compares the schemas of two PostgreSQL database schemas, printing a
migration script.  Alternatively prints DDL for a single PostgreSQL
database schema, but that is mainly for troubleshooting as
`pg_dump --schema-only` is the definitive way to generate DDL.

Usage: xpgdiff.py source-libpq-connstr [target_libpq-connstr]

TODOs:

- Column type length, precision
- More complete support for aggregate functions
- Add support for RULE
- Add support for indexes on views
- Add support for user defined types
- Where there is __eq__, consider implementing __hash__, or comment as to why
  it is not implemented
- Add docstr comments
- Consider typing
"""

import sys

import psycopg2

############################################################################
# CLASSES
############################################################################

class Check:
    """ A check constraint on a table """
    def __init__(self, oid, table, name, expression, definition):
        self.oid = oid
        self.table = table
        self.name = name
        self.expression = expression
        self.definition = definition

    def addstr(self):
        return f'ALTER TABLE {self.table.fullname} ADD {str(self)};'

    def dropstr(self):
        return f'ALTER TABLE {self.table.fullname} DROP CONSTRAINT {self.name};'

    def __eq__(self, other):
        if not isinstance(other, Check):
            raise TypeError('other')
        return self.name == other.name and self.expression == other.expression

    def __str__(self):
#        return f'CONSTRAINT {self.name} CHECK ({self.expression})'
        return f'CONSTRAINT {self.name} {self.definition}'

class Column:
    """ A column in a table """
    def __init__(self, table, colnum, name, _type, notnull, _default, sequence_name, ndims, typmod):
        self.table = table
        self.colnum = colnum
        self.name = name
        self.type = _type
        self.notnull = notnull
        self.default = _default
        self.sequence_name = sequence_name
        self.ndims = ndims
        self.typmod = typmod

    def addstr(self):
        return f'ALTER TABLE {self.table.fullname} ADD {str(self)};'

    def alterstr(self):
        return f'ALTER TABLE {self.table.fullname} ALTER COLUMN {str(self)};'

    def dropstr(self):
        return f'ALTER TABLE {self.table.fullname} DROP COLUMN {self.name};'

    def _typestr(self):
        if self.typmod == -1:
            length = ''
        else:
            # Reverse engineered; possibly fragile and incomplete
            if self.type in ('bpchar', 'varchar'):
                length = f'({self.typmod - 4})'
            elif self.type == 'numeric':
                length = f'({self.typmod >> 16}, {(self.typmod & 0x0ffff) >> 1})'
            elif self.type == 'interval':
                length = f'({self.typmod & 0x0ffff})'
            else:
                length = f'({self.typmod})'
        return f'{self.type}{length}{"[]" if self.ndims else ""}'

    def __eq__(self, other):
        if not isinstance(other, Column):
            raise TypeError('other')
        return self.name == other.name and self.type == other.type and self.notnull == other.notnull and self.default == other.default and self.ndims == other.ndims

    def __str__(self):
        if self.sequence_name and self.type in ('int4', 'int8', 'int2'):
            if self.type == 'int4':
                _type = 'serial'
            elif self.type == 'int8':
                _type = 'bigserial'
            elif self.type == 'int2':
                _type = 'smallserial'
            return f'{self.name} {_type} {"NOT " if self.notnull else ""}NULL{" -- DEFAULT " + self.default if self.default else ""}'

        return f'{self.name} {self._typestr()} {"NOT " if self.notnull else ""}NULL{" DEFAULT " + self.default if self.default else ""}'

class ForeignKey:
    """ A foreign key on a table """
    def __init__(self, oid, table, name, columns, reftable, refcolumns, matchtype, ondelete, onupdate, definition):
        self.oid = oid
        self.table = table
        self.name = name
        self.columns = columns
        self.reftable = reftable
        self.refcolumns = refcolumns
        self.matchtype = matchtype
        self.ondelete = ondelete
        self.onupdate = onupdate
        self.definition = definition

    def addstr(self):
        return f'ALTER TABLE {self.table.fullname} ADD {str(self)};'

    def dropstr(self):
        return f'ALTER TABLE {self.table.fullname} DROP CONSTRAINT {str(self.name)};'

    def __str__(self):
#        return f'CONSTRAINT {self.name} FOREIGN KEY ({column_name_list(self.columns)}) REFERENCES {self.reftable.name} ({column_name_list(self.refcolumns)}) {self.matchtype} ON DELETE {self.ondelete} ON UPDATE {self.onupdate}'
        return f'CONSTRAINT {self.name} {self.definition}'

class Function:
    """ A function/procedure in a schema """
    def __init__(self, oid, schema, owner, name, argtypes, rettype, lang, isagg, iswindow, acl, definition):
        self.oid = oid
        self.schema = schema
        self.owner = owner
        self.name = name
        self.argtypes = argtypes if argtypes != [None] else []
        self.rettype = rettype
        self.lang = lang
        self.isagg = isagg
        self.iswindow = iswindow
        self.acl = acl
        self.grants = grants_for_acl(self, acl)
        self.definition = definition
        self.fullname = f'{schema.name}.{name}({", ".join(self.argtypes)})'

    def dropstr(self):
        return f'DROP FUNCTION {self.fullname};'

    def ownerstr(self):
        return f'ALTER FUNCTION {self.fullname} OWNER TO {self.owner};'

    def __str__(self):
        s = f'{self.definition};'
        for grant in self.grants:
            s += f'\n{str(grant)}'
        s += f'\n{self.ownerstr()}'
        return s

class Grant:
    """ A grant on an object in a schema """
    def __init__(self, obj, role, privilegestr):
        self.obj = obj
        self.role = role
        self.privilegestr = privilegestr

    def grantstr(self):
        return self._grantrevokestr('GRANT')

    def revokestr(self):
        return self._grantrevokestr('REVOKE')

    def _grantrevokestr(self, which):
        return f'{which} {grant_privileges(self.privilegestr)} ON {"FUNCTION " if isinstance(self.obj, Function) else ""}{self.obj.fullname} {"TO" if which == "GRANT" else "FROM"} {self.role};'

    def __eq__(self, other):
        if not isinstance(other, Grant):
            raise TypeError('other')
        return type(self.obj) is type(other.obj) and self.obj.name == other.obj.name and self.role == other.role and self.privilegestr == other.privilegestr

    def __str__(self):
        return self._grantrevokestr('GRANT')

class Index:
    """ An index on a table (or a view?) """
    def __init__(self, oid, table, name, columns, isunique, isprimary, am, definition):
        self.oid = oid
        self.table = table
        self.name = name
        self.columns = columns
        self.isunique = isunique
        self.isprimary = isprimary
        self.am = am
        self.definition = definition
        self.fullname = f'{table.schema.name}.{name}'
    def addstr(self):
        return str(self)

    def dropstr(self):
        return f'DROP INDEX {self.fullname};'

    def __eq__(self, other):
        if not isinstance(other, Index):
            raise TypeError('other')
        if self.definition != other.definition:
            print(f'self: {self.definition} other: {other.definition}')
        return self.definition == other.definition

    def __str__(self):
#        return f'CREATE{" UNIQUE" if self.isunique else ""} INDEX {self.name} ON {self.table.name} USING {self.am} ({column_name_list(self.columns)});'
        return f'{self.definition};'

class PrimaryKey:
    """ A primary key constraint on a table """
    def __init__(self, oid, table, name, columns, definition):
        self.oid = oid
        self.table = table
        self.name = name
        self.columns = columns
        self.definition = definition

    def addstr(self):
        return f'ALTER TABLE {self.table.fullname} ADD {str(self)}'

    def dropstr(self):
        return f'ALTER TABLE {self.table.fullname} DROP CONSTRAINT {self.name}'

    def __eq__(self, other):
        if not isinstance(other, PrimaryKey):
            raise TypeError('other')
        return self.name == other.name and column_name_list(self.columns) == column_name_list(other.columns)

    def __str__(self):
        return f'CONSTRAINT {self.name} {self.definition}'

class Schema:
    """ A database namespace """
    def __init__(self, oid, name):
        self.oid = oid
        self.name = name
        self.tables = []
        self.table_lookup = {}
        self.views = []
        self.functions = []

    def addstr(self):
        return f'CREATE SCHEMA {self.name};'

    def dropstr(self):
        return f'DROP SCHEMA {self.name} CASCADE;'

    def add_function(self, function):
        self.functions.append(function)

    def add_table(self, table):
        self.tables.append(table)
        self.table_lookup[table.oid] = table

    def get_table(self, table_oid):
        return self.table_lookup[table_oid]

    def add_view(self, view):
        self.views.append(view)

class Table:
    """ A table in a schema """
    def __init__(self, oid, schema, owner, name, acl):
        self.oid = oid
        self.schema = schema
        self.owner = owner
        self.name = name
        self.acl = acl
        self.grants = grants_for_acl(self, acl)
        self.columns = []
        self.column_lookup = {}
        self.primary_key = None
        self.unique_keys = []
        self.unique_key_names = set()
        self.foreign_keys = []
        self.checks = []
        self.indexes = []
        self.triggers = []
        self.fullname = f'{schema.name}.{name}'

    def dropstr(self):
        return f'DROP TABLE {self.fullname};'

    def ownerstr(self):
        return f'ALTER TABLE {self.fullname} OWNER TO {self.owner};'

    def add_check(self, check):
        self.checks.append(check)

    def add_column(self, column):
        self.columns.append(column)
        self.column_lookup[column.colnum] = column

    def get_column(self, colnum):
        return self.column_lookup[colnum]

    def get_columns(self, colnums):
        return [self.get_column(colnum) for colnum in colnums]

    def add_foreign_key(self, foreign_key):
        self.foreign_keys.append(foreign_key)

    def add_index(self, index):
        self.indexes.append(index)

    def get_non_constraint_indexes(self):
        return [index for index in self.indexes if (not index.isprimary) and not(index.isunique and index.name in self.unique_key_names)]

    def get_non_constraint_triggers(self):
        return [trigger for trigger in self.triggers if not trigger.constraint]

    def set_primary_key(self, primary_key):
        self.primary_key = primary_key

    def add_trigger(self, trigger):
        self.triggers.append(trigger)

    def add_unique_key(self, unique_key):
        self.unique_keys.append(unique_key)
        self.unique_key_names.add(unique_key.name)

    def __str__(self):
        s = f'CREATE TABLE {self.fullname} (\n'
        for i, column in enumerate(self.columns):
            if i == 0:
                s += f'  {str(column)}\n'
            else:
                s += f', {str(column)}\n'
        if self.primary_key:
            s += f', {str(self.primary_key)}\n'
        for unique_key in self.unique_keys:
            s += f', {str(unique_key)}\n'
#        for foreign_key in self.foreign_keys:
#            s += f', {str(foreign_key)}\n'
        for check in self.checks:
            s += f', {str(check)}\n'
        s += ');'
        for index in self.get_non_constraint_indexes():
            s += f'\n{str(index)}'
        for trigger in self.get_non_constraint_triggers():
            s += f'\n{str(trigger)}'
        for grant in self.grants:
            s += f'\n{str(grant)}'
        s += f'\n{self.ownerstr()}'
        return s

class Trigger:
    """ A trigger on a table or view """
    def __init__(self, table_or_view, name, constraint, definition):
        self.table_or_view = table_or_view
        self.name = name
        self.constraint = constraint
        self.definition = definition

    def addstr(self):
        return f'ALTER {"TABLE" if isinstance(self.table_or_view, Table) else "VIEW"} {self.table_or_view.fullname} ADD {str(self)}'

    def dropstr(self):
        return f'ALTER {"TABLE" if isinstance(self.table_or_view, Table) else "VIEW"} {self.table_or_view.fullname} DROP TRIGGER {self.name}'

    def __eq__(self, other):
        if not isinstance(other, Trigger):
            raise TypeError('other')
        return self.name == other.name and self.constraint == other.constraint and self.definition == other.definition

    def __str__(self):
        return f'{self.definition};'

class UniqueKey:
    """ A unique constraint (aka alternate key) on a table """
    def __init__(self, oid, table, name, columns, definition):
        self.oid = oid
        self.table = table
        self.name = name
        self.columns = columns
        self.definition = definition

    def addstr(self):
        return f'ALTER TABLE {self.table.fullname} ADD {str(self)};'

    def dropstr(self):
        return f'ALTER TABLE {self.table.fullname} DROP CONSTRAINT {self.name};'

    def __eq__(self, other):
        if not isinstance(other, UniqueKey):
            raise TypeError('other')
        return self.name == other.name and column_name_list(self.columns) == column_name_list(other.columns)

    def __str__(self):
#        return f'CONSTRAINT {self.name} UNIQUE ({column_name_list(self.columns)})'
        return f'CONSTRAINT {self.name} {self.definition}'

class View:
    """ A view in a schema """
    def __init__(self, oid, schema, owner, name, acl, definition):
        self.oid = oid
        self.schema = schema
        self.owner = owner
        self.name = name
        self.acl = acl
        self.grants = grants_for_acl(self, acl)
        self.definition = definition
        self.triggers = []
        self.fullname = f'{schema.name}.{name}'

    def dropstr(self):
        return f'DROP VIEW {self.fullname};'

    def ownerstr(self):
        return f'ALTER VIEW {self.fullname} OWNER TO {self.owner};'

    def add_trigger(self, trigger):
        self.triggers.append(trigger)

    def __str__(self):
        s = f'CREATE VIEW {self.fullname} AS\n{self.definition}'
        for trigger in self.triggers:
            if trigger.constraint:
                continue
            s += f'\n{str(trigger)}'
        for grant in self.grants:
            s += f'\n{str(grant)}'
        s += f'\n{self.ownerstr()}'
        return s

############################################################################
# DATABASE-ORIENTED HELPER FUNCTIONS
############################################################################

def column_name_list(columns):
    """
    Gets a comma-separated list of column names.

    :param columns: The list of columns.
    :returns: A comma-separated list of column names.
    """
    if not columns:
        return ''
    return ', '.join([column.name for column in columns])

_FK_ACTIONS = {
    'a': 'NO ACTION',
    'r': 'RESTRICT',
    'c': 'CASCADE',
    'n': 'SET NULL',
    'd': 'SET DEFAULT'
}

def fk_action(action):
    """
    Gets the full name for a foreign key action abbreviation.

    :param action: Action abbreviation.
    :returns: Action full name.
    """
    return _FK_ACTIONS[action]

_FK_MATCHTYPE = {
    'f': 'FULL',
    'p': 'PARTIAL',
    's': 'SIMPLE'
}

def fk_matchtype(matchtype):
    """
    Gets the full name for a foreign key match type abbreviation.

    :param matchtype: The match type abbreviation.
    :returns: Match type full name
    """
    return _FK_MATCHTYPE[matchtype]

_GRANT_PRIVS = {
    'r': 'SELECT',
    'a': 'INSERT',
    'w': 'UPDATE',
    'd': 'DELETE',
    'D': 'TRUNCATE',
    'x': 'REFERENCES',
    't': 'TRIGGER',
    'X': 'EXECUTE'
}

def grant_privileges(perms):
    """
    Gets a comma-separated list of privilege full names for a string of abbreviations.

    :param perms: The string of privilege abbreviations.
    :returns: A comma-separated list of privilege full names.
    """
    return ', '.join([_GRANT_PRIVS[perm] for perm in perms])

def grants_for_acl(obj, acl):
    """
    Gets a list of grants (Grant instances) for an ACL string.

    :param obj: The object the ACL applies to.
    :param acl: The ACL as a string.
    :returns: The list of grants.
    """
    if not isinstance(acl, str) or len(acl) <= 2:
        return []
    if acl[0] != '{' or acl[-1] != '}':
        return []
    acls = acl[1:-2].split(',')
    grants = []
    for _acl in acls:
        role_privileges, owner = _acl.split('/')
        role, privilegestr = role_privileges.split('=')
        if role == owner:
            continue
        grants.append(Grant(obj, role if role else 'PUBLIC', privilegestr))
    return grants

############################################################################
# FUNCTIONS THAT READ DATABASE METADATA
############################################################################

def get_checks(cur, table):
    """
    Gets CHECK constraints for a table, adding them to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get CHECKs for.
    """
    cur.execute(f"""select oid, conname, consrc, pg_get_constraintdef(oid)
from pg_constraint
where conrelid = {table.oid}
and contype = 'c'
order by conname;""")
    for row in cur:
        table.add_check(Check(row[0], table, row[1], row[2], row[3]))

def get_columns(cur, table):
    """
    Gets columns for a table, adding them to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get columns for.
    """
    cur.execute(f"""select a.attnum, a.attname, coalesce(bt.typname, t.typname), a.attnotnull, d.adsrc, pg_get_serial_sequence('{table.name}', a.attname), a.attndims, a.atttypmod
from pg_attribute a
join pg_type t
on t.oid = a.atttypid
left outer join pg_type bt
on bt.oid = t.typelem
left outer join pg_attrdef d
on d.adrelid = a.attrelid
and d.adnum = a.attnum
where a.attrelid = '{table.oid}'
and a.attisdropped = FALSE
and a.attnum >= 1
order by a.attnum;""")
    for row in cur:
        table.add_column(Column(table, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))

def get_foreign_keys(cur, table):
    """
    Gets FK constraints for a table, adding them to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get FKs for.
    """
    cur.execute(f"""select oid, conname, conkey, confrelid, confkey, confmatchtype, confdeltype, confupdtype, pg_get_constraintdef(oid)
from pg_constraint
where conrelid = {table.oid}
and contype = 'f'
order by conname;""")
    for row in cur:
        reftable = table.schema.get_table(row[3])
        table.add_foreign_key(ForeignKey(row[0], table, row[1], table.get_columns(row[2]), reftable, reftable.get_columns(row[4]), fk_matchtype(row[5]), fk_action(row[6]), fk_action(row[7]), row[8]))

def get_functions(cur, schema):
    """
    Gets functions for a schema, adding them to the schema.

    :param cur: A cursor to execute commands on.
    :param schema: The schema to get functions for.
    """
    cur.execute(f"""select s.*, case when s.proisagg = FALSE then pg_get_functiondef(s.oid) else null end as definition
from (
    select p.oid, a.rolname, p.proname, array_agg(t.typname), p.prorettype, p.prolang, p.proisagg, p.proiswindow, p.proacl
    from pg_proc p
    join pg_authid a
    on p.proowner = a.oid
    left outer join pg_type t
    on t.oid = any(p.proargtypes)
    where p.pronamespace = {schema.oid}
    group by p.oid, a.rolname, p.proname, p.proargtypes, p.prorettype, p.prolang, p.proisagg, p.proiswindow, p.proacl
) s
order by s.proname;""")
    for row in cur:
        schema.add_function(Function(row[0], schema, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))

def get_indexes(cur, table):
    """
    Gets indexes for a table, adding them to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get indexes for.
    """
    cur.execute(f"""select c.oid, c.relname, i.indkey, i.indisunique, i.indisprimary, a.amname, pg_get_indexdef(i.indexrelid)
from pg_index i
join pg_class c
on c.oid = i.indexrelid
join pg_am a
on a.oid = c.relam
where i.indrelid = {table.oid}
order by c.relname;""")
    for row in cur:
        colnums = [int(col) for col in row[2].split(' ')]
        table.add_index(Index(row[0], table, row[1], table.get_columns(colnums), row[3], row[4], row[5], row[6]))

def get_primary_key(cur, table):
    """
    Gets the PK constraint for a table, adding it to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get the PK for.
    """
    cur.execute(f"""select oid, conname, conkey, pg_get_constraintdef(oid)
from pg_constraint
where conrelid = {table.oid}
and contype = 'p'
order by conname;""")
    primary_key = None
    for row in cur:
        if primary_key is not None:
            print(f'More than one primary key on {table.name}?', file=sys.stderr)
        else:
            primary_key = PrimaryKey(row[0], table, row[1], table.get_columns(row[2]), row[3])

    if primary_key:
        table.set_primary_key(primary_key)

def get_schemas(cur, schemas):
    """
    Gets schemas for a database, adding them to a list.

    :param cur: A cursor to execute commands on.
    :param schemas: The list schemas are added to.
    """
    cur.execute(f"""select oid, nspname
from pg_namespace
where nspname != 'information_schema'
and not nspname like 'pg_%'
order by nspname;""")
    for row in cur:
        schemas.append(Schema(row[0], row[1]))

def get_tables(cur, schema):
    """
    Gets tables for a schema, adding them to the schema.

    :param cur: A cursor to execute commands on.
    :param schema: The schema to get tables for.
    """
    cur.execute(f"""select c.oid, a.rolname, c.relname, c.relacl
from pg_class c
join pg_authid a
on a.oid = c.relowner
where c.relnamespace = {schema.oid}
and c.relkind = 'r'
order by c.relname;""")
    for row in cur:
        schema.add_table(Table(row[0], schema, row[1], row[2], row[3]))

def get_triggers(cur, table_or_view):
    """
    Gets triggers for a table or view, adding them to the table or view.

    :param cur: A cursor to execute commands on.
    :param table_or_view: The table or view to get triggers for.
    """
    cur.execute(f"""select tgname, tgconstraint, pg_get_triggerdef(oid)
from pg_trigger
where tgrelid = {table_or_view.oid}
order by tgname;""")
    for row in cur:
        table_or_view.add_trigger(Trigger(table_or_view, row[0], row[1], row[2]))

def get_unique_keys(cur, table):
    """
    Gets UNIQUE constraints for a table, adding them to the table.

    :param cur: A cursor to execute commands on.
    :param table: The table to get UNIQUEs for.
    """
    cur.execute(f"""select oid, conname, conkey, pg_get_constraintdef(oid)
from pg_constraint
where conrelid = {table.oid}
and contype = 'u'
order by conname;""")
    for row in cur:
        table.add_unique_key(UniqueKey(row[0], table, row[1], table.get_columns(row[2]), row[3]))

def get_views(cur, schema):
    """
    Gets views for a schema, adding them to the schema.

    :param cur: A cursor to execute commands on.
    :param schema: The schema to get views for.
    """
    cur.execute(f"""select c.oid, a.rolname, c.relname, c.relacl, pg_get_viewdef(c.oid)
from pg_class c
join pg_authid a
on c.relowner = a.oid
where c.relnamespace = {schema.oid}
and c.relkind = 'v'
order by c.relname;""")
    for row in cur:
        schema.add_view(View(row[0], schema, row[1], row[2], row[3], row[4]))

def get_schema_objects(libpq_connstr):
    """
    Gets all objects in all schemas.

    :param libpq_connstr: A libpq connection string to a database.
    :returns: A list of schemas.
    """
    conn = psycopg2.connect(libpq_connstr)
    try:
        cur = conn.cursor()
        try:
            schemas = []
            get_schemas(cur, schemas)
            for schema in schemas:
                get_tables(cur, schema)
                for table in schema.tables:
                    get_columns(cur, table)
                    get_primary_key(cur, table)
                    get_unique_keys(cur, table)
                    get_checks(cur, table)
                    get_indexes(cur, table)
                    get_triggers(cur, table)
                for table in schema.tables:
                    get_foreign_keys(cur, table)
                get_views(cur, schema)
                for view in schema.views:
                    get_triggers(cur, view)
                get_functions(cur, schema)
            return schemas
        finally:
            cur.close()
    finally:
        conn.close()

############################################################################
# FUNCTIONS FOR PRINTING MIGRATION DDL
############################################################################

def next_or_none(seq):
    """
    Gets an iterator-like function for sequence.  Instead of raising a
    StopIteration exception, the function returns None when the sequence
    is exhausted.

    :param seq: The sequence to iterate over.
    :returns: The iterator-like function.
    """
    def _next():
        try:
            return next(_iter)
        except StopIteration:
            return None

    _iter = iter(seq)
    return _next

def print_column_migration_ddl(source_table, source_column, target_column):
    """
    Prints the migration DDL for a column.

    N.B. This does not check that the DDL will work on data in the column.

    :param source_table: The table in the source schema.
    :param source_column: The column in the source schema.
    :param target_column: The column in the target schema.
    """
    if source_column != target_column:
        print(target_column.alterstr())

def print_grant_migration_ddl(source_object, source_grant, target_grant):
    """
    Prints the migration DDL for a single grant on an object.

    :param source_object: The object in the source schema.
    :param source_grant: The grant on the object in the source schema.
    :param target_grant: The grant on the object in the target schema.
    """
    revokes = set(source_grant.privilegestr) - set(target_grant.privilegestr)
    grants = set(target_grant.privilegestr) - set(source_grant.privilegestr)

    if revokes:
        print(Grant(source_object, source_grant.role, "".join(revokes)).revokestr())

    if grants:
        print(Grant(source_object, source_grant.role, "".join(grants)).grantstr())

def print_grants_migration_ddl(source_object, target_object):
    """
    Prints the migration DDL for grants on an object.

    :param source_object: The object in the source schema.
    :param target_object: The object in the target schema.
    """
    next_source_grant = next_or_none(sorted(source_object.grants, key=lambda g: g.role))
    next_target_grant = next_or_none(sorted(target_object.grants, key=lambda g: g.role))

    source_grant = next_source_grant()
    target_grant = next_target_grant()

    while source_grant or target_grant:
        if not target_grant:
            print(f'{source_grant.revokestr()}')
            source_grant = next_source_grant()
            continue

        if not source_grant:
            print(f'{str(target_grant)}')
            target_grant = next_target_grant()
            continue

        if source_grant.role < target_grant.role:
            print(f'{source_grant.revokestr()}')
            source_grant = next_source_grant()
            continue

        if source_grant.role > target_grant.role:
            print(f'{str(target_grant)}')
            target_grant = next_target_grant()
            continue

        print_grant_migration_ddl(source_object, source_grant, target_grant)
        source_grant = next_source_grant()
        target_grant = next_target_grant()

# def print_unique_key_migration_ddl(source_unique_key, target_unique_key):
#     if source_unique_key != target_unique_key:
#         print(source_unique_key.dropstr())
#         print(target_unique_key.addstr())

# def print_check_migration_ddl(source_check, target_check):
#     if source_check != target_check:
#         print(source_check.dropstr())
#         print(target_check.addstr())

# def print_trigger_migration_ddl(source_trigger, target_trigger):
#     if source_trigger != target_trigger:
#         print(source_trigger.dropstr())
#         print(target_trigger.addstr())

# def print_index_migration_ddl(source_index, target_index):
#     if source_index != target_index:
#         print(source_index.dropstr())
#         print(target_index.addstr())

def print_dropadd_migration_ddl(source_objs, target_objs):
    """
    Prints migration DDL for objects that are always drop or add.
    The class for the objects must have a name field; addstr and dropstr
    methods; and a meaningful implementation of __eq__.

    The passed sequences must already be sorted by name.

    :param source_objs: The objects in the source schema.
    :param target_objs: The objects in the target schema.
    """

    next_source_obj = next_or_none(source_objs)
    next_target_obj = next_or_none(target_objs)

    source_obj = next_source_obj()
    target_obj = next_target_obj()

    while source_obj or target_obj:
        if not target_obj:
            print(source_obj.dropstr())
            source_obj = next_source_obj()
            continue

        if not source_obj:
            print(target_obj.addstr())
            target_obj = next_target_obj()
            continue

        if source_obj.name < target_obj.name:
            print(source_obj.dropstr())
            source_obj = next_source_obj()
            continue

        if source_obj.name > target_obj.name:
            print(target_obj.addstr())
            target_obj = next_target_obj()
            continue

        if source_obj != target_obj:
            print('Yes')
            print(source_obj.dropstr())
            print(target_obj.addstr())

        source_obj = next_source_obj()
        target_obj = next_target_obj()

def print_table_migration_ddl(source_table, target_table):
    """
    Prints DDL to migrate a table in one schema to the structure in
    another schema, including columns, constraints, indexes, triggers
    and permissions.

    N.B. The migration does not enforce identical column ordering.

    :param source_table: The source table.
    :param target_table: The target table.
    """
    next_source_column = next_or_none(sorted(source_table.columns, key=lambda c: c.name))
    next_target_column = next_or_none(sorted(target_table.columns, key=lambda c: c.name))

    source_column = next_source_column()
    target_column = next_target_column()

    while source_column or target_column:
        if not target_column:
            print(source_column.dropstr())
            source_column = next_source_column()
            continue

        if not source_column:
            print(target_column.addstr())
            target_column = next_target_column()
            continue

        if source_column.name < target_column.name:
            print(source_column.dropstr())
            source_column = next_source_column()
            continue

        if source_column.name > target_column.name:
            print(target_column.addstr())
            target_column = next_target_column()
            continue

        print_column_migration_ddl(source_table, source_column, target_column)
        source_column = next_source_column()
        target_column = next_target_column()

    if source_table.primary_key is None and target_table.primary_key is not None:
        print(f'{str(target_table.primary_key)};')
    elif source_table.primary_key is not None and target_table.primary_key is None:
        print(source_table.primary_key.dropstr())
    elif source_table.primary_key is not None and target_table.primary_key is not None:
        if source_table.primary_key != target_table.primary_key:
            print(source_table.primary_key.dropstr())
            print(target_table.primary_key.addstr())

    print_dropadd_migration_ddl(source_table.unique_keys, target_table.unique_keys)
    print_dropadd_migration_ddl(source_table.checks, target_table.checks)
    print_dropadd_migration_ddl(source_table.get_non_constraint_indexes(), target_table.get_non_constraint_indexes())
    print_dropadd_migration_ddl(source_table.get_non_constraint_triggers(), target_table.get_non_constraint_triggers())

    print_grants_migration_ddl(source_table, target_table)
    if (source_table.owner != target_table.owner):
        print(target_table.ownerstr())

def print_tables_migration_ddl(source_schema, target_schema):
    """
    Prints DDL to migrate the tables in two schemas.

    :param source_schema: The source schema.
    :param target_schema: The target schema.
    """
    print('--')
    print('-- TABLES')
    print('--')
    next_source_table = next_or_none(source_schema.tables)
    next_target_table = next_or_none(target_schema.tables)

    source_table = next_source_table()
    target_table = next_target_table()

    while source_table or target_table:
        if not target_table:
            print(source_table.dropstr())
            source_table = next_source_table()
            continue

        if not source_table:
            print(f'{str(target_table)}')
            target_table = next_target_table()
            continue

        if source_table.name < target_table.name:
            print(source_table.dropstr())
            source_table = next_source_table()
            continue

        if source_table.name > target_table.name:
            print(f'{str(target_table)}')
            target_table = next_target_table()
            continue

        print_table_migration_ddl(source_table, target_table)
        source_table = next_source_table()
        target_table = next_target_table()

def print_views_migration_ddl(source_schema, target_schema):
    """
    Prints DDL to migrate the views in two schemas.

    :param source_schema: The source schema.
    :param target_schema: The target schema.
    """
    print('--')
    print('-- VIEWS')
    print('--')
    next_source_view = next_or_none(source_schema.views)
    next_target_view = next_or_none(target_schema.views)

    source_view = next_source_view()
    target_view = next_target_view()

    while source_view or target_view:
        if not target_view:
            print(source_view.dropstr())
            source_view = next_source_view()
            continue

        if not source_view:
            print(f'{str(target_view)}')
            target_view = next_target_view()
            continue

        if source_view.name < target_view.name:
            print(source_view.dropstr())
            source_view = next_source_view()
            continue

        if source_view.name > target_view.name:
            print(f'{str(target_view)}')
            target_view = next_target_view()
            continue

        if source_view.definition != target_view.definition:
            print(source_view.dropstr())
            print(f'{str(target_view)}')
        else:
            print_grants_migration_ddl(source_view, target_view)
            if (source_view.owner != target_view.owner):
                print(target_view.ownerstr())

        source_view = next_source_view()
        target_view = next_target_view()

def print_functions_migration_ddl(source_schema, target_schema):
    """
    Prints DDL to migrate the functions in two schemas.

    :param source_schema: The source schema.
    :param target_schema: The target schema.
    """
    print('--')
    print('-- FUNCTIONS')
    print('--')
    next_source_function = next_or_none(sorted(source_schema.functions, key=lambda f: f.fullname))
    next_target_function = next_or_none(sorted(target_schema.functions, key=lambda f: f.fullname))

    source_function = next_source_function()
    target_function = next_target_function()

    while source_function or target_function:
        if not target_function:
            print(source_function.dropstr())
            source_function = next_source_function()
            continue

        if not source_function:
            print(f'{str(target_function)}')
            target_function = next_target_function()
            continue

        if source_function.name < target_function.name:
            print(source_function.dropstr())
            source_function = next_source_function()
            continue

        if source_function.name > target_function.name:
            print(f'{str(target_function)}')
            target_function = next_target_function()
            continue

        if source_function.definition != target_function.definition:
            print(source_function.dropstr())
            print(f'{str(target_function)}')
        else:
            print_grants_migration_ddl(source_function, target_function)
            if source_function.owner != target_function.owner:
                print(target_function.ownerstr())

        source_function = next_source_function()
        target_function = next_target_function()

def print_schema_migration_ddl(source_schema, target_schema):
    """
    Prints the migration DDL for two schemas.  The DDL will migrate
    a database with the source schema to one with the target schema.

    :param source_schema: The source schema.
    :param target_schema: The target schema.
    """
    print_schema_banner(source_schema)
    print_tables_migration_ddl(source_schema, target_schema)
    print()
    print_views_migration_ddl(source_schema, target_schema)
    print()
    print_functions_migration_ddl(source_schema, target_schema)

def print_schemas_migration_ddl(source_schemas, target_schemas):
    """
    Prints the migration DDL for two lists of schemas.  The DDL will migrate
    a database with the source schemas to one with the target schemas.

    :param source_schemas: The source schemas.
    :param target_schemas: The target schemas.
    """
    next_source_schema = next_or_none(sorted(source_schemas, key=lambda f: f.name))
    next_target_schema = next_or_none(sorted(target_schemas, key=lambda f: f.name))

    source_schema = next_source_schema()
    target_schema = next_target_schema()

    while source_schema or target_schema:
        if not target_schema:
            print_schema_banner(source_schema)
            print(source_schema.dropstr())
            source_schema = next_source_schema()
            continue

        if not source_schema:
            print_schema_banner(target_schema)
            print(target_schema.addstr())
            print_schema_ddl(target_schema)
            target_schema = next_target_schema()
            continue

        if source_schema.name < target_schema.name:
            print_schema_banner(source_schema)
            print(source_schema.dropstr())
            source_schema = next_source_schema()
            continue

        if source_schema.name > target_schema.name:
            print_schema_banner(target_schema)
            print(target_schema.addstr())
            print_schema_ddl(target_schema)
            target_schema = next_target_schema()
            continue

        print_schema_migration_ddl(source_schema, target_schema)

        source_schema = next_source_schema()
        target_schema = next_target_schema()

############################################################################
# FUNCTIONS FOR PRINTING SCHEMA DDL
############################################################################

def print_schema_ddl(schema):
    """
    Prints the DDL to create all objects in a schema.

    :param schema: The schema
    """
    for table in schema.tables:
        print(str(table))
        print()

    for table in schema.tables:
        for foreign_key in table.foreign_keys:
            print(foreign_key.addstr())
    print()

    for view in schema.views:
        print(str(view))
        print()

    for function in schema.functions:
        print(str(function))
        print()

def print_schema_banner(schema):
    """
    Prints a banner to call out the schema name.

    :param schema: The schema
    """
    print('-- *************************************')
    print('-- * SCHEMA: ' + schema.name)
    print('-- *************************************')

############################################################################
# FUNCTIONS FOR COMMAND LINE UTILITY
############################################################################

def _main(source_libpq_connstr='', target_libpq_connstr=None):
    source_schemas = get_schema_objects(source_libpq_connstr)
    if target_libpq_connstr:
        target_schemas = get_schema_objects(target_libpq_connstr)
        print_schemas_migration_ddl(source_schemas, target_schemas)
    else:
        for source_schema in source_schemas:
            print_schema_banner(source_schema)
            print_schema_ddl(source_schema)

if __name__ == '__main__':
    _main(*sys.argv[1:])
