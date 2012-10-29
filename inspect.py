#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Usage: inspect.py [--user=user] [--password=password] [--host=host] 
[--database=database]  

Arguments:
    user            the MySQL username
    host            the host name of the MySQL database
    database        name of the database
    password        password belonging to user
    
'''


"""
sqoopy: Generate sqoop custom import statements
Copyright (C) 2012  Diederik van Liere, Wikimedia Foundation

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""
import re
from docopt import docopt

from sqoopy import Db
from sqoopy import column_size

class Fields(object): 
    def __init__(self):
        self._coll = {}
    
    def add(self, field):
        self._coll[field.name] = field
        
    def get(self, name, **kwargs):
        try:
            return self._coll.get(name)
        except KeyError:
            field = Field(**kwargs)
            self.add(field)
            return field

class Field(object):
    def __init__(self, key, datatype, pk, size=0):
        self.canonical_key = self.get_canonical_key(key)
        self.mysql_datatype = datatype
        self.pk = pk
        self.mysql_size = size            
        self.hive_datatype = None
        self.tables = set()
    
    def __str__(self):
        return '%s <%s(%s)>' % (self.canonical_key, self.mysql_datatype, self.mysql_size)
    
    def get_canonical_key(self, key):
        try:
            return key.split('_')[1]
        except IndexError:
            return key

def make_C(parent):
    class C(parent):
        def add(self):
            print ''
    return C

def inspect_table(database, table):
    for data in database.data:
        data = data.split('\t')
        print data
        key = data[0]
        datatype = re.split(column_size, data[1])[0]
        datatype = datatype.lower()
            
        size = re.findall(column_size, data[1])
        if len(size) > 0:
            size = int(size[0][1:-1])
        else:
            size = 0
            
        pk = True if data[3] == 'PRI' or data[3] == 'MUL' else False
        fields = make_C(Field(key, datatype, pk, size))
        print fields
    return fields  
#        if self.verbose:
#            log.info('Table: %s, found column: %s (%s)' % (table, name, datatype))

        # column = Column(name, datatype, size, pk)
        # self.schema.setdefault(name, column)

def main(args):
    database = Db(args.get('--user'), args.get('--password'), args.get('--host'),
                args.get('--database'))
    if not args.get('--table'):
        database.get_tables()
    
    for table in database.tables:
        database.inspect(table)
        fields = inspect_table(database, table)
        

if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
