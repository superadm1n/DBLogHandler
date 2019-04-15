'''
Copyright (c) 2019 Kyle Kowalczyk

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import sqlalchemy
from logging import Handler
from datetime import datetime

class MissingColumn(Exception):
    pass

class DBHandler(Handler):

    '''This handler expects the database to either not have the specified logging table
    exist or if it exists it expects it to be created by itself '''

    def __init__(self, db_uri, table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_engine = sqlalchemy.create_engine(db_uri)
        self.metadata = sqlalchemy.MetaData(self.db_engine)
        self.logs_schema = sqlalchemy.Table(table, self.metadata,
                           sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, nullable=False),
                           sqlalchemy.Column('timestamp', sqlalchemy.DateTime, nullable=False, default=datetime.now()),
                           sqlalchemy.Column('log_message', sqlalchemy.String(500), nullable=False)
                           )

        self.db_uri = db_uri
        self.table = table
        self._on_init()

    def _on_init(self):
        if self._validate_db_table() is False:
            self._create_logs_table()
        else:
            self._validate_table_schema()

    def _validate_table_schema(self):
        '''Validates that the logs table has the proper columns in it.
        :raises MissingColumn: if a column doesnt exist this exception will be raised
        :return: 
        '''

        required_columns = ['id', 'timestamp', 'log_message']

        columns = [x for x in self.metadata.sorted_tables if x.name == self.table][0].columns
        for col in columns:
            if col.name not in required_columns:
                raise MissingColumn('Database table {} is missing column {}'.format(self.table, col))

    def _create_logs_table(self):
        '''Creates the logs database table'''
        self.metadata.create_all()

    def _validate_db_table(self):
        '''Returns true if the logs table exists, false if it does not'''
        if self.db_engine.dialect.has_table(self.db_engine, table_name=self.table):
            return True
        else:
            return False

    def emit(self, record):
        t = self.logs_schema.insert()
        t.execute(log_message=record.msg)
