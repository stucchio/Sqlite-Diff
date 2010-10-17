import unittest
import sqlite3

import sqlite3_diff.utils as utils

class TestUtilityFunctions(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")

    def test_sqlite_master_table_def(self):
        self.assertEquals(utils.table_names(self.db.cursor()),
                          set([u'bonds', u'stocks']))
        self.assertEquals(utils.sqlite_master_table_def(self.db.cursor(), "stocks"),
                          (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'))
        self.assertEquals(utils.table_definition(self.db.cursor(), "stocks"),
                          ((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), [(u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)')]))

    def test_table_columns(self):
        self.db.cursor().execute("create table options (date text, trans text, symbol text, qty real, price real, PRIMARY KEY (symbol, trans) );")
        self.assertEqual(utils.table_columns(self.db.cursor(), "options"),
                         [(0, u'date', u'text', 0, None, 0), (1, u'trans', u'text', 0, None, 1), (2, u'symbol', u'text', 0, None, 1), (3, u'qty', u'real', 0, None, 0), (4, u'price', u'real', 0, None, 0)])
        self.db.cursor().execute("create table futures (date text, trans text, symbol text PRIMARY KEY , qty real, price real );")
        self.assertEqual(utils.table_columns(self.db.cursor(), "futures"),
                         [(0, u'date', u'text', 0, None, 0), (1, u'trans', u'text', 0, None, 0), (2, u'symbol', u'text', 0, None, 1), (3, u'qty', u'real', 0, None, 0), (4, u'price', u'real', 0, None, 0)])

        #Now test finding primary keys
        self.assertEqual(utils.primary_key(self.db.cursor(), "options"), [(1, u'trans', u'text', 0, None, 1), (2, u'symbol', u'text', 0, None, 1)])
        self.assertEqual(utils.primary_key(self.db.cursor(), "futures"), [(2, u'symbol', u'text', 0, None, 1)])


suite = unittest.TestLoader().loadTestsFromTestCase(TestUtilityFunctions)

if __name__=="__main__":
    unittest.main()

