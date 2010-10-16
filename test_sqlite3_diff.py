import unittest
import sqlite3_diff
import sqlite3

class TestTableHeaderComparison(unittest.TestCase):
    def setUp(self):
        self.db1 = sqlite3.connect(":memory:")
        self.db2 = sqlite3.connect(":memory:")

    def __create_stocks_bonds(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.db2.commit()

    def test_table_diff(self):
        self.__create_stocks_bonds()
        #Test whether we get correct diff for different indices
        self.assertFalse(sqlite3_diff.table_diff(self.db1, self.db2, "stocks"))
        self.db1.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")
        self.assertEquals(sqlite3_diff.table_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)'), None)]))
        self.db2.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (trans);")
        self.assertEquals(sqlite3_diff.table_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)'), (u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (trans)'))]))

    def test_table_diff_trigger(self):
        self.__create_stocks_bonds()
        #Test whether we get correct diff for different indices
        self.assertFalse(sqlite3_diff.table_diff(self.db1, self.db2, "stocks"))
        trigger_str = u"""CREATE TRIGGER insert_stock_time AFTER INSERT ON stocks
        BEGIN
        UPDATE stocks SET date = DATETIME('NOW')
        WHERE rowid = new.rowid;
        END"""
        self.db1.cursor().execute(trigger_str)
        self.assertEquals(sqlite3_diff.table_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'trigger', u'insert_stock_time', u'stocks', 0, trigger_str), None)]) )

    def test_different_table_definition(self):
        #Now check if we find a different table definition
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table futures (date text, symbol text, qty real, price real);")
        self.assertEquals(sqlite3_diff.table_diff(self.db1, self.db2, "futures"),
                          (( (u'table', u'futures', u'futures', 2, u'CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)'), (u'table', u'futures', u'futures', 2, u'CREATE TABLE futures (date text, symbol text, qty real, price real)') ),
                           []))

    def test_table_columns(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real, PRIMARY KEY (symbol, trans) );")
        self.assertEqual(sqlite3_diff.table_columns(self.db1.cursor(), "stocks"),
                         [(0, u'date', u'text', 0, None, 0), (1, u'trans', u'text', 0, None, 1), (2, u'symbol', u'text', 0, None, 1), (3, u'qty', u'real', 0, None, 0), (4, u'price', u'real', 0, None, 0)])
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text PRIMARY KEY , qty real, price real );")
        self.assertEqual(sqlite3_diff.table_columns(self.db1.cursor(), "bonds"),
                         [(0, u'date', u'text', 0, None, 0), (1, u'trans', u'text', 0, None, 0), (2, u'symbol', u'text', 0, None, 1), (3, u'qty', u'real', 0, None, 0), (4, u'price', u'real', 0, None, 0)])

        #Now test finding primary keys
        self.assertEqual(sqlite3_diff.primary_key(self.db1.cursor(), "stocks"), [(1, u'trans', u'text', 0, None, 1), (2, u'symbol', u'text', 0, None, 1)])
        self.assertEqual(sqlite3_diff.primary_key(self.db1.cursor(), "bonds"), [(2, u'symbol', u'text', 0, None, 1)])

    def test_compare_same_sqlite_master_table_def(self):
        self.__create_stocks_bonds() #Both tables are the same
        self.assertFalse(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()))

    def test_compare_different_sqlite_master_table_def(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table stocks (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()),
                         [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'),
                           (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()), [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])

    def test_shared_table_names(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.shared_tables(self.db1.cursor(), self.db2.cursor()), set([u'stocks', u'bonds']))

    def test_compare_same_tables(self):
        self.__create_stocks_bonds()
        self.assertFalse(sqlite3_diff.table_name_diff(self.db1.cursor(), self.db2.cursor()))
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.assertTrue(sqlite3_diff.table_name_diff(self.db1.cursor(), self.db2.cursor()))
        result = sqlite3_diff.table_name_diff(self.db1.cursor(), self.db2.cursor())
        self.assertEqual(sqlite3_diff.table_name_diff(self.db1.cursor(), self.db2.cursor()), ([u'futures'], []) )
        self.db2.cursor().execute("create table options (date text, trans text, symbol text, qty real, price real);")
        self.db2.commit()
        self.assertEqual(sqlite3_diff.table_name_diff(self.db1.cursor(), self.db2.cursor()), ([u'futures'], [u'options']) )

class TestUtilityFunctions(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")

    def test_sqlite_master_table_def(self):
        self.assertEquals(sqlite3_diff.table_names(self.db.cursor()),
                          set([u'bonds', u'stocks']))
        self.assertEquals(sqlite3_diff.sqlite_master_table_def(self.db.cursor(), "stocks"),
                          (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'))
        self.assertEquals(sqlite3_diff.table_definition(self.db.cursor(), "stocks"),
                          ((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), [(u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)')]))


class TestTableHeaderComparisonFormatting(unittest.TestCase):
    def setUp(self):
        self.db1 = sqlite3.connect(":memory:")
        self.db2 = sqlite3.connect(":memory:")

    def __create_stocks_bonds(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.db2.commit()

    def test_remove_row_format(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2), '')
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
                         u"""Table(futures),4
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )

    def test_add_row_format(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2), '')
        self.db2.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.commit()
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
                         u"""Table(futures),4
> CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
> 0 rows
"""
                         )

    def test_null_results(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2), '')

    def test_format_index(self):
        self.__create_stocks_bonds()
        self.db1.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
u"""Table(stocks),4
< CREATE INDEX idx_stock_symbol ON stocks (symbol)
"""
                         )
        self.db2.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (date);")
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
u"""Table(stocks),4
> CREATE INDEX idx_stock_symbol ON stocks (date)
---
< CREATE INDEX idx_stock_symbol ON stocks (symbol)
"""
                         )

    def test_format_index2(self):
        self.__create_stocks_bonds()
        self.db2.cursor().execute("CREATE INDEX idx_stock_dates ON stocks (date);")
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
u"""Table(stocks),4
> CREATE INDEX idx_stock_dates ON stocks (date)
"""
                         )


    def test_different_table_def(self):
        self.__create_stocks_bonds()
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table futures (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
                         u"""Table(futures),4
> CREATE TABLE futures (date text, symbol text, qty real, price real)
> 0 rows
---
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )
        self.db2.cursor().execute("create table options (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.format_table_diff(self.db1, self.db2),
                         u"""Table(options),5
> CREATE TABLE options (date text, symbol text, qty real, price real)
> 0 rows
Table(futures),4
> CREATE TABLE futures (date text, symbol text, qty real, price real)
> 0 rows
---
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )

if __name__=="__main__":
    unittest.main()
