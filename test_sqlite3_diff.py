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

    def test_compare_same_table_columns(self):
        self.__create_stocks_bonds() #Both tables are the same
        self.assertFalse(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()))

    def test_compare_different_table_columns(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table stocks (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()), [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.table_column_diff(self.db1.cursor(), self.db2.cursor()), [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])

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
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2), '')
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2),
                         u"""db1,4
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )

    def test_add_row_format(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2), '')
        self.db2.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.commit()
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2),
                         u"""db2,4
> CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
> 0 rows
"""
                         )

    def test_null_results(self):
        self.__create_stocks_bonds()
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2), '')

    def test_different_table_def(self):
        self.__create_stocks_bonds()
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table futures (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2),
                         u"""db2,4
> CREATE TABLE futures (date text, symbol text, qty real, price real)
> 0 rows
---
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )
        self.db2.cursor().execute("create table options (date text, symbol text, qty real, price real);")
        self.assertEqual(sqlite3_diff.format_table_diff("db1", "db2", self.db1, self.db2),
                         u"""db2,5
> CREATE TABLE options (date text, symbol text, qty real, price real)
> 0 rows
db2,4
> CREATE TABLE futures (date text, symbol text, qty real, price real)
> 0 rows
---
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )

if __name__=="__main__":
    unittest.main()
