import unittest
import sqlite3

import sqlite3_diff.format as fmt

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
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2), '')
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
                         u"""Table(futures),4
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )

    def test_add_row_format(self):
        self.__create_stocks_bonds()
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2), '')
        self.db2.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.commit()
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
                         u"""Table(futures),4
> CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
> 0 rows
"""
                         )

    def test_null_results(self):
        self.__create_stocks_bonds()
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2), '')

    def test_format_index(self):
        self.__create_stocks_bonds()
        self.db1.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
u"""Table(stocks),4
< CREATE INDEX idx_stock_symbol ON stocks (symbol)
"""
                         )
        self.db2.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (date);")
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
u"""Table(stocks),4
> CREATE INDEX idx_stock_symbol ON stocks (date)
---
< CREATE INDEX idx_stock_symbol ON stocks (symbol)
"""
                         )

    def test_format_index2(self):
        self.__create_stocks_bonds()
        self.db2.cursor().execute("CREATE INDEX idx_stock_dates ON stocks (date);")
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
u"""Table(stocks),4
> CREATE INDEX idx_stock_dates ON stocks (date)
"""
                         )


    def test_different_table_def(self):
        self.__create_stocks_bonds()
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table futures (date text, symbol text, qty real, price real);")
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
                         u"""Table(futures),4
> CREATE TABLE futures (date text, symbol text, qty real, price real)
> 0 rows
---
< CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)
< 0 rows
"""
                         )
        self.db2.cursor().execute("create table options (date text, symbol text, qty real, price real);")
        self.assertEqual(fmt.format_table_header_diff(self.db1, self.db2),
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

suite = unittest.TestLoader().loadTestsFromTestCase(TestTableHeaderComparisonFormatting)

if __name__=="__main__":
    unittest.main()
