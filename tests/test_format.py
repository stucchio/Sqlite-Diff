import unittest
import sqlite3

import sqlite3_diff.format as fmt
import sqlite3_diff as diff

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

class TestTableDiffFormat(unittest.TestCase):
    def setUp(self):
        self.db1 = sqlite3.connect(":memory:")
        self.db2 = sqlite3.connect(":memory:")

        self.db1.cursor().execute("create table foo (id int PRIMARY KEY, symbol text);")
        self.db2.cursor().execute("create table foo (id int PRIMARY KEY, symbol text);")
        self.db1.commit()
        self.db2.commit()

        for i in range(10):
            self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(i) + ", '" + (str(i) * i) + "');");
            self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(i) + ", '" + (str(i) * i) + "');");

    def test_tables_identical(self):
        self.assertEqual(fmt.format_table_diff("foo", diff.diff_table("foo", self.db1, self.db2)), '')

    def test_table_has_extra_item(self):
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.assertEqual(fmt.format_table_diff("foo", diff.diff_table("foo", self.db1, self.db2)),
                         "> INSERT INTO foo VALUES (11, 'missing');\n")
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'missing');");
        self.assertEqual(fmt.format_table_diff("foo", diff.diff_table("foo", self.db1, self.db2)),
                         "< INSERT INTO foo VALUES (12, 'missing');\n")

    def test_table_has_diff_items(self):
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'AAA');");
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'BBB');");
        self.assertEqual(fmt.format_table_diff("foo", diff.diff_table("foo", self.db1, self.db2)),
                         """< INSERT INTO foo VALUES (11, 'AAA');
---
> INSERT INTO foo VALUES (11, 'BBB');
""")

    def test_table_has_gap(self):
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'AAA');");
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'AAA');");
        self.assertEqual(fmt.format_table_diff("foo", diff.diff_table("foo", self.db1, self.db2)),
                         "> INSERT INTO foo VALUES (11, 'missing');\n")



suite = unittest.TestLoader().loadTestsFromTestCase(TestTableHeaderComparisonFormatting)
suite = unittest.TestLoader().loadTestsFromTestCase(TestTableDiffFormat)

if __name__=="__main__":
    unittest.main()
