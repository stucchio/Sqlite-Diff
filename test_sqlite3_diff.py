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


if __name__=="__main__":
    unittest.main()
