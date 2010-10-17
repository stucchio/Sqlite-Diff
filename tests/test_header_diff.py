import unittest
import sqlite3_diff as diff
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

    def test_table_header_diff(self):
        self.__create_stocks_bonds()
        #Test whether we get correct diff for different indices
        self.assertFalse(diff.table_header_diff(self.db1, self.db2, "stocks"))
        self.db1.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (symbol);")
        self.assertEquals(diff.table_header_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)'), None)]))
        self.db2.cursor().execute("CREATE INDEX idx_stock_symbol ON stocks (trans);")
        self.assertEquals(diff.table_header_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (symbol)'), (u'index', u'idx_stock_symbol', u'stocks', 4, u'CREATE INDEX idx_stock_symbol ON stocks (trans)'))]))

    def test_table_header_diff_trigger(self):
        self.__create_stocks_bonds()
        #Test whether we get correct diff for different indices
        self.assertFalse(diff.table_header_diff(self.db1, self.db2, "stocks"))
        trigger_str = u"""CREATE TRIGGER insert_stock_time AFTER INSERT ON stocks
        BEGIN
        UPDATE stocks SET date = DATETIME('NOW')
        WHERE rowid = new.rowid;
        END"""
        self.db1.cursor().execute(trigger_str)
        self.assertEquals(diff.table_header_diff(self.db1, self.db2, "stocks"),
                          (False, [((u'trigger', u'insert_stock_time', u'stocks', 0, trigger_str), None)]) )

    def test_different_table_definition(self):
        #Now check if we find a different table definition
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table futures (date text, symbol text, qty real, price real);")
        self.assertEquals(diff.table_header_diff(self.db1, self.db2, "futures"),
                          (( (u'table', u'futures', u'futures', 2, u'CREATE TABLE futures (date text, trans text, symbol text, qty real, price real)'), (u'table', u'futures', u'futures', 2, u'CREATE TABLE futures (date text, symbol text, qty real, price real)') ),
                           []))

    def test_compare_same_sqlite_master_table_def(self):
        self.__create_stocks_bonds() #Both tables are the same
        self.assertFalse(diff.table_column_diff(self.db1.cursor(), self.db2.cursor()))

    def test_compare_different_sqlite_master_table_def(self):
        self.db1.cursor().execute("create table stocks (date text, trans text, symbol text, qty real, price real);")
        self.db2.cursor().execute("create table stocks (date text, symbol text, qty real, price real);")
        self.assertEqual(diff.table_column_diff(self.db1.cursor(), self.db2.cursor()),
                         [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'),
                           (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])
        self.db1.cursor().execute("create table bonds (date text, trans text, symbol text, qty real, price real);")
        self.assertEqual(diff.table_column_diff(self.db1.cursor(), self.db2.cursor()), [((u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)'), (u'table', u'stocks', u'stocks', 2, u'CREATE TABLE stocks (date text, symbol text, qty real, price real)'))])

    def test_shared_table_names(self):
        self.__create_stocks_bonds()
        self.assertEqual(diff.shared_tables(self.db1.cursor(), self.db2.cursor()), set([u'stocks', u'bonds']))

    def test_compare_same_tables(self):
        self.__create_stocks_bonds()
        self.assertFalse(diff.table_name_diff(self.db1.cursor(), self.db2.cursor()))
        self.db1.cursor().execute("create table futures (date text, trans text, symbol text, qty real, price real);")
        self.db1.commit()
        self.assertTrue(diff.table_name_diff(self.db1.cursor(), self.db2.cursor()))
        result = diff.table_name_diff(self.db1.cursor(), self.db2.cursor())
        self.assertEqual(diff.table_name_diff(self.db1.cursor(), self.db2.cursor()), ([u'futures'], []) )
        self.db2.cursor().execute("create table options (date text, trans text, symbol text, qty real, price real);")
        self.db2.commit()
        self.assertEqual(diff.table_name_diff(self.db1.cursor(), self.db2.cursor()), ([u'futures'], [u'options']) )


suite = unittest.TestLoader().loadTestsFromTestCase(TestTableHeaderComparison)

if __name__=="__main__":
    unittest.main()
