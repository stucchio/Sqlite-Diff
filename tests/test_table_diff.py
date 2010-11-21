import unittest
import sqlite3_diff as diff
import sqlite3

class TestTableDiff(unittest.TestCase):
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
        self.assertFalse(diff.diff_table("foo", self.db1, self.db2))

    def test_table_has_extra_item(self):
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.assertEqual(diff.diff_table("foo", self.db1, self.db2), [ (None, (11, u"missing") )] )
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'missing');");
        self.assertEqual(diff.diff_table("foo", self.db1, self.db2), [ ((12, u"missing"), None)] )

    def test_table_has_diff_items(self):
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'AAA');");
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'BBB');");
        self.assertEqual(diff.diff_table("foo", self.db1, self.db2), [ ((11, u"AAA"), (11, u"BBB"))] )

    def test_table_has_gap(self):
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(11) + ", 'missing');");
        self.db1.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'AAA');");
        self.db2.cursor().execute("INSERT INTO foo VALUES (" + str(12) + ", 'AAA');");
        self.assertEqual(diff.diff_table("foo", self.db1, self.db2), [ (None, (11, u"missing"))] )

suite = unittest.TestLoader().loadTestsFromTestCase(TestTableDiff)

if __name__=="__main__":
    unittest.main()
