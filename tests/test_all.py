import unittest

import test_utils
import test_header_diff
import test_table_diff
import test_format

if __name__=="__main__":
    suite = unittest.TestSuite()
    suite.addTest(test_utils.suite)
    suite.addTest(test_format.suite)
    suite.addTest(test_header_diff.suite)
    suite.addTest(test_table_diff.suite)

    unittest.TextTestRunner(verbosity=1).run(suite)

