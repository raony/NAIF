import unittest
import db
import metrics
import analysis

def suite():
    return unittest.TestSuite([db.suite(),
                               metrics.suite(),
                               analysis.suite(),
                               ])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
