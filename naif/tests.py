import unittest
import db
import metrics
import analysis
import sys

def suite():
    return unittest.TestSuite([db.suite(),
                               metrics.suite(),
                               analysis.suite(),
                               ])

if __name__ == '__main__':
    if not unittest.TextTestRunner(verbosity=2).run(suite()).wasSuccessful():
        sys.exit(1)
    
