#!/usr/bin/env python

import sys
import glob
import time
import os, os.path
import doctest
import unittest
try:
    import coverage
except ImportError:
    print "No 'coverage' module found. Try:"
    print "     sudo apt-get install python-coverage"
    sys.exit(1)

import logging
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)



VERBOSE=False

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def main_coverage(TESTS):
    TEST_NAMES = [f.rpartition('.')[0] for f in glob.glob("test_*.py")]
    TEST_NAMES.sort()

    pwd=os.getcwd()
    os.chdir(sys.argv[1])
    BAD_MODULES=(sys.argv[3] if len(sys.argv) >= 4 else '').split(',')
    MODULE_NAMES=[sys.argv[2] + '.' +f[0:-3] for f in glob.glob("*.py") if f not in BAD_MODULES]
    MODULE_NAMES.sort()
    os.chdir(pwd)

    modulenames = MODULE_NAMES

    try:
        cov = coverage.coverage(branch=True)
    except TypeError:
        cov = coverage
    cov.erase()
    cov.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')
    cov.start()

    modules = []
    for modulename in modulenames:
        mod = my_import(modulename)
        modules.append(mod)

    if 'unittest' in TESTS:
        print "***** Unittest *****"
        test_args = {'verbosity': 1}
        suite = unittest.TestLoader().loadTestsFromNames(TEST_NAMES)
        unittest.TextTestRunner(**test_args).run(suite)

    if 'doctest' in TESTS:
        t0 = time.time()
        print "\n***** Doctest *****"
        for mod in modules:
            doctest.testmod(mod, verbose=VERBOSE)
        td = time.time() - t0
        print "      Tests took %.3f seconds" % (td, )

    print "\n***** Coverage Python *****"
    cov.stop()
    cov.report(modules, ignore_errors=1, show_missing=1)
    #cov.html_report(morfs=modules, directory='/tmp')
    cov.erase()



if __name__ == '__main__':
    main_coverage(['unittest', 'doctest'])


def run_unittests(g):
    test_args = {'verbosity': 1}
    for t in [t for t in g.keys()
                        if (t.startswith('Test') and issubclass(g[t], unittest.TestCase)) ]:
        suite = unittest.TestLoader().loadTestsFromTestCase(g[t])
        unittest.TextTestRunner(**test_args).run(suite)

