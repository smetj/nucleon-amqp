#!/usr/bin/python

import os
import sys

import nose
from coverage import coverage
import nucleon

thisdir = os.path.abspath(os.path.dirname(__file__))

#Start collecting coverage data
cov = coverage(branch=True, source=[os.path.join(thisdir, 'nucleon', 'amqp')])
cov.start()

# Run the nosetest with xunit enabled
args = (
    'nosetests -v -s --nologcapture '
    '--with-xunit --xunit-file tests.xunit.xml tests'
).split()
nose.run(argv=args)

#Stop collecting the coverage data and convert it into XML format
cov.stop()
cov.save()
cov.xml_report(outfile='coverage.xml.in')

# Rewrite coverage report to remove absolute paths

subs = []  # substitutions we want to make
for p in nucleon.__path__:
    subs.append(os.path.dirname(p) + '/')
    subs.append(p.replace('/', '.'))

with open('coverage.xml.in', 'r') as input:
    with open('coverage.xml', 'w') as output:
        for l in input:
            for s in subs:
                l = l.replace(s, '')
            output.write(l)

cov.report(file=sys.stdout)

