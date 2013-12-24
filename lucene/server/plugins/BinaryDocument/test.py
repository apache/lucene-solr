import sys
sys.path.insert(0, '../../src/python')
import os
import util

util.run('ant compile-test', '../..')
util.run('ant jar test')

