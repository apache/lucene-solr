# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
sys.path.append(os.path.dirname(__file__))
from scriptutil import *

import argparse
import re
from configparser import ConfigParser, ExtendedInterpolation
from textwrap import dedent

def update_changes(filename, new_version, init_changes = '(No Changes)\n\n'):
  print('  adding new section to %s...' % filename, end='', flush=True)
  matcher = re.compile(r'\d+\.\d+\.\d+\s+===')
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    match = new_version.previous_dot_matcher.search(line)
    if match is not None:
      buffer.append(line.replace(match.group(0), new_version.dot))
      buffer.append(init_changes)
    buffer.append(line)
    return match is not None
     
  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def add_constant(new_version, deprecate):
  filename = 'lucene/core/src/java/org/apache/lucene/util/Version.java'
  print('  adding constant %s...' % new_version.constant, end='', flush=True)
  constant_prefix = 'public static final Version LUCENE_'
  matcher = re.compile(constant_prefix)
  prev_matcher = new_version.make_previous_matcher(prefix=constant_prefix, sep='_')

  def ensure_deprecated(buffer):
    last = buffer[-1]
    if last.strip() != '@Deprecated':
      spaces = ' ' * (len(last) - len(last.lstrip()) - 1)
      del buffer[-1] # Remove comment closer line
      if (len(buffer) >= 4 and re.search('for Lucene.\s*$', buffer[-1]) != None):
        del buffer[-3:] # drop the trailing lines '<p> / Use this to get the latest ... / ... for Lucene.'
      buffer.append(( '{0} * @deprecated ({1}) Use latest\n'
                    + '{0} */\n'
                    + '{0}@Deprecated\n').format(spaces, new_version))

  def buffer_constant(buffer, line):
    spaces = ' ' * (len(line) - len(line.lstrip()))
    buffer.append(( '\n{0}/**\n'
                  + '{0} * Match settings and bugs in Lucene\'s {1} release.\n')
                  .format(spaces, new_version))
    if deprecate:
      buffer.append('%s * @deprecated Use latest\n' % spaces)
    else:
      buffer.append(( '{0} * <p>\n'
                    + '{0} * Use this to get the latest &amp; greatest settings, bug\n'
                    + '{0} * fixes, etc, for Lucene.\n').format(spaces))
    buffer.append('%s */\n' % spaces)
    if deprecate:
      buffer.append('%s@Deprecated\n' % spaces)
    buffer.append('{0}public static final Version {1} = new Version({2}, {3}, {4});\n'.format
                  (spaces, new_version.constant, new_version.major, new_version.minor, new_version.bugfix))
  
  class Edit(object):
    found = -1
    def __call__(self, buffer, match, line):
      if new_version.constant in line:
        return None # constant already exists
      # outer match is just to find lines declaring version constants
      match = prev_matcher.search(line)
      if match is not None:
        ensure_deprecated(buffer) # old version should be deprecated
        self.found = len(buffer) + 1 # extra 1 for buffering current line below
      elif self.found != -1:
        # we didn't match, but we previously had a match, so insert new version here
        # first find where to insert (first empty line before current constant)
        c = []
        buffer_constant(c, line)
        tmp = buffer[self.found:]
        buffer[self.found:] = c
        buffer.extend(tmp)
        buffer.append(line)
        return True

      buffer.append(line)
      return False
  
  changed = update_file(filename, matcher, Edit())
  print('done' if changed else 'uptodate')

version_prop_re = re.compile('version\.base=(.*)')
def update_build_version(new_version):
  print('  changing version.base...', end='', flush=True)
  filename = 'lucene/version.properties'
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    buffer.append('version.base=' + new_version.dot + '\n')
    return True 

  changed = update_file(filename, version_prop_re, edit)
  print('done' if changed else 'uptodate')

def update_latest_constant(new_version):
  print('  changing Version.LATEST to %s...' % new_version.constant, end='', flush=True)
  filename = 'lucene/core/src/java/org/apache/lucene/util/Version.java'
  matcher = re.compile('public static final Version LATEST')
  def edit(buffer, match, line):
    if new_version.constant in line:
      return None
    buffer.append(line.rpartition('=')[0] + ('= %s;\n' % new_version.constant))
    return True

  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def onerror(x):
  raise x

def update_example_solrconfigs(new_version):
  print('  updating example solrconfig.xml files')
  matcher = re.compile('<luceneMatchVersion>')

  paths = ['solr/server/solr/configsets', 'solr/example']
  for path in paths:
    if not os.path.isdir(path):
      raise RuntimeError("Can't locate configset dir (layout change?) : " + path)
    for root,dirs,files in os.walk(path, onerror=onerror):
      for f in files:
        if f == 'solrconfig.xml':
          update_solrconfig(os.path.join(root, f), matcher, new_version)

def update_solrconfig(filename, matcher, new_version):
  print('    %s...' % filename, end='', flush=True)
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    match = new_version.previous_dot_matcher.search(line)
    if match is None:
      return False
    buffer.append(line.replace(match.group(1), new_version.dot))
    return True

  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')

def check_lucene_version_tests():
  print('  checking lucene version tests...', end='', flush=True)
  base_dir = os.getcwd()
  os.chdir('lucene/core') 
  run('ant test -Dtestcase=TestVersion')
  os.chdir(base_dir)
  print('ok')

def check_solr_version_tests():
  print('  checking solr version tests...', end='', flush=True)
  base_dir = os.getcwd()
  os.chdir('solr/core') 
  run('ant test -Dtestcase=TestLuceneMatchVersion')
  os.chdir(base_dir)
  print('ok')

def read_config(current_version):
  parser = argparse.ArgumentParser(description='Add a new version')
  parser.add_argument('version', type=Version.parse)
  c = parser.parse_args()

  c.branch_type = find_branch_type()
  c.is_latest_version = c.version.on_or_after(current_version)

  print ("branch_type is %s " % c.branch_type)

  return c

# Hack ConfigParser, designed to parse INI files, to parse & interpolate Java .properties files
def parse_properties_file(filename):
  contents = open(filename, encoding='ISO-8859-1').read().replace('%', '%%') # Escape interpolation metachar
  parser = ConfigParser(interpolation=ExtendedInterpolation())               # Handle ${property-name} interpolation
  parser.read_string("[DUMMY_SECTION]\n" + contents)                         # Add required section
  return dict(parser.items('DUMMY_SECTION'))

def get_solr_init_changes():
  return dedent('''
    Consult the LUCENE_CHANGES.txt file for additional, low level, changes in this release.

    Versions of Major Components
    ---------------------
    Apache Tika %(org.apache.tika.version)s
    Carrot2 %(/org.carrot2/carrot2-mini)s
    Velocity %(/org.apache.velocity/velocity)s and Velocity Tools %(/org.apache.velocity/velocity-tools)s
    Apache UIMA %(org.apache.uima.version)s
    Apache ZooKeeper %(/org.apache.zookeeper/zookeeper)s
    Jetty %(org.eclipse.jetty.version)s


    (No Changes)\n\n
    ''' % parse_properties_file('lucene/ivy-versions.properties'))
  
def main():
  current_version = Version.parse(find_current_version())
  c = read_config(current_version)

  print('\nAdding new version %s' % c.version)
  update_changes('lucene/CHANGES.txt', c.version)
  update_changes('solr/CHANGES.txt', c.version, get_solr_init_changes())

  is_back_compat = current_version.major == c.version.major or current_version.is_back_compat_with(c.version)

  if is_back_compat:
    add_constant(c.version, not c.is_latest_version)
  else:
    print('\nNot adding constant for version %s because it is no longer supported' % c.version)

  if c.is_latest_version:
    print('\nUpdating latest version')
    update_build_version(c.version)
    update_latest_constant(c.version)
    update_example_solrconfigs(c.version)

  if c.version.is_major_release():
    print('\nTODO: ')
    print('  - Move backcompat oldIndexes to unsupportedIndexes in TestBackwardsCompatibility')
    print('  - Update IndexFormatTooOldException throw cases')
  elif is_back_compat:
    print('\nTesting changes')
    check_lucene_version_tests()
    check_solr_version_tests()

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')
