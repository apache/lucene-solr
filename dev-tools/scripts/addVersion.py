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
import io
import re
import subprocess

def update_changes(filename, new_version):
  print('  adding new section to %s...' % filename, end='', flush=True)
  matcher = re.compile(r'\d+\.\d+\.\d+\s+===')
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    match = new_version.previous_dot_matcher.search(line)
    if match is not None:
      buffer.append(line.replace(match.group(0), new_version.dot))
      buffer.append('(No Changes)\n\n')
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
      buffer[-1] = spaces + (' * @deprecated (%s) Use latest\n' % new_version)
      buffer.append(spaces + ' */\n')
      buffer.append(spaces + '@Deprecated\n')

  def buffer_constant(buffer, line):
    spaces = ' ' * (len(line) - len(line.lstrip()))
    buffer.append('\n' + spaces + '/**\n')
    buffer.append(spaces + ' * Match settings and bugs in Lucene\'s %s release.\n' % new_version)
    if deprecate:
      buffer.append(spaces + ' * @deprecated Use latest\n')
    buffer.append(spaces + ' */\n')
    if deprecate:
      buffer.append(spaces + '@Deprecated\n')
    buffer.append(spaces + 'public static final Version %s = new Version(%d, %d, %d);\n' %
                  (new_version.constant, new_version.major, new_version.minor, new_version.bugfix))
  
  class Edit(object):
    found = -1
    def __call__(self, buffer, match, line):
      if new_version.constant in line:
        return None # constant already exists
      # outter match is just to find lines declaring version constants
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

def read_config():
  parser = argparse.ArgumentParser(description='Add a new version')
  parser.add_argument('version', type=Version.parse)
  parser.add_argument('-c', '--changeid', type=int, help='SVN ChangeId for downstream version change to merge')
  parser.add_argument('-r', '--downstream-repo', help='Path to downstream checkout for given changeid')
  c = parser.parse_args()

  c.branch_type = find_branch_type()
  c.matching_branch = c.version.is_bugfix_release() and c.branch_type == 'release' or \
                      c.version.is_minor_release() and c.branch_type == 'stable' or \
                      c.branch_type == 'major'

  if bool(c.changeid) != bool(c.downstream_repo):
    parser.error('--changeid and --upstream-repo must be used together')
  if not c.changeid and not c.matching_branch:
    parser.error('Must use --changeid for forward porting bugfix release version to other branches')
  if c.changeid and c.matching_branch:
    parser.error('Cannot use --changeid on branch that new version will originate on')
  if c.changeid and c.version.is_major_release():
    parser.error('Cannot use --changeid for major release')

  return c
  
def main():
  c = read_config() 

  if c.changeid:
    merge_change(c.changeid, c.downstream_repo)  

  print('\nAdding new version %s' % c.version)
  update_changes('lucene/CHANGES.txt', c.version)
  update_changes('solr/CHANGES.txt', c.version)
  add_constant(c.version, not c.matching_branch) 

  if not c.changeid:
    print('\nUpdating latest version')
    update_build_version(c.version)
    update_latest_constant(c.version)
    update_example_solrconfigs(c.version)

  if c.version.is_major_release():
    print('\nTODO: ')
    print('  - Move backcompat oldIndexes to unsupportedIndexes in TestBackwardsCompatibility')
    print('  - Update IndexFormatTooOldException throw cases')
  else:
    print('\nTesting changes')
    check_lucene_version_tests()
    check_solr_version_tests()

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')
