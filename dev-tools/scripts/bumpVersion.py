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

import argparse
import io
import os
import re
import subprocess
import sys

class Version(object):
  def __init__(self, major, minor, bugfix):
    self.major = major
    self.minor = minor
    self.bugfix = bugfix
    self.previous_dot_matcher = self.make_previous_matcher()
    self.dot = '%d.%d.%d' % (self.major, self.minor, self.bugfix) 
    self.constant = 'LUCENE_%d_%d_%d' % (self.major, self.minor, self.bugfix)

  @classmethod
  def parse(cls, value):
    match = re.search(r'(\d+)\.(\d+).(\d+)', value) 
    if match is None:
      raise argparse.ArgumentTypeError('Version argument must be of format x.y.z')
    return Version(*[int(v) for v in match.groups()])

  def __str__(self):
    return self.dot

  def make_previous_matcher(self, prefix='', suffix='', sep='\\.'):
    if self.is_bugfix_release():
      pattern = '%s%s%s%s%d' % (self.major, sep, self.minor, sep, self.bugfix - 1)
    elif self.is_minor_release():
      pattern = '%s%s%d%s\\d+' % (self.major, sep, self.minor - 1, sep)
    else:
      pattern = '%d%s\\d+%s\\d+' % (self.major - 1, sep, sep)

    return re.compile(prefix + '(' + pattern + ')' + suffix)

  def is_bugfix_release(self):
    return self.bugfix != 0

  def is_minor_release(self):
    return self.bugfix == 0 and self.minor != 0

  def is_major_release(self):
    return self.bugfix == 0 and self.minor == 0

def run(cmd):
  try:
    subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as e:
    print(e.output.decode('utf-8'))
    raise e

def update_file(filename, line_re, edit):
  infile = open(filename, 'r')
  buffer = [] 
  
  changed = False
  for line in infile:
    if not changed:
      match = line_re.search(line)
      if match:
        changed = edit(buffer, match, line)
        if changed is None:
          return False
        continue
    buffer.append(line)
  if not changed:
    raise Exception('Could not find %s in %s' % (line_re, filename))
  with open(filename, 'w') as f:
    f.write(''.join(buffer))
  return True

def update_changes(filename, new_version):
  print('  adding new section to %s...' % filename, end='')
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
  print('  adding constant %s...' % new_version.constant, end='')
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
  print('  changing version.base...', end='')
  filename = 'lucene/version.properties'
  def edit(buffer, match, line):
    if new_version.dot in line:
      return None
    buffer.append('version.base=' + new_version.dot + '\n')
    return True 

  changed = update_file(filename, version_prop_re, edit)
  print('done' if changed else 'uptodate')

def update_latest_constant(new_version):
  print('  changing Version.LATEST to %s...' % new_version.constant, end='')
  filename = 'lucene/core/src/java/org/apache/lucene/util/Version.java'
  matcher = re.compile('public static final Version LATEST')
  def edit(buffer, match, line):
    if new_version.constant in line:
      return None
    buffer.append(line.rpartition('=')[0] + ('= %s;\n' % new_version.constant))
    return True

  changed = update_file(filename, matcher, edit)
  print('done' if changed else 'uptodate')
  
def update_example_solrconfigs(new_version):
  print('  updating example solrconfig.xml files')
  matcher = re.compile('<luceneMatchVersion>')

  for root,dirs,files in os.walk('solr/example'):
    for f in files:
      if f == 'solrconfig.xml':
        update_solrconfig(os.path.join(root, f), matcher, new_version) 

def update_solrconfig(filename, matcher, new_version):
  print('    %s...' % filename, end='')
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

def codec_exists(version):
  codecs_dir = 'lucene/core/src/java/org/apache/lucene/codecs'
  codec_file = '%(dir)s/lucene%(x)s%(y)s/Lucene%(x)s%(y)sCodec.java'
  return os.path.exists(codec_file % {'x': version.major, 'y': version.minor, 'dir': codecs_dir})

def create_backcompat_indexes(version, on_trunk):
  majorminor = '%d%d' % (version.major, version.minor)
  codec = 'Lucene%s' % majorminor
  backcompat_dir = 'lucene/backward-codecs' if on_trunk else 'lucene/core'

  create_index(codec, backcompat_dir, 'cfs', majorminor)
  create_index(codec, backcompat_dir, 'nocfs', majorminor)

def create_index(codec, codecs_dir, type, majorminor):
  filename = 'index.%s.%s.zip' % (majorminor, type)
  print('  creating %s...' % filename, end='')
  index_dir = 'src/test/org/apache/lucene/index'
  if os.path.exists(os.path.join(codecs_dir, index_dir, filename)):
    print('uptodate')
    return

  test = {'cfs': 'testCreateCFS', 'nocfs': 'testCreateNonCFS'}[type]
  ant_args = ' '.join([
    '-Dtests.codec=%s' % codec,
    '-Dtests.useSecurityManager=false',
    '-Dtestcase=CreateBackwardsCompatibilityIndex',
    '-Dtestmethod=%s' % test
  ])
  base_dir = os.getcwd()
  bc_index_dir = '/tmp/idx/index.%s' % type
  bc_index_file = os.path.join(bc_index_dir, filename)
  
  success = False
  if not os.path.exists(bc_index_file):
    os.chdir(codecs_dir)
    run('ant test %s' % ant_args)
    os.chdir('/tmp/idx/index.%s' % type)
    run('zip %s *' % filename)
  run('cp %s %s' % (bc_index_file, os.path.join(base_dir, codecs_dir, index_dir)))
  os.chdir(base_dir)
  run('svn add %s' % os.path.join(codecs_dir, index_dir, filename))
  success = True

  os.chdir(base_dir)
  run('rm -rf %s' % bc_index_dir)
  if success:
    print('done')

def update_backcompat_tests(version, on_trunk):
  majorminor = '%d%d' % (version.major, version.minor)
  print('  adding new indexes to backcompat tests...', end='')
  basedir = 'lucene/backward-codecs' if on_trunk else 'lucene/core'
  filename = '%s/src/test/org/apache/lucene/index/TestBackwardsCompatibility.java' % basedir
  matcher = re.compile(r'final static String\[\] oldNames = {|};')
  cfs_name = '%s.cfs' % majorminor
  nocfs_name = '%s.nocfs' % majorminor

  class Edit(object):
    start = None
    def __call__(self, buffer, match, line):
      if self.start:
        # first check if the indexes we are adding already exist      
        last_ndx = len(buffer) - 1 
        i = last_ndx
        while i >= self.start:
          if cfs_name in buffer[i]:
            return None
          i -= 1

        last = buffer[last_ndx]
        spaces = ' ' * (len(last) - len(last.lstrip()))
        quote_ndx = last.find('"')
        quote_ndx = last.find('"', quote_ndx + 1)
        buffer[last_ndx] = last[:quote_ndx + 1] + "," + last[quote_ndx + 1:]
        buffer.append(spaces + ('"%s",\n' % cfs_name))
        buffer.append(spaces + ('"%s"\n' % nocfs_name))
        buffer.append(line)
        return True

      if 'oldNames' in line:
        self.start = len(buffer) # location of first index name
      buffer.append(line)
      return False
        
  changed = update_file(filename, matcher, Edit())
  print('done' if changed else 'uptodate')

def check_lucene_version_tests():
  print('  checking lucene version tests...', end='')
  base_dir = os.getcwd()
  os.chdir('lucene/core') 
  run('ant test -Dtestcase=TestVersion')
  os.chdir(base_dir)
  print('ok')

def check_solr_version_tests():
  print('  checking solr version tests...', end='')
  base_dir = os.getcwd()
  os.chdir('solr/core') 
  run('ant test -Dtestcase=TestLuceneMatchVersion')
  os.chdir(base_dir)
  print('ok')

def check_backcompat_tests(on_trunk):
  print('  checking backcompat tests...', end='')
  base_dir = os.getcwd()
  basedir = 'lucene/backward-codecs' if on_trunk else 'lucene/core'
  os.chdir(basedir) 
  run('ant test -Dtestcase=TestBackwardsCompatibility')
  os.chdir(base_dir)
  print('ok')

# branch types are "release", "stable" and "trunk"
def find_branch_type():
  output = subprocess.check_output('svn info', shell=True)
  for line in output.split(b'\n'):
    if line.startswith(b'URL:'):
      url = line.split(b'/')[-1]
      break
  else:
    raise Exception('svn info missing repo URL')

  if url == b'trunk':
    return 'trunk'
  if url.startswith(b'branch_'):
    return 'stable'
  if url.startswith(b'lucene_solr_'):
    return 'release'
  raise Exception('Cannot run bumpVersion.py on feature branch')

def find_previous_version():
  return version_prop_re.search(open('lucene/version.properties').read()).group(1)

def merge_change(changeid, repo):
  print('\nMerging downstream change %d...' % changeid, end='')
  run('svn merge -c %d --record-only %s' % (changeid, repo))
  print('done')

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

  if c.matching_branch:
    c.previous_version = Version.parse(find_previous_version())
  elif c.version.is_minor_release():
    c.previous_version = Version(c.version.major, c.version.minor - 1, 0)
  elif c.version.is_bugfix_release():
    c.previous_version = Version(c.version.major, c.version.minor, c.version.bugfix - 1)

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

  run_backcompat_tests = False
  on_trunk = c.branch_type == 'trunk'
  if not c.version.is_bugfix_release() and codec_exists(c.previous_version):
    print('\nCreating backwards compatibility tests')
    create_backcompat_indexes(c.previous_version, on_trunk)
    update_backcompat_tests(c.previous_version, on_trunk)
    run_backcompat_tests = True

  if c.version.is_major_release():
    print('\nTODO: ')
    print('  - Update major version bounds in Version.java')
    print('  - Move backcompat oldIndexes to unsupportedIndexes in TestBackwardsCompatibility')
    print('  - Update IndexFormatTooOldException throw cases')
  else:
    print('\nTesting changes')
    check_lucene_version_tests()
    check_solr_version_tests()
    if run_backcompat_tests: 
      check_backcompat_tests(on_trunk)

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')
