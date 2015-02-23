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
import scriptutil

import argparse
import urllib.error
import urllib.request
import re
import shutil

def create_and_add_index(source, indextype, version, temp_dir):
  if indextype in ('cfs', 'nocfs'):
    dirname = 'index.%s' % indextype
  else:
    dirname = indextype
  filename = {
    'cfs': 'index.%s-cfs.zip',
    'nocfs': 'index.%s-nocfs.zip'
  }[indextype] % version
  print('  creating %s...' % filename, end='', flush=True)
  module = 'backward-codecs'
  index_dir = os.path.join('lucene', module, 'src/test/org/apache/lucene/index')
  test_file = os.path.join(index_dir, filename)
  if os.path.exists(os.path.join(index_dir, filename)):
    print('uptodate')
    return

  test = {
    'cfs': 'testCreateCFS',
    'nocfs': 'testCreateNoCFS'
  }[indextype]
  ant_args = ' '.join([
    '-Dtests.bwcdir=%s' % temp_dir,
    '-Dtests.codec=default',
    '-Dtests.useSecurityManager=false',
    '-Dtestcase=TestBackwardsCompatibility',
    '-Dtestmethod=%s' % test
  ])
  base_dir = os.getcwd()
  bc_index_dir = os.path.join(temp_dir, dirname)
  bc_index_file = os.path.join(bc_index_dir, filename)
  
  if os.path.exists(bc_index_file):
    print('alreadyexists')
  else:
    if os.path.exists(bc_index_dir):
      shutil.rmtree(bc_index_dir)
    os.chdir(os.path.join(source, module))
    scriptutil.run('ant test %s' % ant_args)
    os.chdir(bc_index_dir)
    scriptutil.run('zip %s *' % filename)
    print('done')
  
  print('  adding %s...' % filename, end='', flush=True)
  scriptutil.run('cp %s %s' % (bc_index_file, os.path.join(base_dir, index_dir)))
  os.chdir(base_dir)
  output = scriptutil.run('svn status %s' % test_file)
  if not output.strip():
    # make sure to only add if the file isn't already in svn (we might be regenerating)
    scriptutil.run('svn add %s' % test_file)
  os.chdir(base_dir)
  scriptutil.run('rm -rf %s' % bc_index_dir)
  print('done')

def update_backcompat_tests(types, version):
  print('  adding new indexes to backcompat tests...', end='', flush=True)
  module = 'lucene/backward-codecs'
  filename = '%s/src/test/org/apache/lucene/index/TestBackwardsCompatibility.java' % module
  matcher = re.compile(r'final static String\[\] oldNames = {|};')

  def find_version(x):
    x = x.strip()
    end = x.index("-")
    return scriptutil.Version.parse(x[1:end])

  class Edit(object):
    start = None
    def __call__(self, buffer, match, line):
      if self.start:
        # find where we this version should exist
        i = len(buffer) - 1 
        v = find_version(buffer[i])
        while i >= self.start and v.on_or_after(version):
          i -= 1
          v = find_version(buffer[i])
        i += 1 # readjust since we skipped past by 1

        # unfortunately python doesn't have a range remove from list...
        # here we want to remove any previous references to the version we are adding
        while i < len(buffer) and version.on_or_after(find_version(buffer[i])):
          buffer.pop(i)

        if i == len(buffer) and not buffer[-1].strip().endswith(","):
          # add comma
          buffer[-1] = buffer[-1].rstrip() + ",\n" 

        last = buffer[-1]
        spaces = ' ' * (len(last) - len(last.lstrip()))
        for (j, t) in enumerate(types):
          newline = spaces + ('"%s-%s"' % (version, t))
          if j < len(types) - 1 or i < len(buffer):
            newline += ','
          buffer.insert(i, newline + '\n')
          i += 1

        buffer.append(line)
        return True

      if 'oldNames' in line:
        self.start = len(buffer) # location of first index name
      buffer.append(line)
      return False
        
  changed = scriptutil.update_file(filename, matcher, Edit())
  print('done' if changed else 'uptodate')

def check_backcompat_tests():
  print('  checking backcompat tests...', end='', flush=True)
  olddir = os.getcwd()
  os.chdir('lucene/backward-codecs')
  scriptutil.run('ant test -Dtestcase=TestBackwardsCompatibility')
  os.chdir(olddir)
  print('ok')

def download_from_mirror(version, remotename, localname):
  url = 'http://apache.cs.utah.edu/lucene/java/%s/%s' % (version, remotename)
  try:
    urllib.request.urlretrieve(url, localname)
    return True
  except urllib.error.URLError as e:
    if e.code == 404:
      return False
    raise e

def download_from_archives(version, remotename, localname):
  url = 'http://archive.apache.org/dist/lucene/java/%s/%s' % (version, remotename)
  try:
    urllib.request.urlretrieve(url, localname)
    return True
  except urllib.error.URLError as e:
    if e.code == 404:
      return False
    raise e

def download_release(version, temp_dir, force):
  print('  downloading %s source release...' % version, end='', flush=True)
  source = os.path.join(temp_dir, 'lucene-%s' % version) 
  if os.path.exists(source):
    if force:
      shutil.rmtree(source)
    else:
      print('uptodate')
      return source

  filename = 'lucene-%s-src.tgz' % version
  source_tgz = os.path.join(temp_dir, filename)
  if not download_from_mirror(version, filename, source_tgz) and \
     not download_from_archives(version, filename, source_tgz):
    raise Exception('Could not find version %s in apache mirror or archives' % version)

  olddir = os.getcwd()
  os.chdir(temp_dir)
  scriptutil.run('tar -xvzf %s' % source_tgz)
  os.chdir(olddir) 
  print('done')
  return source

def read_config():
  parser = argparse.ArgumentParser(description='Add backcompat index and test for new version')
  parser.add_argument('--force', action='store_true', default=False,
                      help='Redownload the version and rebuild, even if it already exists')
  parser.add_argument('--no-cleanup', dest='cleanup', action='store_false', default=True,
                      help='Do not cleanup the built indexes, so that they can be reused ' +
                           'for adding to another branch')
  parser.add_argument('--temp-dir', metavar='DIR', default='/tmp/lucenebwc',
                      help='Temp directory to build backcompat indexes within')
  parser.add_argument('version', type=scriptutil.Version.parse,
                      help='Version to add, of the form X.Y.Z')
  c = parser.parse_args()

  return c
  
def main():
  c = read_config() 
  if not os.path.exists(c.temp_dir):
    os.makedirs(c.temp_dir)

  print('\nCreating backwards compatibility indexes')
  source = download_release(c.version, c.temp_dir, c.force)
  create_and_add_index(source, 'cfs', c.version, c.temp_dir)
  create_and_add_index(source, 'nocfs', c.version, c.temp_dir)
    
  print('\nAdding backwards compatibility tests')
  update_backcompat_tests(['cfs', 'nocfs'], c.version)

  print('\nTesting changes')
  check_backcompat_tests()

  if c.cleanup:
    print('\nCleaning up')
    print('  deleting %s...' % c.temp_dir, end='', flush=True)
    shutil.rmtree(c.temp_dir)
    print('done')

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nRecieved Ctrl-C, exiting early')
