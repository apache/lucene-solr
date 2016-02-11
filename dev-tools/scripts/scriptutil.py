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
  def __init__(self, major, minor, bugfix, prerelease):
    self.major = major
    self.minor = minor
    self.bugfix = bugfix
    self.prerelease = prerelease
    self.previous_dot_matcher = self.make_previous_matcher()
    self.dot = '%d.%d.%d' % (self.major, self.minor, self.bugfix) 
    self.constant = 'LUCENE_%d_%d_%d' % (self.major, self.minor, self.bugfix)

  @classmethod
  def parse(cls, value):
    match = re.search(r'(\d+)\.(\d+).(\d+)(.1|.2)?', value) 
    if match is None:
      raise argparse.ArgumentTypeError('Version argument must be of format x.y.z(.1|.2)?')
    parts = [int(v) for v in match.groups()[:-1]]
    parts.append({ None: 0, '.1': 1, '.2': 2 }[match.groups()[-1]])
    return Version(*parts)

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

  def on_or_after(self, other):
    return (self.major > other.major or self.major == other.major and
           (self.minor > other.minor or self.minor == other.minor and
           (self.bugfix > other.bugfix or self.bugfix == other.bugfix and
           self.prerelease >= other.prerelease)))

def run(cmd):
  try:
    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as e:
    print(e.output.decode('utf-8'))
    raise e
  return output.decode('utf-8') 

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

# branch types are "release", "stable" and "trunk"
def find_branch_type():
  output = subprocess.check_output('git status', shell=True)
  for line in output.split(b'\n'):
    if line.startswith(b'On branch '):
      branchName = line.split(b' ')[-1]
      break
  else:
    raise Exception('git status missing branch name')

  if branchName == b'master':
    return 'master'
  if branchName.startswith(b'branch_'):
    return 'stable'
  if branchName.startswith(b'lucene_solr_'):
    return 'release'
  raise Exception('Cannot run bumpVersion.py on feature branch')

version_prop_re = re.compile('version\.base=(.*)')
def find_current_version():
  return version_prop_re.search(open('lucene/version.properties').read()).group(1)

if __name__ == '__main__':
  print('This is only a support module, it cannot be run')
  sys.exit(1)
