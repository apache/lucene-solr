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

import subprocess
import sys
import os

# recursive, unified output format, treat missing files as present but empty
DIFF_FLAGS = '-ruN'

if '-skipWhitespace' in sys.argv:
  sys.argv.remove('-skipWhitespace')
  # ignores only whitespace changes
  DIFF_FLAGS += 'bBw'

if len(sys.argv) != 3:
  print
  print 'Usage: python -u diffSources.py <dir1> <dir2> [-skipWhitespace]'
  print
  print '''This tool creates an applying patch between two directories.

While you could use this to make a committable patch from a branch, that approach loses
the svn history from the branch (better to use "svn merge --reintegrate", for example).  This
diff output should not be considered "authoritative" from a merging standpoint as it does
not reflect what svn will do on merge.
'''
  print
  sys.exit(0)

p = subprocess.Popen(['diff', DIFF_FLAGS, '-x', '.svn', '-x', 'build', sys.argv[1], sys.argv[2]], shell=False, stdout=subprocess.PIPE)

keep = False
while True:
  l = p.stdout.readline()
  if l == '':
    break
  if l.endswith('\r\n'):
    l = l[:-2]
  elif l.endswith('\n'):
    l = l[:-1]
  if l.startswith('diff ') or l.startswith('Binary files '):

    if l.endswith('timehints.txt') or l.find('/build/') != -1 or l.find('/.svn/') != -1:
      keep = False
    elif l.lower().startswith('Only in'):
      keep = True
    elif l.find('/META-INF/') != -1:
      keep = True
    else:
      ext = os.path.splitext(l)[-1]
      keep = ext in ('.xml', '.iml', '.html', '.template', '.py', '.g', '.properties', '.java')

    if keep:
      print
      print
      print l.strip()
  elif keep:
    print l
  elif l.startswith('Only in'):
    print l.strip()
