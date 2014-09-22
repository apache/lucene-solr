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
'''
Creates a unified diff, applyable by the patch tool, between two source checkouts.

Note that .gitignore or svn:ignore rules are used to filter out files that would normally
not be checked in.

While you could use this to make a committable patch from a branch,
that approach loses the svn history from the branch (better to use
"svn merge --reintegrate", for example).  This diff output should
not be considered "authoritative" from a merging standpoint as it
does not reflect what svn will do on merge.
'''

from argparse import ArgumentParser, RawTextHelpFormatter
import os
import subprocess
import sys

def make_filter_func(src_dir):
  if os.path.exists(os.path.join(src_dir, '.git')):
    def git_filter(filename):
      rc = subprocess.call('git --git-dir=%s check-ignore %s' % (src_dir, filename), shell=True)
      return rc == 0
    return git_filter

  else:
    def svn_filter(filename):
      # we can't find if svn will ignore a file unless it exists...
      created = False
      if not os.path.exists(filename):
        head,tail = os.path.split(filename)
        # find a parent directory that already exists, so we
        # can see if that is ignored by svn
        while not os.path.exists(head):
          filename = head
          head,tail = os.path.split(filename)
        created = True
        subprocess.check_call('touch %s' % filename, shell=True)
      try:
        output = subprocess.check_output('svn status %s' % filename,
                                         shell=True, stderr=subprocess.STDOUT).strip()
        return output.startswith(b'I')
      finally:
        if created and os.path.exists(filename):
          os.remove(filename)

    return svn_filter

def print_filtered_output(output, should_filter):
  filtering = False
  line = output.readline()
  while line:

    if line.startswith(b'diff '):
      fromfile, tofile = line.decode('utf-8').split()[-2:]
      if os.path.exists(fromfile) or \
         os.path.exists(tofile):
        filtering = should_filter(fromfile)
      else:
        # If both files do not exist, then the filename must contain spaces,
        # which breaks our split logic.  In this case, just ignore, since
        # patch cannot handle filenames with spaces anyways.
        filtering = True
    elif line.startswith(b'Binary files'):
      filtering = True

    if not filtering:
      print(line.decode('utf-8'), end='')

    line = output.readline()

def run_diff(from_dir, to_dir, skip_whitespace):
  flags = '-ruN'
  if skip_whitespace:
    flags += 'bBw'

  args = ['diff', flags]
  for ignore in ('.svn', '.git', 'build', '.caches'):
    args.append('-x')
    args.append(ignore)
  args.append(from_dir)
  args.append(to_dir)

  return subprocess.Popen(args, shell=False, stdout=subprocess.PIPE)

def parse_config():
  parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
  parser.add_argument('--skip-whitespace', action='store_true', default=False,
                      help='Ignore whitespace differences')
  parser.add_argument('from_dir', help='Source directory to diff from')
  parser.add_argument('to_dir', help='Source directory to diff to')
  c = parser.parse_args() 

  if not os.path.isdir(c.from_dir):
    parser.error('\'from\' path %s is not a valid directory' % c.from_dir)
  if not os.path.exists(os.path.join(c.from_dir, 'lucene', 'CHANGES.txt')):
    parser.error('\'from\' path %s is not a valid lucene/solr checkout' % c.from_dir)
  if not os.path.isdir(c.to_dir):
    parser.error('\'to\' path %s is not a valid directory' % c.to_dir)
  if not os.path.exists(os.path.join(c.to_dir, 'lucene', 'CHANGES.txt')):
    parser.error('\'to\' path %s is not a valid lucene/solr checkout' % c.to_dir)

  return c

def main():
  c = parse_config()

  p = run_diff(c.from_dir, c.to_dir, c.skip_whitespace)
  should_filter = make_filter_func(c.from_dir)
  print_filtered_output(p.stdout, should_filter)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nReceived Ctrl-C, exiting early')

