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

import datetime
import re
import shutil
import os
import sys

# Usage: python -u buildRelease.py /path/to/checkout version(eg: 3.4.0) gpgKey(eg: 6E68DA61) rcNum user [-prepare] [-push]
#
# EG: python -u buildRelease.py -prepare -push /lucene/34x 3.4.0 6E68DA61 1 mikemccand
# 

# TODO: also run smokeTestRelease.py?

# NOTE: you have to type in your gpg password at some point while this
# runs; it's VERY confusing because the output is directed to
# /tmp/release.log, so, you have to tail that and when GPG wants your
# password, type it!

LOG = '/tmp/release.log'

def log(msg):
  f = open(LOG, 'ab')
  f.write(msg)
  f.close()
  
def run(command):
  log('\n\n%s: RUN: %s\n' % (datetime.datetime.now(), command))
  if os.system('%s >> %s 2>&1' % (command, LOG)):
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    print msg
    raise RuntimeError(msg)

def scrubCheckout():
  # removes any files not checked into svn

  unversionedRex = re.compile('^ ?[\?ID] *[1-9 ]*[a-zA-Z]* +(.*)')

  for l in os.popen('svn status --no-ignore -v').readlines():
    match = unversionedRex.match(l)
    if match:
      s = match.group(1)
      if os.path.exists(s):
        print '    delete %s' % s
        if os.path.isdir(s) and not os.path.islink(s):
          shutil.rmtree(s)
        else:
          os.remove(s)

def getSVNRev():
  rev = os.popen('svnversion').read().strip()
  try:
    int(rev)
  except (TypeError, ValueError):
    raise RuntimeError('svn version is not clean: %s' % rev)
  return rev
  

def prepare(root, version, gpgKeyID):
  print
  print 'Prepare release...'
  if os.path.exists(LOG):
    os.remove(LOG)

  os.chdir(root)
  print '  svn up...'
  run('svn up')

  rev = getSVNRev()
  print '  svn rev: %s' % rev
  log('\nSVN rev: %s\n' % rev)

  print '  ant clean test'
  run('ant clean test')

  print '  clean checkout'
  scrubCheckout()
  open('rev.txt', 'wb').write(rev)
  
  print '  lucene prepare-release'
  os.chdir('lucene')
  run('ant -Dversion=%s -Dspecversion=%s -Dgpg.key=%s prepare-release' % (version, version, gpgKeyID))
  print '  solr prepare-release'
  os.chdir('../solr')
  run('ant -Dversion=%s -Dspecversion=%s -Dgpg.key=%s prepare-release' % (version, version, gpgKeyID))
  print '  done!'
  print
  return rev

def push(version, root, rev, rcNum, username):
  print 'Push...'
  dir = 'lucene-solr-%s-RC%d-rev%s' % (version, rcNum, rev)
  s = os.popen('ssh %s@people.apache.org "ls -ld public_html/staging_area/%s" 2>&1' % (username, dir)).read()
  if s.lower().find('no such file or directory') == -1:
    print '  Remove old dir...'
    run('ssh %s@people.apache.org "chmod -R u+rwX public_html/staging_area/%s; rm -rf public_html/staging_area/%s"' % 
        (username, dir, dir))
  run('ssh %s@people.apache.org "mkdir -p public_html/staging_area/%s/lucene public_html/staging_area/%s/solr"' % \
      (username, dir, dir))
  print '  Lucene'
  os.chdir('%s/lucene/dist' % root)
  print '    zip...'
  if os.path.exists('lucene.tar.bz2'):
    os.remove('lucene.tar.bz2')
  run('tar cjf lucene.tar.bz2 *')
  print '    copy...'
  run('scp lucene.tar.bz2 %s@people.apache.org:public_html/staging_area/%s/lucene' % (username, dir))
  print '    unzip...'
  run('ssh %s@people.apache.org "cd public_html/staging_area/%s/lucene; tar xjf lucene.tar.bz2; rm -f lucene.tar.bz2"' % (username, dir))
  os.remove('lucene.tar.bz2')
  print '    copy changes...'
  os.chdir('..')
  run('scp -r build/docs/changes %s@people.apache.org:public_html/staging_area/%s/lucene/changes-%s' % (username, dir, version))

  print '  Solr'
  os.chdir('%s/solr/package' % root)
  print '    zip...'
  if os.path.exists('solr.tar.bz2'):
    os.remove('solr.tar.bz2')
  run('tar cjf solr.tar.bz2 *')
  print '    copy...'
  run('scp solr.tar.bz2 %s@people.apache.org:public_html/staging_area/%s/solr' % (username, dir))
  print '    unzip...'
  run('ssh %s@people.apache.org "cd public_html/staging_area/%s/solr; tar xjf solr.tar.bz2; rm -f solr.tar.bz2"' % (username, dir))
  os.remove('solr.tar.bz2')

  print '  KEYS'
  run('wget http://people.apache.org/keys/group/lucene.asc')
  os.rename('lucene.asc', 'KEYS')
  run('chmod a+r-w KEYS')
  run('scp KEYS %s@people.apache.org:public_html/staging_area/%s/lucene' % (username, dir))
  run('scp KEYS %s@people.apache.org:public_html/staging_area/%s/solr' % (username, dir))
  os.remove('KEYS')

  print '  chmod...'
  run('ssh %s@people.apache.org "chmod -R a+rX-w public_html/staging_area/%s"' % (username, dir))

  print '  done!  URL: https://people.apache.org/~%s/staging_area/%s' % (username, dir)

  
def main():
  doPrepare = '-prepare' in sys.argv
  if doPrepare:
    sys.argv.remove('-prepare')
  doPush = '-push' in sys.argv
  if doPush:
    sys.argv.remove('-push')
  root = os.path.abspath(sys.argv[1])
  version = sys.argv[2]
  gpgKeyID = sys.argv[3]
  rcNum = int(sys.argv[4])
  username = sys.argv[5]

  if doPrepare:
    rev = prepare(root, version, gpgKeyID)
  else:
    os.chdir(root)
    rev = open('rev.txt').read()

  if doPush:
    push(version, root, rev, rcNum, username)
    
if __name__ == '__main__':
  main()
