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
import time
import shutil
import os
import sys
import subprocess

# Usage: python3.2 -u buildAndPushRelease.py [-sign gpgKey(eg: 6E68DA61)] [-prepare] [-push userName] [-pushLocal dirName] [-smoke tmpDir] /path/to/checkout version(eg: 3.4.0) rcNum(eg: 0)
#
# EG: python3.2 -u buildAndPushRelease.py -prepare -push -sign 6E68DA61 mikemccand /lucene/34x 3.4.0 0

# NOTE: if you specify -sign, you have to type in your gpg password at
# some point while this runs; it's VERY confusing because the output
# is directed to /tmp/release.log, so, you have to tail that and when
# GPG wants your password, type it!  Also sometimes you have to type
# it twice in a row!

LOG = '/tmp/release.log'

def log(msg):
  f = open(LOG, mode='ab')
  f.write(msg.encode('utf-8'))
  f.close()
  
def run(command):
  log('\n\n%s: RUN: %s\n' % (datetime.datetime.now(), command))
  if os.system('%s >> %s 2>&1' % (command, LOG)):
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    print(msg)
    raise RuntimeError(msg)

def runAndSendGPGPassword(command, password):
  p = subprocess.Popen(command, shell=True, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
  f = open(LOG, 'ab')
  while True:
    p.stdout.flush()
    line = p.stdout.readline()
    if len(line) == 0:
      break
    f.write(line)
    if line.find(b'Enter GPG keystore password:') != -1:
      time.sleep(1.0)
      p.stdin.write((password + '\n').encode('UTF-8'))
      p.stdin.write('\n'.encode('UTF-8'))

  result = p.poll()
  if result != 0:
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    print(msg)
    raise RuntimeError(msg)

def scrubCheckout():
  # removes any files not checked into svn

  unversionedRex = re.compile('^ ?[\?ID] *[1-9 ]*[a-zA-Z]* +(.*)')

  for l in os.popen('svn status --no-ignore -v').readlines():
    match = unversionedRex.match(l)
    if match:
      s = match.group(1)
      if os.path.exists(s):
        print('    delete %s' % s)
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
  

def prepare(root, version, gpgKeyID, gpgPassword, doTest):
  print()
  print('Prepare release...')
  if os.path.exists(LOG):
    os.remove(LOG)

  os.chdir(root)
  print('  svn up...')
  run('svn up')

  rev = getSVNRev()
  print('  svn rev: %s' % rev)
  log('\nSVN rev: %s\n' % rev)

  if doTest:
    # Don't run tests if we are gonna smoke test after the release...
    print('  ant clean test')
    run('ant clean test')

  print('  clean checkout')
  scrubCheckout()
  open('rev.txt', mode='wb').write(rev.encode('UTF-8'))
  
  print('  lucene prepare-release')
  os.chdir('lucene')
  cmd = 'ant -Dversion=%s -Dspecversion=%s' % (version, version)
  if gpgKeyID is not None:
    cmd += ' -Dgpg.key=%s prepare-release' % gpgKeyID
  else:
    cmd += ' prepare-release-no-sign'

  if gpgPassword is not None:
    runAndSendGPGPassword(cmd, gpgPassword)
  else:
    run(cmd)
  
  print('  solr prepare-release')
  os.chdir('../solr')
  cmd = 'ant -Dversion=%s -Dspecversion=%s' % (version, version)
  if gpgKeyID is not None:
    cmd += ' -Dgpg.key=%s prepare-release' % gpgKeyID
  else:
    cmd += ' prepare-release-no-sign'

  if gpgPassword is not None:
    runAndSendGPGPassword(cmd, gpgPassword)
  else:
    run(cmd)
    
  print('  done!')
  print()
  return rev

def push(version, root, rev, rcNum, username):
  print('Push...')
  dir = 'lucene-solr-%s-RC%d-rev%s' % (version, rcNum, rev)
  s = os.popen('ssh %s@people.apache.org "ls -ld public_html/staging_area/%s" 2>&1' % (username, dir)).read()
  if 'no such file or directory' not in s.lower():
    print('  Remove old dir...')
    run('ssh %s@people.apache.org "chmod -R u+rwX public_html/staging_area/%s; rm -rf public_html/staging_area/%s"' % 
        (username, dir, dir))
  run('ssh %s@people.apache.org "mkdir -p public_html/staging_area/%s/lucene public_html/staging_area/%s/solr"' % \
      (username, dir, dir))
  print('  Lucene')
  os.chdir('%s/lucene/dist' % root)
  print('    zip...')
  if os.path.exists('lucene.tar.bz2'):
    os.remove('lucene.tar.bz2')
  run('tar cjf lucene.tar.bz2 *')
  print('    copy...')
  run('scp lucene.tar.bz2 %s@people.apache.org:public_html/staging_area/%s/lucene' % (username, dir))
  print('    unzip...')
  run('ssh %s@people.apache.org "cd public_html/staging_area/%s/lucene; tar xjf lucene.tar.bz2; rm -f lucene.tar.bz2"' % (username, dir))
  os.remove('lucene.tar.bz2')

  print('  Solr')
  os.chdir('%s/solr/package' % root)
  print('    zip...')
  if os.path.exists('solr.tar.bz2'):
    os.remove('solr.tar.bz2')
  run('tar cjf solr.tar.bz2 *')
  print('    copy...')
  run('scp solr.tar.bz2 %s@people.apache.org:public_html/staging_area/%s/solr' % (username, dir))
  print('    unzip...')
  run('ssh %s@people.apache.org "cd public_html/staging_area/%s/solr; tar xjf solr.tar.bz2; rm -f solr.tar.bz2"' % (username, dir))
  os.remove('solr.tar.bz2')

  print('  chmod...')
  run('ssh %s@people.apache.org "chmod -R a+rX-w public_html/staging_area/%s"' % (username, dir))

  print('  done!')
  url = 'https://people.apache.org/~%s/staging_area/%s' % (username, dir)
  return url

def pushLocal(version, root, rev, rcNum, localDir):
  print('Push local [%s]...' % localDir)
  os.makedirs(localDir)

  dir = 'lucene-solr-%s-RC%d-rev%s' % (version, rcNum, rev)
  os.makedirs('%s/%s/lucene' % (localDir, dir))
  os.makedirs('%s/%s/solr' % (localDir, dir))
  print('  Lucene')
  os.chdir('%s/lucene/dist' % root)
  print('    zip...')
  if os.path.exists('lucene.tar.bz2'):
    os.remove('lucene.tar.bz2')
  run('tar cjf lucene.tar.bz2 *')

  os.chdir('%s/%s/lucene' % (localDir, dir))
  print('    unzip...')
  run('tar xjf "%s/lucene/dist/lucene.tar.bz2"' % root)
  os.remove('%s/lucene/dist/lucene.tar.bz2' % root)
  print('    copy changes...')
  run('cp -r "%s/lucene/build/docs/changes" changes-%s' % (root, version))

  print('  Solr')
  os.chdir('%s/solr/package' % root)
  print('    zip...')
  if os.path.exists('solr.tar.bz2'):
    os.remove('solr.tar.bz2')
  run('tar cjf solr.tar.bz2 *')
  print('    unzip...')
  os.chdir('%s/%s/solr' % (localDir, dir))
  run('tar xjf "%s/solr/package/solr.tar.bz2"' % root)
  os.remove('%s/solr/package/solr.tar.bz2' % root)

  print('  KEYS')
  run('wget http://people.apache.org/keys/group/lucene.asc')
  os.rename('lucene.asc', 'KEYS')
  run('chmod a+r-w KEYS')
  run('cp KEYS ../lucene')

  print('  chmod...')
  os.chdir('..')
  run('chmod -R a+rX-w .')

  print('  done!')
  return 'file://%s/%s' % (os.path.abspath(localDir), dir)
  
def main():
  doPrepare = '-prepare' in sys.argv
  if doPrepare:
    sys.argv.remove('-prepare')

  try:
    idx = sys.argv.index('-push')
  except ValueError:
    doPushRemote = False
  else:
    doPushRemote = True
    username = sys.argv[idx+1]
    del sys.argv[idx:idx+2]

  try:
    idx = sys.argv.index('-smoke')
  except ValueError:
    smokeTmpDir = None
  else:
    smokeTmpDir = sys.argv[idx+1]
    del sys.argv[idx:idx+2]
    if os.path.exists(smokeTmpDir):
      print()
      print('ERROR: smoke tmpDir "%s" exists; please remove first' % smokeTmpDir)
      print()
      sys.exit(1)
    
  try:
    idx = sys.argv.index('-pushLocal')
  except ValueError:
    doPushLocal = False
  else:
    doPushLocal = True
    localStagingDir = sys.argv[idx+1]
    del sys.argv[idx:idx+2]
    if os.path.exists(localStagingDir):
      print()
      print('ERROR: pushLocal dir "%s" exists; please remove first' % localStagingDir)
      print()
      sys.exit(1)

  if doPushRemote and doPushLocal:
    print()
    print('ERROR: specify at most one of -push or -pushLocal (got both)')
    print()
    sys.exit(1)

  try:
    idx = sys.argv.index('-sign')
  except ValueError:
    gpgKeyID = None
  else:
    gpgKeyID = sys.argv[idx+1]
    del sys.argv[idx:idx+2]

    sys.stdout.flush()
    import getpass
    gpgPassword = getpass.getpass('Enter GPG keystore password: ')

  root = os.path.abspath(sys.argv[1])
  version = sys.argv[2]
  rcNum = int(sys.argv[3])

  if doPrepare:
    rev = prepare(root, version, gpgKeyID, gpgPassword, smokeTmpDir is None)
  else:
    os.chdir(root)
    rev = open('rev.txt', encoding='UTF-8').read()

  if doPushRemote:
    url = push(version, root, rev, rcNum, username)
  elif doPushLocal:
    url = pushLocal(version, root, rev, rcNum, localStagingDir)
  else:
    url = None

  if url is not None:
    print('  URL: %s' % url)

  if smokeTmpDir is not None:
    import smokeTestRelease
    smokeTestRelease.DEBUG = False
    smokeTestRelease.smokeTest(url, rev, version, smokeTmpDir, gpgKeyID is not None, '')

if __name__ == '__main__':
  try:
    main()
  except:
    import traceback
    traceback.print_exc()
