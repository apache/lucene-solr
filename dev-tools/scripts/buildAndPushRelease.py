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
import datetime
import re
import time
import shutil
import os
import sys
import subprocess
import textwrap

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

def getGitRev():
  status = os.popen('git status').read().strip()
  if 'nothing to commit, working directory clean' not in status:
    raise RuntimeError('git clone is dirty:\n\n%s' % status)

  # TODO: we should also detect unpushed changes here?  Something like "git cherry -v origin/branch_5_5"?
  print('  git clone is clean')
  return os.popen('git rev-parse HEAD').read().strip()

def prepare(root, version, gpgKeyID, gpgPassword):
  print()
  print('Prepare release...')
  if os.path.exists(LOG):
    os.remove(LOG)

  os.chdir(root)
  print('  svn up...')
  run('svn up')

  rev = getGitRev()
  print('  git rev: %s' % rev)
  log('\nGIT rev: %s\n' % rev)

  print('  ant clean test')
  run('ant clean test')

  open('rev.txt', mode='wb').write(rev.encode('UTF-8'))
  
  print('  lucene prepare-release')
  os.chdir('lucene')
  cmd = 'ant -Dversion=%s' % version
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
  cmd = 'ant -Dversion=%s' % version
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
  url = 'http://people.apache.org/~%s/staging_area/%s' % (username, dir)
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

def read_version(path):
  version_props_file = os.path.join(path, 'lucene', 'version.properties')
  return re.search(r'version\.base=(.*)', open(version_props_file).read()).group(1)

def parse_config():
  epilogue = textwrap.dedent('''
    Example usage for a Release Manager:
    python3.2 -u buildAndPushRelease.py --push-remote mikemccand --sign 6E68DA61 --rc-num 1 /path/to/lucene_solr_4_7
  ''')
  description = 'Utility to build, push, and test a release.'
  parser = argparse.ArgumentParser(description=description, epilog=epilogue,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--no-prepare', dest='prepare', default=True, action='store_false',
                      help='Use the already built release in the provided checkout')
  parser.add_argument('--push-remote', metavar='USERNAME',
                      help='Push the release to people.apache.org for the given user')
  parser.add_argument('--push-local', metavar='PATH',
                      help='Push the release to the local path')
  parser.add_argument('--sign', metavar='KEYID',
                      help='Sign the release with the given gpg key')
  parser.add_argument('--rc-num', metavar='NUM', type=int, default=1,
                      help='Release Candidate number, required')
  parser.add_argument('--smoke-test', metavar='PATH', 
                      help='Run the smoker tester on the release in the given directory')
  parser.add_argument('root', metavar='checkout_path',
                      help='Root of SVN checkout for lucene-solr')
  config = parser.parse_args()

  if config.push_remote is not None and config.push_local is not None:
    parser.error('Cannot specify --push-remote and --push-local together')
  if not config.prepare and config.sign:
    parser.error('Cannot sign already built release')
  if config.push_local is not None and os.path.exists(config.push_local):
    parser.error('Cannot push to local path that already exists')
  if config.rc_num <= 0:
    parser.error('Release Candidate number must be a positive integer')
  if not os.path.isdir(config.root):
    # TODO: add additional svn check to ensure dir is a real lucene-solr checkout
    parser.error('Root path is not a valid lucene-solr checkout')

  config.version = read_version(config.root)
  print('Building version: %s' % config.version)

  if config.sign:
    sys.stdout.flush()
    import getpass
    config.key_id = config.sign
    config.key_password = getpass.getpass('Enter GPG keystore password: ')
  else:
    config.gpg_password = None

  return config
  
def main():
  c = parse_config()

  if c.prepare:
    rev = prepare(c.root, c.version, c.key_id, c.key_password)
  else:
    os.chdir(root)
    rev = open('rev.txt', encoding='UTF-8').read()

  if c.push_remote:
    url = push(c.version, c.root, rev, c.rc_num, c.push_remote)
  elif c.push_local:
    url = pushLocal(c.version, c.root, rev, c.rc_num, c.push_local)
  else:
    url = None

  if url is not None:
    print('  URL: %s' % url)
    print('Next set the PYTHON_EXEC env var and you can run the smoker tester:')
    p = re.compile("(.*)\/")
    m = p.match(sys.argv[0])
    print(' $PYTHON_EXEC %ssmokeTestRelease.py %s' % (m.group(), url))

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Keyboard interrupt...exiting')

