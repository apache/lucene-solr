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
import urllib.request, urllib.error, urllib.parse
import xml.etree.ElementTree as ET

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
  if result is not None:
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    print(msg)
    raise RuntimeError(msg)

def load(urlString):
  try:
    content = urllib.request.urlopen(urlString).read().decode('utf-8')
  except Exception as e:
    print('Retrying download of url %s after exception: %s' % (urlString, e))
    content = urllib.request.urlopen(urlString).read().decode('utf-8')
  return content

def getGitRev():
  status = os.popen('git status').read().strip()
  if 'nothing to commit, working directory clean' not in status and 'nothing to commit, working tree clean' not in status:
    raise RuntimeError('git clone is dirty:\n\n%s' % status)
  branch = os.popen('git rev-parse --abbrev-ref HEAD').read().strip()
  command = 'git log origin/%s..' % branch
  unpushedCommits = os.popen(command).read().strip()
  if len(unpushedCommits) > 0:
    raise RuntimeError('There are unpushed commits - "%s" output is:\n\n%s' % (command, unpushedCommits))

  print('  git clone is clean')
  return os.popen('git rev-parse HEAD').read().strip()

def prepare(root, version, gpgKeyID, gpgPassword):
  print()
  print('Prepare release...')
  if os.path.exists(LOG):
    os.remove(LOG)

  os.chdir(root)
  print('  git pull...')
  run('git pull')

  rev = getGitRev()
  print('  git rev: %s' % rev)
  log('\nGIT rev: %s\n' % rev)

  print('  Check DOAP files')
  checkDOAPfiles(version)

  print('  ant clean test validate documentation-lint')
  run('ant clean test validate documentation-lint')

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

reVersion1 = re.compile(r'\>(\d+)\.(\d+)\.(\d+)(-alpha|-beta)?/\<', re.IGNORECASE)
reVersion2 = re.compile(r'-(\d+)\.(\d+)\.(\d+)(-alpha|-beta)?\.zip<', re.IGNORECASE)
reDoapRevision = re.compile(r'(\d+)\.(\d+)(?:\.(\d+))?(-alpha|-beta)?', re.IGNORECASE)
def checkDOAPfiles(version):
  # In Lucene and Solr DOAP files, verify presence of all releases less than the one being produced.
  errorMessages = []
  for product in 'lucene', 'solr':
    url = 'https://archive.apache.org/dist/lucene/%s' % ('java' if product == 'lucene' else product)
    distpage = load(url)
    releases = set()
    for regex in reVersion1, reVersion2:
      for tup in regex.findall(distpage):
        if tup[0] in ('1', '2'):                    # Ignore 1.X and 2.X releases
          continue
        releases.add(normalizeVersion(tup))
    doapNS = '{http://usefulinc.com/ns/doap#}'
    xpathRevision = '{0}Project/{0}release/{0}Version/{0}revision'.format(doapNS)
    doapFile = "dev-tools/doap/%s.rdf" % product
    treeRoot = ET.parse(doapFile).getroot()
    doapRevisions = set()
    for revision in treeRoot.findall(xpathRevision):
      match = reDoapRevision.match(revision.text)
      if (match is not None):
        if (match.group(1) not in ('0', '1', '2')): # Ignore 0.X, 1.X and 2.X revisions
          doapRevisions.add(normalizeVersion(match.groups()))
      else:
        errorMessages.append('ERROR: Failed to parse revision: %s in %s' % (revision.text, doapFile))
    missingDoapRevisions = set()
    for release in releases:
      if release not in doapRevisions and release < version: # Ignore releases greater than the one being produced
        missingDoapRevisions.add(release)
    if len(missingDoapRevisions) > 0:
      errorMessages.append('ERROR: Missing revision(s) in %s: %s' % (doapFile, ', '.join(sorted(missingDoapRevisions))))
  if (len(errorMessages) > 0):
    raise RuntimeError('\n%s\n(Hint: copy/paste from the stable branch version of the file(s).)'
                       % '\n'.join(errorMessages))

def normalizeVersion(tup):
  suffix = ''
  if tup[-1] is not None and tup[-1].lower() == '-alpha':
    tup = tup[:(len(tup) - 1)]
    suffix = '-ALPHA'
  elif tup[-1] is not None and tup[-1].lower() == '-beta':
    tup = tup[:(len(tup) - 1)]
    suffix = '-BETA'
  while tup[-1] in ('', None):
    tup = tup[:(len(tup) - 1)]
  while len(tup) < 3:
    tup = tup + ('0',)
  return '.'.join(tup) + suffix

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
  run('wget http://home.apache.org/keys/group/lucene.asc')
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
    python3 -u dev-tools/scripts/buildAndPushRelease.py --push-local /tmp/releases/6.0.1 --sign 6E68DA61 --rc-num 1
  ''')
  description = 'Utility to build, push, and test a release.'
  parser = argparse.ArgumentParser(description=description, epilog=epilogue,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--no-prepare', dest='prepare', default=True, action='store_false',
                      help='Use the already built release in the provided checkout')
  parser.add_argument('--push-local', metavar='PATH',
                      help='Push the release to the local path')
  parser.add_argument('--sign', metavar='KEYID',
                      help='Sign the release with the given gpg key')
  parser.add_argument('--rc-num', metavar='NUM', type=int, default=1,
                      help='Release Candidate number.  Default: 1')
  parser.add_argument('--root', metavar='PATH', default='.',
                      help='Root of Git working tree for lucene-solr.  Default: "." (the current directory)')
  config = parser.parse_args()

  if not config.prepare and config.sign:
    parser.error('Cannot sign already built release')
  if config.push_local is not None and os.path.exists(config.push_local):
    parser.error('Cannot push to local path that already exists')
  if config.rc_num <= 0:
    parser.error('Release Candidate number must be a positive integer')
  if not os.path.isdir(config.root):
    parser.error('Root path "%s" is not a directory' % config.root)
  cwd = os.getcwd()
  os.chdir(config.root)
  config.root = os.getcwd() # Absolutize root dir
  if os.system('git rev-parse') or 3 != len([d for d in ('dev-tools','lucene','solr') if os.path.isdir(d)]):
    parser.error('Root path "%s" is not a valid lucene-solr checkout' % config.root)
  os.chdir(cwd)

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

def check_cmdline_tools():  # Fail fast if there are cmdline tool problems
  if os.system('git --version >/dev/null 2>/dev/null'):
    raise RuntimeError('"git --version" returned a non-zero exit code.')
  antVersion = os.popen('ant -version').read().strip()
  if not antVersion.startswith('Apache Ant(TM) version 1.8') and not antVersion.startswith('Apache Ant(TM) version 1.9'):
    raise RuntimeError('ant version is not 1.8.X: "%s"' % antVersion)
  
def main():
  check_cmdline_tools()

  c = parse_config()

  if c.prepare:
    rev = prepare(c.root, c.version, c.key_id, c.key_password)
  else:
    os.chdir(root)
    rev = open('rev.txt', encoding='UTF-8').read()

  if c.push_local:
    url = pushLocal(c.version, c.root, rev, c.rc_num, c.push_local)
  else:
    url = None

  if url is not None:
    print('  URL: %s' % url)
    print('Next run the smoker tester:')
    p = re.compile(".*/")
    m = p.match(sys.argv[0])
    print('%s -u %ssmokeTestRelease.py %s' % (sys.executable, m.group(), url))

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Keyboard interrupt...exiting')

