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
import shutil
import hashlib
import httplib
import re
import urllib2
import urlparse
import sys
import HTMLParser

# This tool expects to find /lucene and /solr off the base URL.  You
# must have a working gpg, tar, unzip in your path.  This has only
# been tested on Linux so far!

# http://s.apache.org/lusolr32rc2

JAVA5_HOME = '/usr/local/src/jdk1.5.0_22'
JAVA6_HOME = '/usr/local/src/jdk1.6.0_21'

# TODO
#   + verify KEYS contains key that signed the release
#   + make sure changes HTML looks ok
#   - verify license/notice of all dep jars
#   - check maven
#   - check JAR manifest version
#   - check license/notice exist
#   - check no "extra" files
#   - make sure jars exist inside bin release
#   - run "ant test"
#   - make sure docs exist
#   - use java5 for lucene/modules

reHREF = re.compile('<a href="(.*?)">(.*?)</a>')

# Set to True to avoid re-downloading the packages...
DEBUG = False

def getHREFs(urlString):

  # Deref any redirects
  while True:
    url = urlparse.urlparse(urlString)
    h = httplib.HTTPConnection(url.netloc)
    h.request('GET', url.path)
    r = h.getresponse()
    newLoc = r.getheader('location')
    if newLoc is not None:
      urlString = newLoc
    else:
      break

  links = []
  for subUrl, text in reHREF.findall(urllib2.urlopen(urlString).read()):
    fullURL = urlparse.urljoin(urlString, subUrl)
    links.append((text, fullURL))
  return links

def download(name, urlString, tmpDir):
  fileName = '%s/%s' % (tmpDir, name)
  if DEBUG and os.path.exists(fileName):
    if fileName.find('.asc') == -1:
      print '    already done: %.1f MB' % (os.path.getsize(fileName)/1024./1024.)
    return
  fIn = urllib2.urlopen(urlString)
  fOut = open(fileName, 'wb')
  success = False
  try:
    while True:
      s = fIn.read(65536)
      if s == '':
        break
      fOut.write(s)
    fOut.close()
    fIn.close()
    success = True
  finally:
    fIn.close()
    fOut.close()
    if not success:
      os.remove(fileName)
  if fileName.find('.asc') == -1:
    print '    %.1f MB' % (os.path.getsize(fileName)/1024./1024.)
    
def load(urlString):
  return urllib2.urlopen(urlString).read()
  
def checkSigs(project, urlString, version, tmpDir):

  print '  test basics...'
  ents = getDirEntries(urlString)
  artifact = None
  keysURL = None
  changesURL = None
  mavenURL = None
  expectedSigs = ['asc', 'md5', 'sha1']
  artifacts = []
  for text, subURL in ents:
    if text == 'KEYS':
      keysURL = subURL
    elif text == 'maven/':
      mavenURL = subURL
    elif text.startswith('changes'):
      if text not in ('changes/', 'changes-%s/' % version):
        raise RuntimeError('%s: found %s vs expected changes-%s/' % (project, text, version))
      changesURL = subURL
    elif artifact == None:
      artifact = text
      artifactURL = subURL
      if project == 'solr':
        expected = 'apache-solr-%s' % version
      else:
        expected = 'lucene-%s' % version
      if not artifact.startswith(expected):
        raise RuntimeError('%s: unknown artifact %s: expected prefix %s' % (project, text, expected))
      sigs = []
    elif text.startswith(artifact + '.'):
      sigs.append(text[len(artifact)+1:])
    else:
      if sigs != expectedSigs:
        raise RuntimeError('%s: artifact %s has wrong sigs: expected %s but got %s' % (project, artifact, expectedSigs, sigs))
      artifacts.append((artifact, artifactURL))
      artifact = text
      artifactURL = subURL
      sigs = []

  if sigs != []:
    artifacts.append((artifact, artifactURL))
    if sigs != expectedSigs:
      raise RuntimeError('%s: artifact %s has wrong sigs: expected %s but got %s' % (project, artifact, expectedSigs, sigs))

  if project == 'lucene':
    expected = ['lucene-%s-src.tgz' % version,
                'lucene-%s.tgz' % version,
                'lucene-%s.zip' % version]
  else:
    expected = ['apache-solr-%s-src.tgz' % version,
                'apache-solr-%s.tgz' % version,
                'apache-solr-%s.zip' % version]

  actual = [x[0] for x in artifacts]
  if expected != actual:
    raise RuntimeError('%s: wrong artifacts: expected %s but got %s' % (project, expected, actual))
                
  if keysURL is None:
    raise RuntimeError('%s is missing KEYS' % project)

  download('%s.KEYS' % project, keysURL, tmpDir)

  keysFile = '%s/%s.KEYS' % (tmpDir, project)

  # Set up clean gpg world; import keys file:
  gpgHomeDir = '%s/%s.gpg' % (tmpDir, project)
  if os.path.exists(gpgHomeDir):
    shutil.rmtree(gpgHomeDir)
  os.makedirs(gpgHomeDir, 0700)
  run('gpg --homedir %s --import %s' % (gpgHomeDir, keysFile),
      '%s/%s.gpg.import.log 2>&1' % (tmpDir, project))

  if mavenURL is None:
    raise RuntimeError('%s is missing maven' % project)

  if project == 'lucene':
    if changesURL is None:
      raise RuntimeError('%s is missing changes-%s' % (project, version))
    testChanges(project, version, changesURL)

  for artifact, urlString in artifacts:
    print '  download %s...' % artifact
    download(artifact, urlString, tmpDir)
    verifyDigests(artifact, urlString, tmpDir)

    print '    verify sig'
    # Test sig
    download(artifact + '.asc', urlString + '.asc', tmpDir)
    sigFile = '%s/%s.asc' % (tmpDir, artifact)
    artifactFile = '%s/%s' % (tmpDir, artifact)
    logFile = '%s/%s.%s.gpg.verify.log' % (tmpDir, project, artifact)
    run('gpg --homedir %s --verify %s %s' % (gpgHomeDir, sigFile, artifactFile),
        logFile)
    # Forward any GPG warnings:
    f = open(logFile, 'rb')
    for line in f.readlines():
      if line.lower().find('warning') != -1:
        print '      GPG: %s' % line.strip()
    f.close()

def testChanges(project, version, changesURLString):
  print '  check changes HTML...'
  changesURL = None
  contribChangesURL = None
  for text, subURL in getDirEntries(changesURLString):
    if text == 'Changes.html':
      changesURL = subURL
    elif text == 'Contrib-Changes.html':
      contribChangesURL = subURL

  if changesURL is None:
    raise RuntimeError('did not see Changes.html link from %s' % changesURLString)
  if contribChangesURL is None:
    raise RuntimeError('did not see Contrib-Changes.html link from %s' % changesURLString)

  s = load(changesURL)
  checkChangesContent(s, version, changesURL, project, True)

def testChangesText(dir, version, project):
  "Checks all CHANGES.txt under this dir."
  for root, dirs, files in os.walk(dir):

    # NOTE: O(N) but N should be smallish:
    if 'CHANGES.txt' in files:
      fullPath = '%s/CHANGES.txt' % root
      print 'CHECK %s' % fullPath
      checkChangesContent(open(fullPath).read(), version, fullPath, project, False)
      
def checkChangesContent(s, version, name, project, isHTML):

  if isHTML and s.find('Release %s' % version) == -1:
    raise RuntimeError('did not see "Release %s" in %s' % (version, name))

  if s.lower().find('not yet released') != -1:
    raise RuntimeError('saw "not yet released" in %s' % name)

  if not isHTML:
    if project == 'lucene':
      sub = 'Lucene %s' % version
    else:
      sub = version
      
    if s.find(sub) == -1:
      # contrib/benchmark never seems to include release info:
      if name.find('/benchmark/') == -1:
        raise RuntimeError('did not see "%s" in %s' % (sub, name))
  
def run(command, logFile):
  if os.system('%s > %s 2>&1' % (command, logFile)):
    raise RuntimeError('command "%s" failed; see log file %s/%s' % (command, os.getcwd(), logFile))
    
def verifyDigests(artifact, urlString, tmpDir):
  print '    verify md5/sha1 digests'
  md5Expected, t = load(urlString + '.md5').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('MD5 %s.md5 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  sha1Expected, t = load(urlString + '.sha1').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('SHA1 %s.sha1 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  m = hashlib.md5()
  s = hashlib.sha1()
  f = open('%s/%s' % (tmpDir, artifact))
  while True:
    x = f.read(65536)
    if x == '':
      break
    m.update(x)
    s.update(x)
  f.close()
  md5Actual = m.hexdigest()
  sha1Actual = s.hexdigest()
  if md5Actual != md5Expected:
    raise RuntimeError('MD5 digest mismatch for %s: expected %s but got %s' % (artifact, md5Expected, md5Actual))
  if sha1Actual != sha1Expected:
    raise RuntimeError('SHA1 digest mismatch for %s: expected %s but got %s' % (artifact, sha1Expected, sha1Actual))
  
def getDirEntries(urlString):
  links = getHREFs(urlString)
  for i, (text, subURL) in enumerate(links):
    if text == 'Parent Directory':
      return links[(i+1):]

def unpack(project, tmpDir, artifact, version):
  destDir = '%s/unpack' % tmpDir
  if os.path.exists(destDir):
    shutil.rmtree(destDir)
  os.makedirs(destDir)
  os.chdir(destDir)
  print '    unpack %s...' % artifact
  unpackLogFile = '%s/%s-unpack-%s.log' % (tmpDir, project, artifact)
  if artifact.endswith('.tar.gz') or artifact.endswith('.tgz'):
    run('tar xzf %s/%s' % (tmpDir, artifact), unpackLogFile)
  elif artifact.endswith('.zip'):
    run('unzip %s/%s' % (tmpDir, artifact), unpackLogFile)

  # make sure it unpacks to proper subdir
  l = os.listdir(destDir)
  if project == 'solr':
    expected = 'apache-%s-%s' % (project, version)
  else:
    expected = '%s-%s' % (project, version)
  if l != [expected]:
    raise RuntimeError('unpack produced entries %s; expected only %s' % (l, expected))

  unpackPath = '%s/%s' % (destDir, expected)
  verifyUnpacked(project, artifact, unpackPath, version)

def verifyUnpacked(project, artifact, unpackPath, version):
  os.chdir(unpackPath)
  isSrc = artifact.find('-src') != -1
  l = os.listdir(unpackPath)
  textFiles = ['LICENSE', 'NOTICE', 'README']
  if project == 'lucene':
    textFiles.extend(('JRE_VERSION_MIGRATION', 'CHANGES'))
    if isSrc:
      textFiles.append('BUILD')
  for fileName in textFiles:
    fileName += '.txt'
    if fileName not in l:
      raise RuntimeError('file "%s" is missing from artifact %s' % (fileName, artifact))
    l.remove(fileName)

  if not isSrc:
    if project == 'lucene':
      expectedJARs = ('lucene-core-%s' % version,
                      'lucene-core-%s-javadoc' % version,
                      'lucene-test-framework-%s' % version,
                      'lucene-test-framework-%s-javadoc' % version)
    else:
      expectedJARs = ()

    for fileName in expectedJARs:
      fileName += '.jar'
      if fileName not in l:
        raise RuntimeError('%s: file "%s" is missing from artifact %s' % (project, fileName, artifact))
      l.remove(fileName)

  if project == 'lucene':
    extras = ('lib', 'docs', 'contrib')
    if isSrc:
      extras += ('build.xml', 'index.html', 'common-build.xml', 'src', 'backwards')
  else:
    extras = ()

  for e in extras:
    if e not in l:
      raise RuntimeError('%s: %s missing from artifact %s' % (project, e, artifact))
    l.remove(e)

  if project == 'lucene':
    if len(l) > 0:
      raise RuntimeError('%s: unexpected files/dirs in artifact %s: %s' % (project, artifact, l))

  if isSrc:
    if project == 'lucene':
      print '    run tests w/ Java 5...'
      run('export JAVA_HOME=%s; ant test' % JAVA5_HOME, '%s/test.log' % unpackPath)
      run('export JAVA_HOME=%s; ant jar' % JAVA5_HOME, '%s/compile.log' % unpackPath)
      testDemo(isSrc, version)
    else:
      print '    run tests w/ Java 6...'
      run('export JAVA_HOME=%s; ant test' % JAVA6_HOME, '%s/test.log' % unpackPath)
  else:
    if project == 'lucene':
      testDemo(isSrc, version)

  testChangesText('.', version, project)

def testDemo(isSrc, version):
  print '    test demo...'
  if isSrc:
    # allow lucene dev version to be either 3.3 or 3.3.0:
    if version.endswith('.0'):
      cp = 'build/lucene-core-%s-SNAPSHOT.jar:build/contrib/demo/lucene-demo-%s-SNAPSHOT.jar' % (version, version)
      cp += ':build/lucene-core-%s-SNAPSHOT.jar:build/contrib/demo/lucene-demo-%s-SNAPSHOT.jar' % (version[:-2], version[:-2])
    else:
      cp = 'build/lucene-core-%s-SNAPSHOT.jar:build/contrib/demo/lucene-demo-%s-SNAPSHOT.jar' % (version, version)
    docsDir = 'src'
  else:
    cp = 'lucene-core-%s.jar:contrib/demo/lucene-demo-%s.jar' % (version, version)
    docsDir = 'docs'
  run('export JAVA_HOME=%s; %s/bin/java -cp %s org.apache.lucene.demo.IndexFiles -index index -docs %s' % (JAVA5_HOME, JAVA5_HOME, cp, docsDir), 'index.log')
  run('export JAVA_HOME=%s; %s/bin/java -cp %s org.apache.lucene.demo.SearchFiles -index index -query lucene' % (JAVA5_HOME, JAVA5_HOME, cp), 'search.log')
  reMatchingDocs = re.compile('(\d+) total matching documents')
  m = reMatchingDocs.search(open('search.log', 'rb').read())
  if m is None:
    raise RuntimeError('lucene demo\'s SearchFiles found no results')
  else:
    numHits = int(m.group(1))
    if numHits < 100:
      raise RuntimeError('lucene demo\'s SearchFiles found too few results: %s' % numHits)
    print '      got %d hits for query "lucene"' % numHits
        
def main():

  if len(sys.argv) != 4:
    print
    print 'Usage python -u %s BaseURL version tmpDir' % sys.argv[0]
    print
    sys.exit(1)

  baseURL = sys.argv[1]
  version = sys.argv[2]
  tmpDir = os.path.abspath(sys.argv[3])

  if not DEBUG:
    if os.path.exists(tmpDir):
      raise RuntimeError('temp dir %s exists; please remove first' % tmpDir)
    os.makedirs(tmpDir)
  
  lucenePath = None
  solrPath = None
  print 'Load release URL...'
  for text, subURL in getDirEntries(baseURL):
    if text.lower().find('lucene') != -1:
      lucenePath = subURL
    elif text.lower().find('solr') != -1:
      solrPath = subURL

  if lucenePath is None:
    raise RuntimeError('could not find lucene subdir')
  if solrPath is None:
    raise RuntimeError('could not find solr subdir')

  print
  print 'Test Lucene...'
  checkSigs('lucene', lucenePath, version, tmpDir)
  for artifact in ('lucene-%s.tgz' % version, 'lucene-%s.zip' % version):
    unpack('lucene', tmpDir, artifact, version)
  unpack('lucene', tmpDir, 'lucene-%s-src.tgz' % version, version)

  print
  print 'Test Solr...'
  checkSigs('solr', solrPath, version, tmpDir)
  for artifact in ('apache-solr-%s.tgz' % version, 'apache-solr-%s.zip' % version):
    unpack('solr', tmpDir, artifact, version)
  unpack('solr', tmpDir, 'apache-solr-%s-src.tgz' % version, version)

if __name__ == '__main__':
  main()
  
