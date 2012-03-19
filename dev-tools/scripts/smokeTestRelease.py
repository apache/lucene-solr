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
from collections import defaultdict
import xml.etree.ElementTree as ET
import filecmp
import platform

# This tool expects to find /lucene and /solr off the base URL.  You
# must have a working gpg, tar, unzip in your path.  This has been
# tested on Linux and on Cygwin under Windows 7.

# http://s.apache.org/lusolr32rc2

JAVA5_HOME = '/usr/local/src/jdk1.5.0_22'
JAVA6_HOME = '/usr/local/src/jdk1.6.0_21'
JAVA7_HOME = '/usr/local/src/jdk1.7.0_01'

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

def download(name, urlString, tmpDir, quiet=False):
  fileName = '%s/%s' % (tmpDir, name)
  if DEBUG and os.path.exists(fileName):
    if not quiet and fileName.find('.asc') == -1:
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
  if not quiet and fileName.find('.asc') == -1:
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
    # Test sig (this is done with a clean brand-new GPG world)
    download(artifact + '.asc', urlString + '.asc', tmpDir)
    sigFile = '%s/%s.asc' % (tmpDir, artifact)
    artifactFile = '%s/%s' % (tmpDir, artifact)
    logFile = '%s/%s.%s.gpg.verify.log' % (tmpDir, project, artifact)
    run('gpg --homedir %s --verify %s %s' % (gpgHomeDir, sigFile, artifactFile),
        logFile)
    # Forward any GPG warnings, except the expected one (since its a clean world)
    f = open(logFile, 'rb')
    for line in f.readlines():
      if line.lower().find('warning') != -1 \
      and line.find('WARNING: This key is not certified with a trusted signature') == -1:
        print '      GPG: %s' % line.strip()
    f.close()

    # Test trust (this is done with the real users config)
    run('gpg --import %s' % (keysFile),
        '%s/%s.gpg.trust.import.log 2>&1' % (tmpDir, project))
    print '    verify trust'
    logFile = '%s/%s.%s.gpg.trust.log' % (tmpDir, project, artifact)
    run('gpg --verify %s %s' % (sigFile, artifactFile), logFile)
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
    logPath = os.path.abspath(logFile)
    raise RuntimeError('command "%s" failed; see log file %s' % (command, logPath))
    
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
    if text == 'Parent Directory' or text == '..':
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
      extras += ('build.xml', 'index.html', 'common-build.xml', 'core', 'backwards', 'test-framework', 'tools', 'site')
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
      # test javadocs
      print '    generate javadocs w/ Java 5...'
      run('export JAVA_HOME=%s; ant javadocs' % JAVA5_HOME, '%s/javadocs.log' % unpackPath)
    else:
      print '    run tests w/ Java 6...'
      run('export JAVA_HOME=%s; ant test' % JAVA6_HOME, '%s/test.log' % unpackPath)

      # test javadocs
      print '    generate javadocs w/ Java 6...'
      # uncomment this after 3.5.0 and delete the hack below
      # run('export JAVA_HOME=%s; ant javadocs' % JAVA6_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir('lucene')
      run('export JAVA_HOME=%s; ant javadocs' % JAVA6_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir(unpackPath)

      os.chdir('solr')
      run('export JAVA_HOME=%s; ant javadocs' % JAVA6_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir(unpackPath)
      # end hackidy-hack     

      print '    run tests w/ Java 7...'
      run('export JAVA_HOME=%s; ant test' % JAVA7_HOME, '%s/test.log' % unpackPath)
 
      # test javadocs
      print '    generate javadocs w/ Java 7...'
      # uncomment this after 3.5.0 and delete the hack below
      # run('export JAVA_HOME=%s; ant javadocs' % JAVA7_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir('lucene')
      run('export JAVA_HOME=%s; ant javadocs' % JAVA7_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir(unpackPath)

      os.chdir('solr')
      run('export JAVA_HOME=%s; ant javadocs' % JAVA7_HOME, '%s/javadocs.log' % unpackPath)
      os.chdir(unpackPath)
      # end hackidy-hack   
  else:
    if project == 'lucene':
      testDemo(isSrc, version)

  testChangesText('.', version, project)

def testDemo(isSrc, version):
  print '    test demo...'
  sep = ';' if platform.system().lower().startswith('cygwin') else ':'
  if isSrc:
    # allow lucene dev version to be either 3.3 or 3.3.0:
    if version.endswith('.0'):
      cp = 'build/core/lucene-core-{0}-SNAPSHOT.jar{1}build/contrib/demo/classes/java'.format(version, sep)
      cp += '{1}build/core/lucene-core-{0}-SNAPSHOT.jar{1}build/contrib/demo/classes/java'.format(version[:-2], sep)
    else:
      cp = 'build/core/lucene-core-{0}-SNAPSHOT.jar{1}build/contrib/demo/classes/java'.format(version, sep)
    docsDir = 'core/src'
  else:
    cp = 'lucene-core-{0}.jar{1}contrib/demo/lucene-demo-{0}.jar'.format(version, sep)
    docsDir = 'docs'
  run('export JAVA_HOME=%s; %s/bin/java -cp "%s" org.apache.lucene.demo.IndexFiles -index index -docs %s' % (JAVA5_HOME, JAVA5_HOME, cp, docsDir), 'index.log')
  run('export JAVA_HOME=%s; %s/bin/java -cp "%s" org.apache.lucene.demo.SearchFiles -index index -query lucene' % (JAVA5_HOME, JAVA5_HOME, cp), 'search.log')
  reMatchingDocs = re.compile('(\d+) total matching documents')
  m = reMatchingDocs.search(open('search.log', 'rb').read())
  if m is None:
    raise RuntimeError('lucene demo\'s SearchFiles found no results')
  else:
    numHits = int(m.group(1))
    if numHits < 100:
      raise RuntimeError('lucene demo\'s SearchFiles found too few results: %s' % numHits)
    print '      got %d hits for query "lucene"' % numHits

def checkMaven(baseURL, tmpDir, version):
  # Locate the release branch in subversion
  m = re.match('(\d+)\.(\d+)', version) # Get Major.minor version components
  releaseBranchText = 'lucene_solr_%s_%s/' % (m.group(1), m.group(2))
  branchesURL = 'http://svn.apache.org/repos/asf/lucene/dev/branches/'
  releaseBranchSvnURL = None
  branches = getDirEntries(branchesURL)
  for text, subURL in branches:
    if text == releaseBranchText:
      releaseBranchSvnURL = subURL
  if releaseBranchSvnURL is None:
    raise RuntimeError('Release branch %s%s not found' % (branchesURL, releaseBranchText))

  print '    download POM templates',
  POMtemplates = defaultdict()
  getPOMtemplates(POMtemplates, tmpDir, releaseBranchSvnURL)
  print
  print '    download artifacts',
  artifacts = {'lucene': [], 'solr': []}
  for project in ('lucene', 'solr'):
    artifactsURL = '%s/%s/maven/org/apache/%s' % (baseURL, project, project)
    targetDir = '%s/maven/org/apache/%s' % (tmpDir, project)
    if not os.path.exists(targetDir):
      os.makedirs(targetDir)
    crawl(artifacts[project], artifactsURL, targetDir)
  print
  print '    verify that each binary artifact has a deployed POM...'
  verifyPOMperBinaryArtifact(artifacts, version)
  print '    verify that there is an artifact for each POM template...'
  verifyArtifactPerPOMtemplate(POMtemplates, artifacts, tmpDir, version)
  print "    verify Maven artifacts' md5/sha1 digests..."
  verifyMavenDigests(artifacts)
  print '    verify that all non-Mavenized deps are deployed...'
  nonMavenizedDeps = dict()
  checkNonMavenizedDeps(nonMavenizedDeps, POMtemplates, artifacts, tmpDir,
                        version, releaseBranchSvnURL)
  print '    check for javadoc and sources artifacts...'
  checkJavadocAndSourceArtifacts(nonMavenizedDeps, artifacts, version)
  print "    verify deployed POMs' coordinates..."
  verifyDeployedPOMsCoordinates(artifacts, version)
  print '    verify maven artifact sigs',
  verifyMavenSigs(baseURL, tmpDir, artifacts)

  distributionFiles = getDistributionsForMavenChecks(tmpDir, version, baseURL)

  print '    verify that non-Mavenized deps are same as in the binary distribution...'
  checkIdenticalNonMavenizedDeps(distributionFiles, nonMavenizedDeps)
  print '    verify that Maven artifacts are same as in the binary distribution...'
  checkIdenticalMavenArtifacts(distributionFiles, nonMavenizedDeps, artifacts)

def getDistributionsForMavenChecks(tmpDir, version, baseURL):
  distributionFiles = defaultdict()
  for project in ('lucene', 'solr'):
    distribution = '%s-%s.tgz' % (project, version)
    if project == 'solr': distribution = 'apache-' + distribution
    if not os.path.exists('%s/%s' % (tmpDir, distribution)):
      distURL = '%s/%s/%s' % (baseURL, project, distribution)
      print '    download %s...' % distribution,
      download(distribution, distURL, tmpDir)
    destDir = '%s/unpack-%s-maven' % (tmpDir, project)
    if os.path.exists(destDir):
      shutil.rmtree(destDir)
    os.makedirs(destDir)
    os.chdir(destDir)
    print '    unpack %s...' % distribution
    unpackLogFile = '%s/unpack-%s-maven-checks.log' % (tmpDir, distribution)
    run('tar xzf %s/%s' % (tmpDir, distribution), unpackLogFile)
    if project == 'solr': # unpack the Solr war
      unpackLogFile = '%s/unpack-solr-war-maven-checks.log' % tmpDir
      print '        unpack Solr war...'
      run('jar xvf */dist/*.war', unpackLogFile)
    distributionFiles[project] = []
    for root, dirs, files in os.walk(destDir):
      distributionFiles[project].extend([os.path.join(root, file) for file in files])
  return distributionFiles

def checkJavadocAndSourceArtifacts(nonMavenizedDeps, artifacts, version):
  for project in ('lucene', 'solr'):
    for artifact in artifacts[project]:
      if artifact.endswith(version + '.jar') and artifact not in nonMavenizedDeps.keys():
        javadocJar = artifact[:-4] + '-javadoc.jar'
        if javadocJar not in artifacts[project]:
          raise RuntimeError('missing: %s' % javadocJar)
        sourcesJar = artifact[:-4] + '-sources.jar'
        if sourcesJar not in artifacts[project]:
          raise RuntimeError('missing: %s' % sourcesJar)

def checkIdenticalNonMavenizedDeps(distributionFiles, nonMavenizedDeps):
  for project in ('lucene', 'solr'):
    distFilenames = dict()
    for file in distributionFiles[project]:
      distFilenames[os.path.basename(file)] = file
    for dep in nonMavenizedDeps.keys():
      if ('/%s/' % project) in dep:
        depOrigFilename = os.path.basename(nonMavenizedDeps[dep])
        if not depOrigFilename in distFilenames:
          raise RuntimeError('missing: non-mavenized dependency %s' % nonMavenizedDeps[dep])
        identical = filecmp.cmp(dep, distFilenames[depOrigFilename], shallow=False)
        if not identical:
          raise RuntimeError('Deployed non-mavenized dep %s differs from distribution dep %s'
                            % (dep, distFilenames[depOrigFilename]))

def checkIdenticalMavenArtifacts(distributionFiles, nonMavenizedDeps, artifacts):
  reJarWar = re.compile(r'\.[wj]ar$')
  for project in ('lucene', 'solr'):
    distFilenames = dict()
    for file in distributionFiles[project]:
      distFilenames[os.path.basename(file)] = file
    for artifact in artifacts[project]:
      if reJarWar.search(artifact):
        if artifact not in nonMavenizedDeps.keys():
          artifactFilename = os.path.basename(artifact)
          if artifactFilename not in distFilenames.keys():
            raise RuntimeError('Maven artifact %s is not present in %s binary distribution'
                              % (artifact, project))
          identical = filecmp.cmp(artifact, distFilenames[artifactFilename], shallow=False)
          if not identical:
            raise RuntimeError('Maven artifact %s is not identical to %s in %s binary distribution'
                              % (artifact, distFilenames[artifactFilename], project))

def verifyMavenDigests(artifacts):
  reJarWarPom = re.compile(r'\.(?:[wj]ar|pom)$')
  for project in ('lucene', 'solr'):
    for artifactFile in [a for a in artifacts[project] if reJarWarPom.search(a)]:
      if artifactFile + '.md5' not in artifacts[project]:
        raise RuntimeError('missing: MD5 digest for %s' % artifactFile)
      if artifactFile + '.sha1' not in artifacts[project]:
        raise RuntimeError('missing: SHA1 digest for %s' % artifactFile)
      with open(artifactFile + '.md5', 'r') as md5File:
        md5Expected = md5File.read().strip()
      with open(artifactFile + '.sha1', 'r') as sha1File:
        sha1Expected = sha1File.read().strip()
      md5 = hashlib.md5()
      sha1 = hashlib.sha1()
      inputFile = open(artifactFile)
      while True:
        bytes = inputFile.read(65536)
        if bytes == '': break
        md5.update(bytes)
        sha1.update(bytes)
      inputFile.close()
      md5Actual = md5.hexdigest()
      sha1Actual = sha1.hexdigest()
      if md5Actual != md5Expected:
        raise RuntimeError('MD5 digest mismatch for %s: expected %s but got %s'
                           % (artifactFile, md5Expected, md5Actual))
      if sha1Actual != sha1Expected:
        raise RuntimeError('SHA1 digest mismatch for %s: expected %s but got %s'
                           % (artifactFile, sha1Expected, sha1Actual))

def checkNonMavenizedDeps(nonMavenizedDependencies, POMtemplates, artifacts,
                          tmpDir, version, releaseBranchSvnURL):
  """
  - check for non-mavenized dependencies listed in the grandfather POM template
  - nonMavenizedDependencies is populated with a map from non-mavenized dependency
    artifact path to the original jar path
  """
  namespace = '{http://maven.apache.org/POM/4.0.0}'
  xpathProfile = '{0}profiles/{0}profile'.format(namespace)
  xpathPlugin = '{0}build/{0}plugins/{0}plugin'.format(namespace)
  xpathExecution= '{0}executions/{0}execution'.format(namespace)
  xpathResourceDir = '{0}configuration/{0}resources/{0}resource/{0}directory'.format(namespace)

  treeRoot = ET.parse(POMtemplates['grandfather'][0]).getroot()
  for profile in treeRoot.findall(xpathProfile):
    pomDirs = []
    profileId = profile.find('%sid' % namespace)
    if profileId is not None and profileId.text == 'bootstrap':
      plugins = profile.findall(xpathPlugin)
      for plugin in plugins:
        artifactId = plugin.find('%sartifactId' % namespace).text.strip()
        if artifactId == 'maven-resources-plugin':
          for config in plugin.findall(xpathExecution):
            pomDirs.append(config.find(xpathResourceDir).text.strip())
      for plugin in plugins:
        artifactId = plugin.find('%sartifactId' % namespace).text.strip()
        if artifactId == 'maven-install-plugin':
          for execution in plugin.findall(xpathExecution):
            groupId, artifactId, file, pomFile = '', '', '', ''
            for child in execution.find('%sconfiguration' % namespace).getchildren():
              text = child.text.strip()
              if child.tag == '%sgroupId' % namespace:
                groupId = text if text != '${project.groupId}' else 'org.apache.lucene'
              elif child.tag == '%sartifactId' % namespace: artifactId = text
              elif child.tag == '%sfile' % namespace: file = text
              elif child.tag == '%spomFile' % namespace: pomFile = text
            if groupId in ('org.apache.lucene', 'org.apache.solr'):
              depJar = '%s/maven/%s/%s/%s/%s-%s.jar'    \
                     % (tmpDir, groupId.replace('.', '/'),
                        artifactId, version, artifactId, version)
              if depJar not in artifacts['lucene']  \
                  and depJar not in artifacts['solr']:
                raise RuntimeError('Missing non-mavenized dependency %s' % depJar)
              nonMavenizedDependencies[depJar] = file
            elif pomFile: # Find non-Mavenized deps with associated POMs
              pomFile = pomFile.split('/')[-1] # remove path
              for pomDir in pomDirs:
                entries = getDirEntries('%s/%s' % (releaseBranchSvnURL, pomDir))
                for text, subURL in entries:
                  if text == pomFile:
                    doc2 = ET.XML(load(subURL))
                    groupId2, artifactId2, packaging2, POMversion = getPOMcoordinate(doc2)
                    depJar = '%s/maven/%s/%s/%s/%s-%s.jar' \
                        % (tmpDir, groupId2.replace('.', '/'),
                           artifactId2, version, artifactId2, version)
                    if depJar not in artifacts['lucene']   \
                        and depJar not in artifacts['solr']:
                      raise RuntimeError('Missing non-mavenized dependency %s' % depJar)
                    nonMavenizedDependencies[depJar] = file
                    break

def getPOMcoordinate(treeRoot):
  namespace = '{http://maven.apache.org/POM/4.0.0}'
  groupId = treeRoot.find('%sgroupId' % namespace)
  if groupId is None:
    groupId = treeRoot.find('{0}parent/{0}groupId'.format(namespace))
  groupId = groupId.text.strip()
  artifactId = treeRoot.find('%sartifactId' % namespace).text.strip()
  version = treeRoot.find('%sversion' % namespace)
  if version is None:
    version = treeRoot.find('{0}parent/{0}version'.format(namespace))
  version = version.text.strip()
  packaging = treeRoot.find('%spackaging' % namespace)
  packaging = 'jar' if packaging is None else packaging.text.strip()
  return groupId, artifactId, packaging, version

def verifyMavenSigs(baseURL, tmpDir, artifacts):
  """Verify Maven artifact signatures"""
  for project in ('lucene', 'solr'):
    keysFile = '%s/%s.KEYS' % (tmpDir, project)
    if not os.path.exists(keysFile):
      keysURL = '%s/%s/KEYS' % (baseURL, project)
      download('%s.KEYS' % project, keysURL, tmpDir, quiet=True)

    # Set up clean gpg world; import keys file:
    gpgHomeDir = '%s/%s.gpg' % (tmpDir, project)
    if os.path.exists(gpgHomeDir):
      shutil.rmtree(gpgHomeDir)
    os.makedirs(gpgHomeDir, 0700)
    run('gpg --homedir %s --import %s' % (gpgHomeDir, keysFile),
        '%s/%s.gpg.import.log' % (tmpDir, project))

    reArtifacts = re.compile(r'\.(?:pom|[jw]ar)$')
    for artifactFile in [a for a in artifacts[project] if reArtifacts.search(a)]:
      artifact = os.path.basename(artifactFile)
      sigFile = '%s.asc' % artifactFile
      # Test sig (this is done with a clean brand-new GPG world)
      logFile = '%s/%s.%s.gpg.verify.log' % (tmpDir, project, artifact)
      run('gpg --homedir %s --verify %s %s' % (gpgHomeDir, sigFile, artifactFile),
          logFile)
      # Forward any GPG warnings, except the expected one (since its a clean world)
      f = open(logFile, 'rb')
      for line in f.readlines():
        if line.lower().find('warning') != -1 \
           and line.find('WARNING: This key is not certified with a trusted signature') == -1 \
           and line.find('WARNING: using insecure memory') == -1:
          print '      GPG: %s' % line.strip()
      f.close()

      # Test trust (this is done with the real users config)
      run('gpg --import %s' % keysFile,
          '%s/%s.gpg.trust.import.log' % (tmpDir, project))
      logFile = '%s/%s.%s.gpg.trust.log' % (tmpDir, project, artifact)
      run('gpg --verify %s %s' % (sigFile, artifactFile), logFile)
      # Forward any GPG warnings:
      f = open(logFile, 'rb')
      for line in f.readlines():
        if line.lower().find('warning') != -1 \
           and line.find('WARNING: This key is not certified with a trusted signature') == -1 \
           and line.find('WARNING: using insecure memory') == -1:
          print '      GPG: %s' % line.strip()
      f.close()

      sys.stdout.write('.')
  print

def verifyPOMperBinaryArtifact(artifacts, version):
  """verify that each binary jar and war has a corresponding POM file"""
  reBinaryJarWar = re.compile(r'%s\.[jw]ar$' % re.escape(version))
  for project in ('lucene', 'solr'):
    for artifact in [a for a in artifacts[project] if reBinaryJarWar.search(a)]:
      POM = artifact[:-4] + '.pom'
      if POM not in artifacts[project]:
        raise RuntimeError('missing: POM for %s' % artifact)

def verifyDeployedPOMsCoordinates(artifacts, version):
  """
  verify that each POM's coordinate (drawn from its content) matches
  its filepath, and verify that the corresponding artifact exists.
  """
  for project in ('lucene', 'solr'):
    for POM in [a for a in artifacts[project] if a.endswith('.pom')]:
      treeRoot = ET.parse(POM).getroot()
      groupId, artifactId, packaging, POMversion = getPOMcoordinate(treeRoot)
      POMpath = '%s/%s/%s/%s-%s.pom' \
              % (groupId.replace('.', '/'), artifactId, version, artifactId, version)
      if not POM.endswith(POMpath):
        raise RuntimeError("Mismatch between POM coordinate %s:%s:%s and filepath: %s"
                          % (groupId, artifactId, POMversion, POM))
      # Verify that the corresponding artifact exists
      artifact = POM[:-3] + packaging
      if artifact not in artifacts[project]:
        raise RuntimeError('Missing corresponding .%s artifact for POM %s' % (packaging, POM))

def verifyArtifactPerPOMtemplate(POMtemplates, artifacts, tmpDir, version):
  """verify that each POM template's artifact is present in artifacts"""
  namespace = '{http://maven.apache.org/POM/4.0.0}'
  xpathPlugin = '{0}build/{0}plugins/{0}plugin'.format(namespace)
  xpathSkipConfiguration = '{0}configuration/{0}skip'.format(namespace)
  for project in ('lucene', 'solr'):
    for POMtemplate in POMtemplates[project]:
      treeRoot = ET.parse(POMtemplate).getroot()
      skipDeploy = False
      for plugin in treeRoot.findall(xpathPlugin):
        artifactId = plugin.find('%sartifactId' % namespace).text.strip()
        if artifactId == 'maven-deploy-plugin':
          skip = plugin.find(xpathSkipConfiguration)
          if skip is not None: skipDeploy = (skip.text.strip().lower() == 'true')
      if not skipDeploy:
        groupId, artifactId, packaging, POMversion = getPOMcoordinate(treeRoot)
        # Ignore POMversion, since its value will not have been interpolated
        artifact = '%s/maven/%s/%s/%s/%s-%s.%s' \
                 % (tmpDir, groupId.replace('.', '/'), artifactId,
                    version, artifactId, version, packaging)
        if artifact not in artifacts['lucene'] and artifact not in artifacts['solr']:
          raise RuntimeError('Missing artifact %s' % artifact)

def getPOMtemplates(POMtemplates, tmpDir, releaseBranchSvnURL):
  releaseBranchSvnURL += 'dev-tools/maven/'
  targetDir = '%s/dev-tools/maven' % tmpDir
  if not os.path.exists(targetDir):
    os.makedirs(targetDir)
  allPOMtemplates = []
  crawl(allPOMtemplates, releaseBranchSvnURL, targetDir, set(['Apache Subversion'])) # Ignore "Apache Subversion" links
  POMtemplates['lucene'] = [p for p in allPOMtemplates if '/lucene/' in p]
  if POMtemplates['lucene'] is None:
    raise RuntimeError('No Lucene POMs found at %s' % releaseBranchSvnURL)
  POMtemplates['solr'] = [p for p in allPOMtemplates if '/solr/' in p]
  if POMtemplates['solr'] is None:
    raise RuntimeError('No Solr POMs found at %s' % releaseBranchSvnURL)
  POMtemplates['grandfather'] = [p for p in allPOMtemplates if '/maven/pom.xml.template' in p]
  if POMtemplates['grandfather'] is None:
    raise RuntimeError('No Lucene/Solr grandfather POM found at %s' % releaseBranchSvnURL)

def crawl(downloadedFiles, urlString, targetDir, exclusions=set()):
  for text, subURL in getDirEntries(urlString):
    if text not in exclusions:
      path = os.path.join(targetDir, text)
      if text.endswith('/'):
        if not os.path.exists(path):
          os.makedirs(path)
        crawl(downloadedFiles, subURL, path, exclusions)
      else:
        if not os.path.exists(path) or not DEBUG:
          download(text, subURL, targetDir, quiet=True)
        downloadedFiles.append(path)
        sys.stdout.write('.')

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

  print 'Test Maven artifacts for Lucene and Solr...'
  checkMaven(baseURL, tmpDir, version)

if __name__ == '__main__':
  main()
  
