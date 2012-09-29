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
import codecs
import tarfile
import zipfile
import threading
import traceback
import subprocess
import signal
import shutil
import hashlib
import http.client
import re
import urllib.request, urllib.error, urllib.parse
import urllib.parse
import sys
import html.parser
from collections import defaultdict
import xml.etree.ElementTree as ET
import filecmp
import platform
import checkJavaDocs
import checkJavadocLinks
import io
import codecs

# This tool expects to find /lucene and /solr off the base URL.  You
# must have a working gpg, tar, unzip in your path.  This has been
# tested on Linux and on Cygwin under Windows 7.

cygwin = platform.system().lower().startswith('cygwin')
cygwinWindowsRoot = os.popen('cygpath -w /').read().strip().replace('\\','/') if cygwin else ''

def unshortenURL(url):
  parsed = urllib.parse.urlparse(url)
  if parsed[0] in ('http', 'https'):
    h = http.client.HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if int(response.status/100) == 3 and response.getheader('Location'):
      return response.getheader('Location')
  return url  

def javaExe(version):
  if version == '1.6':
    path = JAVA6_HOME
  elif version == '1.7':
    path = JAVA7_HOME
  else:
    raise RuntimeError("unknown Java version '%s'" % version)
  if cygwin:
    path = os.popen('cygpath -u "%s"' % path).read().strip()
  return 'export JAVA_HOME="%s" PATH="%s/bin:$PATH"' % (path, path)

def verifyJavaVersion(version):
  s = os.popen('%s; java -version 2>&1' % javaExe(version)).read()
  if s.find(' version "%s.' % version) == -1:
    raise RuntimeError('got wrong version for java %s:\n%s' % (version, s))

# http://s.apache.org/lusolr32rc2
env = os.environ
try:
  JAVA6_HOME = env['JAVA6_HOME']
except KeyError:
  JAVA6_HOME = '/usr/local/jdk1.6.0_27'

try:
  JAVA7_HOME = env['JAVA7_HOME']
except KeyError:
  JAVA7_HOME = '/usr/local/jdk1.7.0_01'

verifyJavaVersion('1.6')
verifyJavaVersion('1.7')

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

# Set to False to avoid re-downloading the packages...
FORCE_CLEAN = True

def getHREFs(urlString):

  # Deref any redirects
  while True:
    url = urllib.parse.urlparse(urlString)
    h = http.client.HTTPConnection(url.netloc)
    h.request('GET', url.path)
    r = h.getresponse()
    newLoc = r.getheader('location')
    if newLoc is not None:
      urlString = newLoc
    else:
      break

  links = []
  try:
    html = urllib.request.urlopen(urlString).read().decode('UTF-8')
  except:
    print('\nFAILED to open url %s' % urlString)
    traceback.print_exc()
    raise
  
  for subUrl, text in reHREF.findall(html):
    fullURL = urllib.parse.urljoin(urlString, subUrl)
    links.append((text, fullURL))
  return links

def download(name, urlString, tmpDir, quiet=False):
  fileName = '%s/%s' % (tmpDir, name)
  if not FORCE_CLEAN and os.path.exists(fileName):
    if not quiet and fileName.find('.asc') == -1:
      print('    already done: %.1f MB' % (os.path.getsize(fileName)/1024./1024.))
    return
  fIn = urllib.request.urlopen(urlString)
  fOut = open(fileName, 'wb')
  success = False
  try:
    while True:
      s = fIn.read(65536)
      if s == b'':
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
    print('    %.1f MB' % (os.path.getsize(fileName)/1024./1024.))
    
def load(urlString):
  return urllib.request.urlopen(urlString).read().decode('utf-8')

def noJavaPackageClasses(desc, file):
  with zipfile.ZipFile(file) as z2:
    for name2 in z2.namelist():
      if name2.endswith('.class') and (name2.startswith('java/') or name2.startswith('javax/')):
        raise RuntimeError('%s contains sheisty class "%s"' %  (desc, name2))

def decodeUTF8(bytes):
  return codecs.getdecoder('UTF-8')(bytes)[0]

MANIFEST_FILE_NAME = 'META-INF/MANIFEST.MF'
NOTICE_FILE_NAME = 'META-INF/NOTICE.txt'
LICENSE_FILE_NAME = 'META-INF/LICENSE.txt'

def checkJARMetaData(desc, jarFile, version):

  with zipfile.ZipFile(jarFile, 'r') as z:
    for name in (MANIFEST_FILE_NAME, NOTICE_FILE_NAME, LICENSE_FILE_NAME):
      try:
        # The Python docs state a KeyError is raised ... so this None
        # check is just defensive:
        if z.getinfo(name) is None:
          raise RuntimeError('%s is missing %s' % (desc, name))
      except KeyError:
        raise RuntimeError('%s is missing %s' % (desc, name))
      
    s = decodeUTF8(z.read(MANIFEST_FILE_NAME))
    
    for verify in (
      'Implementation-Vendor: The Apache Software Foundation',
      # Make sure 1.6 compiler was used to build release bits:
      'X-Compile-Source-JDK: 1.6',
      # Make sure .class files are 1.6 format:
      'X-Compile-Target-JDK: 1.6',
      # Make sure this matches the version we think we are releasing:
      'Specification-Version: %s' % version):
      if s.find(verify) == -1:
        raise RuntimeError('%s is missing "%s" inside its META-INF/MANIFES.MF' % \
                           (desc, verify))

    notice = decodeUTF8(z.read(NOTICE_FILE_NAME))
    license = decodeUTF8(z.read(LICENSE_FILE_NAME))

    idx = desc.find('inside WAR file')
    if idx != -1:
      desc2 = desc[:idx]
    else:
      desc2 = desc

    justFileName = os.path.split(desc2)[1]
    
    if justFileName.lower().find('solr') != -1:
      if SOLR_LICENSE is None:
        raise RuntimeError('BUG in smokeTestRelease!')
      if SOLR_NOTICE is None:
        raise RuntimeError('BUG in smokeTestRelease!')
      if notice != SOLR_NOTICE:
        raise RuntimeError('%s: %s contents doesn\'t match main NOTICE.txt' % \
                           (desc, NOTICE_FILE_NAME))
      if license != SOLR_LICENSE:
        raise RuntimeError('%s: %s contents doesn\'t match main LICENSE.txt' % \
                           (desc, LICENSE_FILE_NAME))
    else:
      if LUCENE_LICENSE is None:
        raise RuntimeError('BUG in smokeTestRelease!')
      if LUCENE_NOTICE is None:
        raise RuntimeError('BUG in smokeTestRelease!')
      if notice != LUCENE_NOTICE:
        raise RuntimeError('%s: %s contents doesn\'t match main NOTICE.txt' % \
                           (desc, NOTICE_FILE_NAME))
      if license != LUCENE_LICENSE:
        raise RuntimeError('%s: %s contents doesn\'t match main LICENSE.txt' % \
                           (desc, LICENSE_FILE_NAME))

def normSlashes(path):
  return path.replace(os.sep, '/')
    
def checkAllJARs(topDir, project, version):
  print('    verify JAR/WAR metadata...')  
  for root, dirs, files in os.walk(topDir):

    normRoot = normSlashes(root)

    if project == 'solr' and normRoot.endswith('/example/lib'):
      # Solr's example intentionally ships servlet JAR:
      continue
    
    for file in files:
      if file.lower().endswith('.jar'):
        if project == 'solr':
          if normRoot.endswith('/contrib/dataimporthandler/lib') and (file.startswith('mail-') or file.startswith('activation-')):
            print('      **WARNING**: skipping check of %s/%s: it has javax.* classes' % (root, file))
            continue
        fullPath = '%s/%s' % (root, file)
        noJavaPackageClasses('JAR file "%s"' % fullPath, fullPath)
        if file.lower().find('lucene') != -1 or file.lower().find('solr') != -1:
          checkJARMetaData('JAR file "%s"' % fullPath, fullPath, version)
  

def checkSolrWAR(warFileName, version):

  """
  Crawls for JARs inside the WAR and ensures there are no classes
  under java.* or javax.* namespace.
  """

  print('    make sure WAR file has no javax.* or java.* classes...')

  checkJARMetaData(warFileName, warFileName, version)

  with zipfile.ZipFile(warFileName, 'r') as z:
    for name in z.namelist():
      if name.endswith('.jar'):
        noJavaPackageClasses('JAR file %s inside WAR file %s' % (name, warFileName),
                             io.BytesIO(z.read(name)))
        if name.lower().find('lucene') != -1 or name.lower().find('solr') != -1:
          checkJARMetaData('JAR file %s inside WAR file %s' % (name, warFileName),
                           io.BytesIO(z.read(name)),
                           version)
        
def checkSigs(project, urlString, version, tmpDir, isSigned):

  print('  test basics...')
  ents = getDirEntries(urlString)
  artifact = None
  keysURL = None
  changesURL = None
  mavenURL = None
  expectedSigs = []
  if isSigned:
    expectedSigs.append('asc')
  expectedSigs.extend(['md5', 'sha1'])
  
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

  print('  get KEYS')
  download('%s.KEYS' % project, keysURL, tmpDir)

  keysFile = '%s/%s.KEYS' % (tmpDir, project)

  # Set up clean gpg world; import keys file:
  gpgHomeDir = '%s/%s.gpg' % (tmpDir, project)
  if os.path.exists(gpgHomeDir):
    shutil.rmtree(gpgHomeDir)
  os.makedirs(gpgHomeDir, 0o700)
  run('gpg --homedir %s --import %s' % (gpgHomeDir, keysFile),
      '%s/%s.gpg.import.log 2>&1' % (tmpDir, project))

  if mavenURL is None:
    raise RuntimeError('%s is missing maven' % project)

  if changesURL is None:
    raise RuntimeError('%s is missing changes-%s' % (project, version))
  testChanges(project, version, changesURL)

  for artifact, urlString in artifacts:
    print('  download %s...' % artifact)
    download(artifact, urlString, tmpDir)
    verifyDigests(artifact, urlString, tmpDir)

    if isSigned:
      print('    verify sig')
      # Test sig (this is done with a clean brand-new GPG world)
      download(artifact + '.asc', urlString + '.asc', tmpDir)
      sigFile = '%s/%s.asc' % (tmpDir, artifact)
      artifactFile = '%s/%s' % (tmpDir, artifact)
      logFile = '%s/%s.%s.gpg.verify.log' % (tmpDir, project, artifact)
      run('gpg --homedir %s --verify %s %s' % (gpgHomeDir, sigFile, artifactFile),
          logFile)
      # Forward any GPG warnings, except the expected one (since its a clean world)
      f = open(logFile, encoding='UTF-8')
      for line in f.readlines():
        if line.lower().find('warning') != -1 \
        and line.find('WARNING: This key is not certified with a trusted signature') == -1:
          print('      GPG: %s' % line.strip())
      f.close()

      # Test trust (this is done with the real users config)
      run('gpg --import %s' % (keysFile),
          '%s/%s.gpg.trust.import.log 2>&1' % (tmpDir, project))
      print('    verify trust')
      logFile = '%s/%s.%s.gpg.trust.log' % (tmpDir, project, artifact)
      run('gpg --verify %s %s' % (sigFile, artifactFile), logFile)
      # Forward any GPG warnings:
      f = open(logFile, encoding='UTF-8')
      for line in f.readlines():
        if line.lower().find('warning') != -1:
          print('      GPG: %s' % line.strip())
      f.close()

def testChanges(project, version, changesURLString):
  print('  check changes HTML...')
  changesURL = None
  for text, subURL in getDirEntries(changesURLString):
    if text == 'Changes.html':
      changesURL = subURL

  if changesURL is None:
    raise RuntimeError('did not see Changes.html link from %s' % changesURLString)

  s = load(changesURL)
  checkChangesContent(s, version, changesURL, project, True)

def testChangesText(dir, version, project):
  "Checks all CHANGES.txt under this dir."
  for root, dirs, files in os.walk(dir):

    # NOTE: O(N) but N should be smallish:
    if 'CHANGES.txt' in files:
      fullPath = '%s/CHANGES.txt' % root
      #print 'CHECK %s' % fullPath
      checkChangesContent(open(fullPath, encoding='UTF-8').read(), version, fullPath, project, False)
      
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
      # benchmark never seems to include release info:
      if name.find('/benchmark/') == -1:
        raise RuntimeError('did not see "%s" in %s' % (sub, name))

reUnixPath = re.compile(r'\b[a-zA-Z_]+=(?:"(?:\\"|[^"])*"' + '|(?:\\\\.|[^"\'\\s])*' + r"|'(?:\\'|[^'])*')" \
                        + r'|(/(?:\\.|[^"\'\s])*)' \
                        + r'|("/(?:\\.|[^"])*")'   \
                        + r"|('/(?:\\.|[^'])*')")

def unix2win(matchobj):
  if matchobj.group(1) is not None: return cygwinWindowsRoot + matchobj.group()
  if matchobj.group(2) is not None: return '"%s%s' % (cygwinWindowsRoot, matchobj.group().lstrip('"'))
  if matchobj.group(3) is not None: return "'%s%s" % (cygwinWindowsRoot, matchobj.group().lstrip("'"))
  return matchobj.group()

def cygwinifyPaths(command):
  # The problem: Native Windows applications running under Cygwin
  # (e.g. Ant, which isn't available as a Cygwin package) can't
  # handle Cygwin's Unix-style paths.  However, environment variable
  # values are automatically converted, so only paths outside of
  # environment variable values should be converted to Windows paths.
  # Assumption: all paths will be absolute.
  if '; ant ' in command: command = reUnixPath.sub(unix2win, command)
  return command

def run(command, logFile):
  if cygwin: command = cygwinifyPaths(command)
  if os.system('%s > %s 2>&1' % (command, logFile)):
    logPath = os.path.abspath(logFile)
    print('\ncommand "%s" failed:' % command)

    # Assume log file was written in system's default encoding, but
    # even if we are wrong, we replace errors ... the ASCII chars
    # (which is what we mostly care about eg for the test seed) should
    # still survive:
    txt = codecs.open(logPath, 'r', encoding=sys.getdefaultencoding(), errors='replace').read()

    # Encode to our output encoding (likely also system's default
    # encoding):
    bytes = txt.encode(sys.stdout.encoding, errors='replace')

    # Decode back to string and print... we should hit no exception here
    # since all errors have been replaced:
    print(codecs.getdecoder(sys.stdout.encoding)(bytes)[0])
    print()

    raise RuntimeError('command "%s" failed; see log file %s' % (command, logPath))
    
def verifyDigests(artifact, urlString, tmpDir):
  print('    verify md5/sha1 digests')
  md5Expected, t = load(urlString + '.md5').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('MD5 %s.md5 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  sha1Expected, t = load(urlString + '.sha1').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('SHA1 %s.sha1 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  m = hashlib.md5()
  s = hashlib.sha1()
  f = open('%s/%s' % (tmpDir, artifact), 'rb')
  while True:
    x = f.read(65536)
    if len(x) == 0:
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
  if urlString.startswith('file:/') and not urlString.startswith('file://'):
    # stupid bogus ant URI
    urlString = "file:///" + urlString[6:]

  if urlString.startswith('file://'):
    path = urlString[7:]
    if path.endswith('/'):
      path = path[:-1]
    if cygwin: # Convert Windows path to Cygwin path
      path = re.sub(r'^/([A-Za-z]):/', r'/cygdrive/\1/', path)
    l = []
    for ent in os.listdir(path):
      entPath = '%s/%s' % (path, ent)
      if os.path.isdir(entPath):
        entPath += '/'
        ent += '/'
      l.append((ent, 'file://%s' % entPath))
    l.sort()
    return l
  else:
    links = getHREFs(urlString)
    for i, (text, subURL) in enumerate(links):
      if text == 'Parent Directory' or text == '..':
        return links[(i+1):]

def unpackAndVerify(project, tmpDir, artifact, version):
  destDir = '%s/unpack' % tmpDir
  if os.path.exists(destDir):
    shutil.rmtree(destDir)
  os.makedirs(destDir)
  os.chdir(destDir)
  print('  unpack %s...' % artifact)
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
  verifyUnpacked(project, artifact, unpackPath, version, tmpDir)

LUCENE_NOTICE = None
LUCENE_LICENSE = None
SOLR_NOTICE = None
SOLR_LICENSE = None

def verifyUnpacked(project, artifact, unpackPath, version, tmpDir):
  global LUCENE_NOTICE
  global LUCENE_LICENSE
  global SOLR_NOTICE
  global SOLR_LICENSE

  os.chdir(unpackPath)
  isSrc = artifact.find('-src') != -1
  l = os.listdir(unpackPath)
  textFiles = ['LICENSE', 'NOTICE', 'README']
  if project == 'lucene':
    textFiles.extend(('JRE_VERSION_MIGRATION', 'CHANGES', 'MIGRATE'))
    if isSrc:
      textFiles.append('BUILD')
  for fileName in textFiles:
    fileName += '.txt'
    if fileName not in l:
      raise RuntimeError('file "%s" is missing from artifact %s' % (fileName, artifact))
    l.remove(fileName)

  if project == 'lucene':
    if LUCENE_NOTICE is None:
      LUCENE_NOTICE = open('%s/NOTICE.txt' % unpackPath, encoding='UTF-8').read()
    if LUCENE_LICENSE is None:
      LUCENE_LICENSE = open('%s/LICENSE.txt' % unpackPath, encoding='UTF-8').read()
  else:
    if SOLR_NOTICE is None:
      SOLR_NOTICE = open('%s/NOTICE.txt' % unpackPath, encoding='UTF-8').read()
    if SOLR_LICENSE is None:
      SOLR_LICENSE = open('%s/LICENSE.txt' % unpackPath, encoding='UTF-8').read()

  if not isSrc:
    # TODO: we should add verifyModule/verifySubmodule (e.g. analysis) here and recurse through
    if project == 'lucene':
      expectedJARs = ()
    else:
      expectedJARs = ()

    for fileName in expectedJARs:
      fileName += '.jar'
      if fileName not in l:
        raise RuntimeError('%s: file "%s" is missing from artifact %s' % (project, fileName, artifact))
      l.remove(fileName)

  if project == 'lucene':
    # TODO: clean this up to not be a list of modules that we must maintain
    extras = ('analysis', 'benchmark', 'codecs', 'core', 'demo', 'docs', 'facet', 'grouping', 'highlighter', 'join', 'memory', 'misc', 'queries', 'queryparser', 'sandbox', 'spatial', 'suggest', 'test-framework', 'licenses')
    if isSrc:
      extras += ('build.xml', 'common-build.xml', 'module-build.xml', 'ivy-settings.xml', 'backwards', 'tools', 'site')
  else:
    extras = ()

  # TODO: if solr, verify lucene/licenses, solr/licenses are present

  for e in extras:
    if e not in l:
      raise RuntimeError('%s: %s missing from artifact %s' % (project, e, artifact))
    l.remove(e)

  if project == 'lucene':
    if len(l) > 0:
      raise RuntimeError('%s: unexpected files/dirs in artifact %s: %s' % (project, artifact, l))

  if isSrc:
    print('    make sure no JARs/WARs in src dist...')
    lines = os.popen('find . -name \\*.jar').readlines()
    if len(lines) != 0:
      print('    FAILED:')
      for line in lines:
        print('      %s' % line.strip())
      raise RuntimeError('source release has JARs...')
    lines = os.popen('find . -name \\*.war').readlines()
    if len(lines) != 0:
      print('    FAILED:')
      for line in lines:
        print('      %s' % line.strip())
      raise RuntimeError('source release has WARs...')

    print('    run "ant validate"')
    run('%s; ant validate' % javaExe('1.7'), '%s/validate.log' % unpackPath)

    if project == 'lucene':
      print('    run tests w/ Java 6...')
      run('%s; ant test' % javaExe('1.6'), '%s/test.log' % unpackPath)
      run('%s; ant jar' % javaExe('1.6'), '%s/compile.log' % unpackPath)
      testDemo(isSrc, version)
      # test javadocs
      print('    generate javadocs w/ Java 6...')
      run('%s; ant javadocs' % javaExe('1.6'), '%s/javadocs.log' % unpackPath)
      checkJavadocpath('%s/build/docs' % unpackPath)
    else:
      os.chdir('solr')
      # DISABLED until solr tests consistently pass
      #print('    run tests w/ Java 6...')
      #run('%s; ant test' % javaExe('1.6'), '%s/test.log' % unpackPath)

      # test javadocs
      print('    generate javadocs w/ Java 6...')
      run('%s; ant javadocs' % javaExe('1.6'), '%s/javadocs.log' % unpackPath)
      checkJavadocpath('%s/solr/build/docs' % unpackPath, False)

      # DISABLED until solr tests consistently pass
      #print('    run tests w/ Java 7...')
      #run('%s; ant test' % javaExe('1.7'), '%s/test.log' % unpackPath)
 
      # test javadocs
      print('    generate javadocs w/ Java 7...')
      run('%s; ant javadocs' % javaExe('1.7'), '%s/javadocs.log' % unpackPath)
      checkJavadocpath('%s/solr/build/docs' % unpackPath, False)

      print('    test solr example w/ Java 6...')
      run('%s; ant clean example' % javaExe('1.6'), '%s/antexample.log' % unpackPath)
      testSolrExample(unpackPath, JAVA6_HOME, True)

      print('    test solr example w/ Java 7...')
      run('%s; ant clean example' % javaExe('1.7'), '%s/antexample.log' % unpackPath)
      testSolrExample(unpackPath, JAVA7_HOME, True)
      os.chdir('..')

      print('    check NOTICE')
      testNotice(unpackPath)

  else:

    checkAllJARs(os.getcwd(), project, version)
    
    if project == 'lucene':
      testDemo(isSrc, version)

    else:
      checkSolrWAR('%s/example/webapps/solr.war' % unpackPath, version)

      print('    copying unpacked distribution for Java 6 ...')
      java6UnpackPath = '%s-java6' %unpackPath
      if os.path.exists(java6UnpackPath):
        shutil.rmtree(java6UnpackPath)
      shutil.copytree(unpackPath, java6UnpackPath)
      os.chdir(java6UnpackPath)
      print('    test solr example w/ Java 6...')
      testSolrExample(java6UnpackPath, JAVA6_HOME, False)

      print('    copying unpacked distribution for Java 7 ...')
      java7UnpackPath = '%s-java7' %unpackPath
      if os.path.exists(java7UnpackPath):
        shutil.rmtree(java7UnpackPath)
      shutil.copytree(unpackPath, java7UnpackPath)
      os.chdir(java7UnpackPath)
      print('    test solr example w/ Java 7...')
      testSolrExample(java7UnpackPath, JAVA7_HOME, False)

      os.chdir(unpackPath)

  testChangesText('.', version, project)

  if project == 'lucene' and not isSrc:
    print('    check Lucene\'s javadoc JAR')
    checkJavadocpath('%s/docs' % unpackPath)

def testNotice(unpackPath):
  solrNotice = open('%s/NOTICE.txt' % unpackPath, encoding='UTF-8').read()
  luceneNotice = open('%s/lucene/NOTICE.txt' % unpackPath, encoding='UTF-8').read()

  expected = """
=========================================================================
==  Apache Lucene Notice                                               ==
=========================================================================

""" + luceneNotice + """---
"""
  
  if solrNotice.find(expected) == -1:
    raise RuntimeError('Solr\'s NOTICE.txt does not have the verbatim copy, plus header/footer, of Lucene\'s NOTICE.txt')
  
def readSolrOutput(p, startupEvent, failureEvent, logFile):
  f = open(logFile, 'wb')
  try:
    while True:
      line = p.readline()
      if len(line) == 0:
        break
      f.write(line)
      f.flush()
      # print 'SOLR: %s' % line.strip()
      if not startupEvent.isSet() and line.find(b'Started SocketConnector@0.0.0.0:8983') != -1:
        startupEvent.set()
  except:
    print()
    print('Exception reading Solr output:')
    traceback.print_exc()
    failureEvent.set()
  finally:
    f.close()
    
def testSolrExample(unpackPath, javaPath, isSrc):
  logFile = '%s/solr-example.log' % unpackPath
  os.chdir('example')
  print('      start Solr instance (log=%s)...' % logFile)
  env = {}
  env.update(os.environ)
  env['JAVA_HOME'] = javaPath
  env['PATH'] = '%s/bin:%s' % (javaPath, env['PATH'])
  server = subprocess.Popen(['java', '-jar', 'start.jar'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

  startupEvent = threading.Event()
  failureEvent = threading.Event()
  serverThread = threading.Thread(target=readSolrOutput, args=(server.stderr, startupEvent, failureEvent, logFile))
  serverThread.setDaemon(True)
  serverThread.start()

  # Make sure Solr finishes startup:
  startupEvent.wait()
  print('      startup done')
  
  try:
    print('      test utf8...')
    run('sh ./exampledocs/test_utf8.sh', 'utf8.log')
    print('      index example docs...')
    run('sh ./exampledocs/post.sh ./exampledocs/*.xml', 'post-example-docs.log')
    print('      run query...')
    s = urllib.request.urlopen('http://localhost:8983/solr/select/?q=video').read().decode('UTF-8')
    if s.find('<result name="response" numFound="3" start="0">') == -1:
      print('FAILED: response is:\n%s' % s)
      raise RuntimeError('query on solr example instance failed')
  finally:
    # Stop server:
    print('      stop server (SIGINT)...')
    os.kill(server.pid, signal.SIGINT)

    # Give it 10 seconds to gracefully shut down
    serverThread.join(10.0)

    if serverThread.isAlive():
      # Kill server:
      print('***WARNING***: Solr instance didn\'t respond to SIGINT; using SIGKILL now...')
      os.kill(server.pid, signal.SIGKILL)

      serverThread.join(10.0)

      if serverThread.isAlive():
        # Shouldn't happen unless something is seriously wrong...
        print('***WARNING***: Solr instance didn\'t respond to SIGKILL; ignoring...')

  if failureEvent.isSet():
    raise RuntimeError('exception while reading Solr output')
    
  os.chdir('..')
    
def checkJavadocpath(path, failOnMissing=True):
  # check for level='package'
  # we fail here if its screwed up
  if failOnMissing and checkJavaDocs.checkPackageSummaries(path, 'package'):
    raise RuntimeError('missing javadocs package summaries!')
    
  # now check for level='class'
  if checkJavaDocs.checkPackageSummaries(path):
    # disabled: RM cannot fix all this, see LUCENE-3887
    # raise RuntimeError('javadoc problems')
    print('\n***WARNING***: javadocs want to fail!\n')

  if checkJavadocLinks.checkAll(path):
    raise RuntimeError('broken javadocs links found!')

def testDemo(isSrc, version):
  print('    test demo...')
  sep = ';' if cygwin else ':'
  if isSrc:
    cp = 'build/core/classes/java{0}build/demo/classes/java{0}build/analysis/common/classes/java{0}build/queryparser/classes/java'.format(sep)
    docsDir = 'core/src'
  else:
    cp = 'core/lucene-core-{0}.jar{1}demo/lucene-demo-{0}.jar{1}analysis/common/lucene-analyzers-common-{0}.jar{1}queryparser/lucene-queryparser-{0}.jar'.format(version, sep)
    docsDir = 'docs'
  run('%s; java -cp "%s" org.apache.lucene.demo.IndexFiles -index index -docs %s' % (javaExe('1.6'), cp, docsDir), 'index.log')
  run('%s; java -cp "%s" org.apache.lucene.demo.SearchFiles -index index -query lucene' % (javaExe('1.6'), cp), 'search.log')
  reMatchingDocs = re.compile('(\d+) total matching documents')
  m = reMatchingDocs.search(open('search.log', encoding='UTF-8').read())
  if m is None:
    raise RuntimeError('lucene demo\'s SearchFiles found no results')
  else:
    numHits = int(m.group(1))
    if numHits < 100:
      raise RuntimeError('lucene demo\'s SearchFiles found too few results: %s' % numHits)
    print('      got %d hits for query "lucene"' % numHits)

def checkMaven(baseURL, tmpDir, version, isSigned):
  # Locate the release branch in subversion
  m = re.match('(\d+)\.(\d+)', version) # Get Major.minor version components
  releaseBranchText = 'lucene_solr_%s_%s/' % (m.group(1), m.group(2))
  branchesURL = 'http://svn.apache.org/repos/asf/lucene/dev/branches/'
  releaseBranchSvnURL = None
  branches = getDirEntries(branchesURL)
  for text, subURL in branches:
    if text == releaseBranchText:
      releaseBranchSvnURL = subURL

  print('    get POM templates', end=' ')
  POMtemplates = defaultdict()
  getPOMtemplates(POMtemplates, tmpDir, releaseBranchSvnURL)
  print()
  print('    download artifacts', end=' ')
  artifacts = {'lucene': [], 'solr': []}
  for project in ('lucene', 'solr'):
    artifactsURL = '%s/%s/maven/org/apache/%s' % (baseURL, project, project)
    targetDir = '%s/maven/org/apache/%s' % (tmpDir, project)
    if not os.path.exists(targetDir):
      os.makedirs(targetDir)
    crawl(artifacts[project], artifactsURL, targetDir)
  print()
  print('    verify that each binary artifact has a deployed POM...')
  verifyPOMperBinaryArtifact(artifacts, version)
  print('    verify that there is an artifact for each POM template...')
  verifyArtifactPerPOMtemplate(POMtemplates, artifacts, tmpDir, version)
  print("    verify Maven artifacts' md5/sha1 digests...")
  verifyMavenDigests(artifacts)
  print('    verify that all non-Mavenized deps are deployed...')
  nonMavenizedDeps = dict()
  checkNonMavenizedDeps(nonMavenizedDeps, POMtemplates, artifacts, tmpDir,
                        version, releaseBranchSvnURL)
  print('    check for javadoc and sources artifacts...')
  checkJavadocAndSourceArtifacts(nonMavenizedDeps, artifacts, version)
  print("    verify deployed POMs' coordinates...")
  verifyDeployedPOMsCoordinates(artifacts, version)
  if isSigned:
    print('    verify maven artifact sigs', end=' ')
    verifyMavenSigs(baseURL, tmpDir, artifacts)

  distributionFiles = getDistributionsForMavenChecks(tmpDir, version, baseURL)

  print('    verify that non-Mavenized deps are same as in the binary distribution...')
  checkIdenticalNonMavenizedDeps(distributionFiles, nonMavenizedDeps)
  print('    verify that Maven artifacts are same as in the binary distribution...')
  checkIdenticalMavenArtifacts(distributionFiles, nonMavenizedDeps, artifacts, version)

  checkAllJARs('%s/maven/org/apache/lucene' % tmpDir, 'lucene', version)
  checkAllJARs('%s/maven/org/apache/solr' % tmpDir, 'solr', version)

def getDistributionsForMavenChecks(tmpDir, version, baseURL):
  distributionFiles = defaultdict()
  for project in ('lucene', 'solr'):
    distribution = '%s-%s.tgz' % (project, version)
    if project == 'solr': distribution = 'apache-' + distribution
    if not os.path.exists('%s/%s' % (tmpDir, distribution)):
      distURL = '%s/%s/%s' % (baseURL, project, distribution)
      print('    download %s...' % distribution, end=' ')
      download(distribution, distURL, tmpDir)
    destDir = '%s/unpack-%s-maven' % (tmpDir, project)
    if os.path.exists(destDir):
      shutil.rmtree(destDir)
    os.makedirs(destDir)
    os.chdir(destDir)
    print('    unpack %s...' % distribution)
    unpackLogFile = '%s/unpack-%s-maven-checks.log' % (tmpDir, distribution)
    run('tar xzf %s/%s' % (tmpDir, distribution), unpackLogFile)
    if project == 'solr': # unpack the Solr war
      unpackLogFile = '%s/unpack-solr-war-maven-checks.log' % tmpDir
      print('        unpack Solr war...')
      run('jar xvf */dist/*.war', unpackLogFile)
    distributionFiles[project] = []
    for root, dirs, files in os.walk(destDir):
      distributionFiles[project].extend([os.path.join(root, file) for file in files])
  return distributionFiles

def checkJavadocAndSourceArtifacts(nonMavenizedDeps, artifacts, version):
  for project in ('lucene', 'solr'):
    for artifact in artifacts[project]:
      if artifact.endswith(version + '.jar') and artifact not in list(nonMavenizedDeps.keys()):
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
    for dep in list(nonMavenizedDeps.keys()):
      if ('/%s/' % project) in dep:
        depOrigFilename = os.path.basename(nonMavenizedDeps[dep])
        if not depOrigFilename in distFilenames:
          raise RuntimeError('missing: non-mavenized dependency %s' % nonMavenizedDeps[dep])
        identical = filecmp.cmp(dep, distFilenames[depOrigFilename], shallow=False)
        if not identical:
          raise RuntimeError('Deployed non-mavenized dep %s differs from distribution dep %s'
                            % (dep, distFilenames[depOrigFilename]))

def checkIdenticalMavenArtifacts(distributionFiles, nonMavenizedDeps, artifacts, version):
  reJarWar = re.compile(r'%s\.[wj]ar$' % version) # exclude *-javadoc.jar and *-sources.jar
  for project in ('lucene', 'solr'):
    distFilenames = dict()
    for file in distributionFiles[project]:
      baseName = os.path.basename(file)
      if project == 'solr': # Remove 'apache-' prefix to allow comparison to Maven artifacts
        baseName = baseName.replace('apache-', '')
      distFilenames[baseName] = file
    for artifact in artifacts[project]:
      if reJarWar.search(artifact):
        if artifact not in list(nonMavenizedDeps.keys()):
          artifactFilename = os.path.basename(artifact)
          if artifactFilename not in list(distFilenames.keys()):
            raise RuntimeError('Maven artifact %s is not present in %s binary distribution'
                              % (artifact, project))
         # TODO: Either fix the build to ensure that maven artifacts *are* identical, or recursively compare contents
         # identical = filecmp.cmp(artifact, distFilenames[artifactFilename], shallow=False)
         # if not identical:
         #   raise RuntimeError('Maven artifact %s is not identical to %s in %s binary distribution'
         #                     % (artifact, distFilenames[artifactFilename], project))

def verifyMavenDigests(artifacts):
  reJarWarPom = re.compile(r'\.(?:[wj]ar|pom)$')
  for project in ('lucene', 'solr'):
    for artifactFile in [a for a in artifacts[project] if reJarWarPom.search(a)]:
      if artifactFile + '.md5' not in artifacts[project]:
        raise RuntimeError('missing: MD5 digest for %s' % artifactFile)
      if artifactFile + '.sha1' not in artifacts[project]:
        raise RuntimeError('missing: SHA1 digest for %s' % artifactFile)
      with open(artifactFile + '.md5', encoding='UTF-8') as md5File:
        md5Expected = md5File.read().strip()
      with open(artifactFile + '.sha1', encoding='UTF-8') as sha1File:
        sha1Expected = sha1File.read().strip()
      md5 = hashlib.md5()
      sha1 = hashlib.sha1()
      inputFile = open(artifactFile, 'rb')
      while True:
        bytes = inputFile.read(65536)
        if len(bytes) == 0:
          break
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
              doc2 = None
              workingCopy = os.path.abspath('%s/../..' % sys.path[0])
              for pomDir in pomDirs:
                if releaseBranchSvnURL is None:
                  pomPath = '%s/%s/%s' % (workingCopy, pomDir, pomFile)
                  if os.path.exists(pomPath):
                    doc2 = ET.XML(open(pomPath, encoding='UTF-8').read())
                    break
                else:
                  entries = getDirEntries('%s/%s' % (releaseBranchSvnURL, pomDir))
                  for text, subURL in entries:
                    if text == pomFile:
                      doc2 = ET.XML(load(subURL))
                      break
                  if doc2 is not None: break

              groupId2, artifactId2, packaging2, POMversion = getPOMcoordinate(doc2)
              depJar = '%s/maven/%s/%s/%s/%s-%s.jar' \
                     % (tmpDir, groupId2.replace('.', '/'),
                        artifactId2, version, artifactId2, version)
              if depJar not in artifacts['lucene'] and depJar not in artifacts['solr']:
                raise RuntimeError('Missing non-mavenized dependency %s' % depJar)
              nonMavenizedDependencies[depJar] = file

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
    os.makedirs(gpgHomeDir, 0o700)
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
      f = open(logFile, encoding='UTF-8')
      for line in f.readlines():
        if line.lower().find('warning') != -1 \
           and line.find('WARNING: This key is not certified with a trusted signature') == -1 \
           and line.find('WARNING: using insecure memory') == -1:
          print('      GPG: %s' % line.strip())
      f.close()

      # Test trust (this is done with the real users config)
      run('gpg --import %s' % keysFile,
          '%s/%s.gpg.trust.import.log' % (tmpDir, project))
      logFile = '%s/%s.%s.gpg.trust.log' % (tmpDir, project, artifact)
      run('gpg --verify %s %s' % (sigFile, artifactFile), logFile)
      # Forward any GPG warnings:
      f = open(logFile, encoding='UTF-8')
      for line in f.readlines():
        if line.lower().find('warning') != -1 \
           and line.find('WARNING: This key is not certified with a trusted signature') == -1 \
           and line.find('WARNING: using insecure memory') == -1:
          print('      GPG: %s' % line.strip())
      f.close()

      sys.stdout.write('.')
  print()

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
  allPOMtemplates = []
  sourceLocation = releaseBranchSvnURL
  if sourceLocation is None:
    # Use the POM templates under dev-tools/maven/ in the local working copy
    # sys.path[0] is the directory containing this script: dev-tools/scripts/
    sourceLocation = os.path.abspath('%s/../maven' % sys.path[0])
    rePOMtemplate = re.compile(r'^pom.xml.template$')
    for root, dirs, files in os.walk(sourceLocation):
      allPOMtemplates.extend([os.path.join(root, f) for f in files if rePOMtemplate.search(f)])
  else:
    sourceLocation += 'dev-tools/maven/'
    targetDir = '%s/dev-tools/maven' % tmpDir
    if not os.path.exists(targetDir):
      os.makedirs(targetDir)
    crawl(allPOMtemplates, sourceLocation, targetDir, set(['Apache Subversion', 'maven.testlogging.properties']))

  reLucenePOMtemplate = re.compile(r'.*/maven/lucene.*/pom\.xml\.template$')
  POMtemplates['lucene'] = [p for p in allPOMtemplates if reLucenePOMtemplate.search(p)]
  if POMtemplates['lucene'] is None:
    raise RuntimeError('No Lucene POMs found at %s' % sourceLocation)
  reSolrPOMtemplate = re.compile(r'.*/maven/solr.*/pom\.xml\.template$')
  POMtemplates['solr'] = [p for p in allPOMtemplates if reSolrPOMtemplate.search(p)]
  if POMtemplates['solr'] is None:
    raise RuntimeError('No Solr POMs found at %s' % sourceLocation)
  POMtemplates['grandfather'] = [p for p in allPOMtemplates if '/maven/pom.xml.template' in p]
  if len(POMtemplates['grandfather']) == 0:
    raise RuntimeError('No Lucene/Solr grandfather POM found at %s' % sourceLocation)

def crawl(downloadedFiles, urlString, targetDir, exclusions=set()):
  for text, subURL in getDirEntries(urlString):
    if text not in exclusions:
      path = os.path.join(targetDir, text)
      if text.endswith('/'):
        if not os.path.exists(path):
          os.makedirs(path)
        crawl(downloadedFiles, subURL, path, exclusions)
      else:
        if not os.path.exists(path) or FORCE_CLEAN:
          download(text, subURL, targetDir, quiet=True)
        downloadedFiles.append(path)
        sys.stdout.write('.')

reAllowedVersion = re.compile(r'^\d+\.\d+\.\d+(-ALPHA|-BETA)?$')

def main():

  if len(sys.argv) < 4:
    print()
    print('Usage python -u %s BaseURL version tmpDir' % sys.argv[0])
    print()
    sys.exit(1)

  baseURL = sys.argv[1]
  version = sys.argv[2]

  if not reAllowedVersion.match(version):
    raise RuntimeError('version "%s" does not match format X.Y.Z[-ALPHA|-BETA]' % version)
  
  tmpDir = os.path.abspath(sys.argv[3])
  isSigned = True 
  if len(sys.argv) == 5:
    isSigned = (sys.argv[4] == "True")

  smokeTest(baseURL, version, tmpDir, isSigned)

def smokeTest(baseURL, version, tmpDir, isSigned):

  if FORCE_CLEAN:
    if os.path.exists(tmpDir):
      raise RuntimeError('temp dir %s exists; please remove first' % tmpDir)

  if not os.path.exists(tmpDir):
    os.makedirs(tmpDir)
  
  lucenePath = None
  solrPath = None
  print()
  print('Load release URL "%s"...' % baseURL)
  newBaseURL = unshortenURL(baseURL)
  if newBaseURL != baseURL:
    print('  unshortened: %s' % newBaseURL)
    baseURL = newBaseURL
    
  for text, subURL in getDirEntries(baseURL):
    if text.lower().find('lucene') != -1:
      lucenePath = subURL
    elif text.lower().find('solr') != -1:
      solrPath = subURL

  if lucenePath is None:
    raise RuntimeError('could not find lucene subdir')
  if solrPath is None:
    raise RuntimeError('could not find solr subdir')

  print()
  print('Test Lucene...')
  checkSigs('lucene', lucenePath, version, tmpDir, isSigned)
  for artifact in ('lucene-%s.tgz' % version, 'lucene-%s.zip' % version):
    unpackAndVerify('lucene', tmpDir, artifact, version)
  unpackAndVerify('lucene', tmpDir, 'lucene-%s-src.tgz' % version, version)

  print()
  print('Test Solr...')
  checkSigs('solr', solrPath, version, tmpDir, isSigned)
  for artifact in ('apache-solr-%s.tgz' % version, 'apache-solr-%s.zip' % version):
    unpackAndVerify('solr', tmpDir, artifact, version)
  unpackAndVerify('solr', tmpDir, 'apache-solr-%s-src.tgz' % version, version)

  print()
  print('Test Maven artifacts for Lucene and Solr...')
  checkMaven(baseURL, tmpDir, version, isSigned)

  print('\nSUCCESS!\n')

if __name__ == '__main__':
  print('NOTE: output encoding is %s' % sys.stdout.encoding)
  try:
    main()
  except:
    traceback.print_exc()
    sys.exit(1)
  sys.exit(0)
