import traceback
import datetime
import pickle
import http.client
import zipfile
import subprocess
import types
import os
import re
import urllib.request, urllib.error, urllib.parse
import sys
import time
import tests
import shutil

# Current server version:
VERSION = '0.1-SNAPSHOT'

# Lucene version we're using:
LUCENE_VERSION = '4.6-SNAPSHOT'
#LUCENE_VERSION = '4.5-SNAPSHOT'

reTimeStamp = re.compile(r'-(\d\d\d\d\d\d\d\d\.\d\d\d\d\d\d)')

class Output:

  def __init__(self):
    self.depth = 0

  def push(self, name):
    self.message('--> START: %s' % name)
    self.depth += 2

  def pop(self):
    self.message('--> DONE: %s' % name)
    self.depth -= 2

  def message(self, s):
    print('%s%s' % (' '*self.depth, s))

#
# EG in local.py
#
#   Local checkout to pull Lucene jars from:
#     LUCENE_ROOT = '/path/to/my/4xcheckout
#
#   Or:
#     LUCENE_ROOT = None
#
#   To pull the released JARs

try:
  from local import *
except ImportError:
  # None means to pull the released JARs
  LUCENE_ROOT = None

JAR_CACHE = '%s/.jarcache' % os.environ['HOME']

STATE_FILE = '%s/state.pk' % JAR_CACHE

VERBOSE = '-verbose' in sys.argv
if VERBOSE:
  sys.argv.remove('-verbose')
  
dirStack = []

def run(cmd, wd='.'):
  dirSave = None
  if wd != '.':
    dirSave = os.getcwd()
    os.chdir(wd)
  if VERBOSE:
    print('cwd=%s: run %s' % (os.getcwd(), cmd))
  x = subprocess.call(cmd, shell=True)
  if x != 0:
    sys.exit(1)
  if dirSave is not None:
    os.chdir(dirSave)
  
  #try:
  #  subprocess.check_call(cmd, shell=True)
  #finally:
  #  if dirSave is not None:
  #    os.chdir(dirSave)

def pushDir(dir):
  dirStack.append(os.getcwd())
  os.chdir(dir)

def popDir():
  os.chdir(dirStack.pop())

def download1(groupID, artifactID, version):
  url = 'http://search.maven.org/remotecontent?filepath=%s/%s/%s/%s-%s.jar' % (groupID.replace('.', '/'), artifactID, version, artifactID, version)
  try:
    return urllib.request.urlopen(url)
  except:
    return None

class LoadApacheSnapshots:
  def __init__(self):
    if os.path.exists(STATE_FILE):
      self.state = pickle.loads(open(STATE_FILE, 'rb').read())
    else:
      self.state = {}
    self.changed = False
    self.h = http.client.HTTPSConnection('repository.apache.org', 443)

  def load(self, groupID, artifactID, version, force=False):

    tup = groupID, artifactID, version
    if tup in self.state:
      loc = self.state[tup]
      s = reTimeStamp.search(loc).group(1)
      t = datetime.datetime(year=int(s[:4]), month=int(s[4:6]), day=int(s[6:8]), hour=int(s[9:11]), minute=int(s[11:13]), second=int(s[13:15]))

      # If our copy is < 24 hours "old" then don't check:
      if datetime.datetime.utcnow() - t < datetime.timedelta(hours=24):
        justName = os.path.split(loc)[1]
        return None, justName
      
    for i in range(2):
      path = '/service/local/artifact/maven/redirect?r=snapshots&g=%s&a=%s&v=%s&e=jar' % (groupID, artifactID, version)
      self.h.putrequest('GET', path)
      self.h.endheaders()
      try:
        r = self.h.getresponse()
      except http.client.BadStatusLine:
        # reconnect
        self.h.close()
        self.h = http.client.HTTPSConnection('repository.apache.org', 443)
        continue
      
      loc = r.getheader('location')
      justName = os.path.split(loc)[1]
      r.read()
      if self.state.get(tup) != loc or force:
        self.changed = True
        self.state[tup] = loc
        return urllib.request.urlopen(loc), justName
      else:
        return None, justName

  def close(self):
    if self.changed:
      open('%s.new' % STATE_FILE, 'wb').write(pickle.dumps(self.state))
      if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
      os.rename('%s.new' % STATE_FILE, STATE_FILE)
    self.h.close()

def getJavaClasses(path):
  l = []
  for root, dirs, files in os.walk(path):
    for file in files:
      if file.endswith('.class'):
        fullPath = '%s/%s' % (root, file)
        if os.path.isfile(fullPath):
          l.append(fullPath[len(path):])
  return l

loadSnapshots = None

def getJAR(luceneCheckout, groupID, artifactID, version):
  """
  Loads and caches a dependency JAR from maven.
  """

  global loadSnapshots

  if not os.path.exists(JAR_CACHE):
    os.makedirs(JAR_CACHE)

  fileName = '%s-%s.jar' % (artifactID, version)

  if luceneCheckout is not None and groupID == 'org.apache.lucene' and version.endswith('-SNAPSHOT'):
    # Pull snapshot jars directly from local lucene dev area:
    if artifactID == 'lucene-test-framework':
      subPath = 'test-framework'
    else:
      subPath = '/'.join(artifactID.split('-')[1:])
      subPath = subPath.replace('analyzers/', 'analysis/')
    jarFile = '%s/lucene/build/%s/%s-%s.jar' % (luceneCheckout, subPath, artifactID, version)
    if not os.path.exists(jarFile):
      # An unpacked release:
      jarFile = '%s/%s/%s-%s.jar' % (luceneCheckout, subPath, artifactID, version.replace('-SNAPSHOT', '.0'))
    if not os.path.exists(jarFile):
      raise RuntimeError('jar "%s" doesn\'t exist, run ant jar' % jarFile)
    return jarFile
  
  fullPath = '%s/%s' % (JAR_CACHE, fileName)

  if version.endswith('SNAPSHOT'):
    if loadSnapshots is None:
      loadSnapshots = LoadApacheSnapshots()
    fIn, name = loadSnapshots.load(groupID, artifactID, version, force=not os.path.exists(fullPath))
    doDownload = fIn is not None
  elif not os.path.exists(fullPath):
    doDownload = True
    name = fileName
    fIn = None
  else:
    doDownload = False
    
  if doDownload:
    sys.stdout.write('    fetch %s to %s...' % (name, fullPath))
    if not version.endswith('SNAPSHOT'):
      fIn = download1(groupID, artifactID, version)

    if fIn is None:
      raise RuntimeError('failed to find download location for %s/%s/%s' % (groupID, artifactID, version))

    fOut = open(fullPath + '.new', 'wb')
    while True:
      s = fIn.read(65536)
      if len(s) == 0:
        break
      fOut.write(s)
      sys.stdout.write('.')
      sys.stdout.flush()
    fOut.close()
    fIn.close()
    os.rename(fullPath + '.new', fullPath)
    print('done')
    print('      %d bytes' % os.path.getsize(fullPath))

  return fullPath

def getJavaSources(path):
  l = []
  for root, dirs, files in os.walk(path):
    for file in files:
      if file.endswith('.java'):
        fullPath = '%s/%s' % (root, file)
        if os.path.isfile(fullPath):
          l.append((fullPath[len(path):], os.path.getmtime(fullPath)))
  return l

def getChangedSources(sources, classesDir):
  """
  For each source + mtime, checks the mtime of the corresponding
  .class and only returns sources newer than their classes.
  """
  
  changedSources = []
  for sourceFile, mtime in sources:
    classFile = '%s%s.class' % (classesDir, sourceFile[:-5])
    if not os.path.exists(classFile) or mtime >= os.path.getmtime(classFile):
      changedSources.append(sourceFile)
  return changedSources

def javac(classPath, sources, dest):
  print('  javac %d sources' % len(sources))
  if not os.path.exists(dest):
    os.makedirs(dest)
  cmd = 'javac'
  cmd += ' -Xlint'
  cmd += ' -cp %s' % ':'.join(classPath)
  cmd += ' -d %s' % dest
  cmd += ' '
  cmd += ' '.join([os.path.abspath(x) for x in sources])
  #print cmd
  run(cmd)

def jar(srcDir, destJar):
  destJar = os.path.abspath(destJar)
  #files = getJavaClasses(srcDir)
  #run('jar cf %s.tmp -C %s %s' % (destJar, srcDir, ' '.join(["'%s'" % x for x in files])))

  cmd = 'jar cf %s.tmp -C %s .' % (destJar, srcDir)
  if os.path.exists('src/resources'):
    cmd += ' -C src/resources .'
  run(cmd)
  os.rename('%s.tmp' % destJar, destJar)

def findTests(path):
  l = []
  for root, dirs, files in os.walk(path):
    for file in files:
      if file.endswith('.class') and (file.startswith('Test') or file.endswith('Test.class')):
        fullPath = '%s/%s' % (root, file)
        fullPath = fullPath.replace('./', '')
        if os.path.isfile(fullPath):
          l.append(fullPath[len(path):-6].replace('/', '.'))
  return l

def runTests(testClassPath, testClasses, jvmCount, seed, locale, tz, fileencoding):
  command = 'java -Dtests.prefix=tests -Xmx512M -Dtests.iters= -Dtests.verbose=false -Dtests.infostream=false -Dtests.lockdir=build -Dtests.postingsformat=random -Dtests.locale=random -Dtests.timezone=random -Dtests.directory=random -Dtests.linedocsfile=europarl.lines.txt.gz -Dtests.luceneMatchVersion=5.0 -Dtests.cleanthreads=perClass -Djava.util.logging.config.file=solr/testlogging.properties -Dtests.nightly=false -Dtests.weekly=false -Dtests.slow=false -Dtests.asserts.gracious=false -Dtests.multiplier=1 -Djava.awt.headless=true -Djava.io.tempDir=. -Djunit4.tempDir=. -DtempDir=.'

  command += ' -Dtests.codec=random'

  if seed is not None:
    command += ' -Dtests.seed=%s' % seed

  if locale is not None:
    command += ' -Dtests.locale=%s' % locale

  if tz is not None:
    command += ' -Dtests.timezone=%s' % tz

  if fileencoding is not None:
    command += ' -Dtests.fileencoding=%s' % fileencoding
    
  if len(testClasses) == 1:
    first = testClasses[0]
    if type(first) is tuple:
      if first[1] is not None:
        command += ' -Dtests.method=%s' % first[1]
      testClasses = [first[0]]
      
  command += ' -ea com.carrotsearch.ant.tasks.junit4.slave.SlaveMainSafe -flush -stdin'
  env = os.environ
  env['CLASSPATH'] = ':'.join(testClassPath)
  if jvmCount is not None:
    processCount = jvmCount
  else:
    try:
      import multiprocessing
      processCount = multiprocessing.cpu_count()//2-1
    except:
      processCount = 2

  processCount = max(1, min(processCount, len(testClasses)))

  print('  using %d test JVMs' % processCount)
    
  p = tests.Parent(processCount, env, command, testClasses)
  return p.anyFail

def dedup(l):
  s = set()
  l2 = []
  for v in l:
    if v not in s:
      s.add(v)
      l2.append(v)
  return l2

class Build:

  def __init__(self, name, version, deps, compileTestDeps, runTestDeps, subs=None, packageFiles=None):
    self.tStart = time.time()
    self.subs = subs
    self.name = name
    self.version = version
    self.deps = deps
    self.compileTestDeps = compileTestDeps
    self.runTestDeps = runTestDeps
    self.jarFile = 'build/%s-%s.jar' % (self.name, self.version)
    self.packageFiles = packageFiles

    self.doPackage = False
    self.doCompile = False
    self.seed = None
    self.jvmCount = None
    self.onlyTest = None
    self.coreDone = False
    self.doTest = False
    self.doClean = False
    self.locale = None
    self.tz = None
    self.fileencoding = None
    self.testPackages = []
    
    # Process command-line:
    i = 1
    while i < len(sys.argv):
      x = sys.argv[i]
      i += 1
      if x == 'clean':
        self.doClean = True
      elif x == 'package':
        self.doPackage = True
      elif x == 'coredone':
        self.coreDone = True
      elif x == '-locale':
        if i == len(sys.argv):
          print()
          print('ERROR: missing argument for -locale')
          print()
          sys.exit(1)
        self.locale = sys.argv[i]
        i += 1
      elif x == 'compile':
        self.doCompile = True
      elif x == 'test':
        self.doTest = True
      elif x == '-jvms':
        if i == len(sys.argv):
          print()
          print('ERROR: missing argument for -jvms')
          print()
          sys.exit(1)
        self.jvmCount = int(sys.argv[i])
        i += 1
      elif x == '-seed':
        if i == len(sys.argv):
          print()
          print('ERROR: missing argument for -seed')
          print()
          sys.exit(1)
        self.seed = sys.argv[i]
        i += 1
      elif x == '-tz':
        if i == len(sys.argv):
          print()
          print('ERROR: missing argument for -tz')
          print()
          sys.exit(1)
        self.tz = sys.argv[i]
        i += 1
      elif x == '-fileencoding':
        if i == len(sys.argv):
          print()
          print('ERROR: missing argument for -fileencoding')
          print()
          sys.exit(1)
        self.fileencoding = sys.argv[i]
        i += 1
      else:
        if x.startswith('-'):
          raise RuntimeError('unrecognized option "%s"' % x)
                             
        self.doTest = True
        if x.find('.') != -1:
          self.onlyTest = x.split('.')
          if len(self.onlyTest) != 2:
            raise RuntimeError('single test case should be TestFoo or TestFoo.testMethod')
        else:
          self.onlyTest = (x, None)

    if not self.doClean and not self.doPackage and not self.doCompile:
      self.doTest = True
    
    if self.doClean:
      print('Clean:')
      if os.path.exists('build'):
        shutil.rmtree('build')
      if os.path.exists('dist'):
        shutil.rmtree('dist')

  def addTestPackage(self, destZipFile, entries):
    self.testPackages.append((destZipFile, entries))

  def run(self):
    global loadSnapshots

    try:
      if self.doCompile or self.doTest or self.doPackage:
        self.compile()
        self.compileTest()

      if self.doPackage:
        self.package(self.packageFiles)

      if self.doTest:
        failed = self.runTests(self.onlyTest)
        if failed:
          raise RuntimeError('tests failed')

    finally:
      if loadSnapshots is not None:
        loadSnapshots.close()
        loadSnapshots = None

    if self.subs is not None and self.onlyTest is None:
      for subDir in self.subs:
        print()
        print('%s' % subDir)
        pushDir(subDir)
        run('%s -u build.py coredone %s' % (sys.executable, ' '.join(sys.argv[1:])))
        popDir()
        
    if not self.coreDone:
      print()
      print('DONE total %.1f sec' % (time.time() - self.tStart))
      print()
      
  def compile(self):
    """
    Compiles src/java/* into jar
    """

    t0 = time.time()

    print()
    print('Compile')

    print('  get dep jars')
    self.depJars = []
    for dep in self.deps:
      if type(dep) is tuple:
        self.depJars.append(getJAR(*((LUCENE_ROOT,)+dep)))
    
    coreSources = getJavaSources('src/java/')
    if os.path.exists(self.jarFile):
      maxModTime = max([x[1] for x in coreSources])
      if maxModTime < os.path.getmtime(self.jarFile):
        print('  no changes')
        return

    classesDir = 'build/classes/java/'
    changedSources = getChangedSources(coreSources, classesDir)
    if len(changedSources) != 0:
      classPath = []
      classPath.extend(self.depJars)
      for dep in self.deps:
        if type(dep) is not tuple:
          classPath.append(dep)
      classPath.append(classesDir)
      classPath = dedup(classPath)
      javac(classPath, ['src/java/%s' % x for x in changedSources], classesDir)
      print('    %.2f sec' % (time.time()-t0))

    print('  jar %s' % self.jarFile)
    t0 = time.time()
    jar(classesDir, self.jarFile)
    print('    %.2f sec' % (time.time()-t0))

  def compileTest(self):

    print()
    print('Compile test')
    t0 = time.time()

    testSources = getJavaSources('src/test/')
    classesDir = 'build/classes/test/'
    changedSources = getChangedSources(testSources, classesDir)
    if len(changedSources) > 0:
      testClassPath = []
      testClassPath.append(os.path.abspath('build/classes/test'))
      print('  get dep jars')
      for dep in self.compileTestDeps:
        if type(dep) is tuple:
          testClassPath.append(getJAR(*(LUCENE_ROOT,)+dep))
        else:
          testClassPath.append(os.path.abspath(dep))
      testClassPath = dedup(testClassPath)
      javac(testClassPath, ['src/test/%s' % x for x in changedSources], classesDir)
      print('  %.2f sec' % (time.time()-t0))

      for destZipFile, entries in self.testPackages:
        # TODO: be incremental here
        self.buildZip(destZipFile, entries)
        
    else:
      print('  no changes')

  def buildZip(self, zipFile, entries):
    dir = os.path.split(zipFile)[0]
    if not os.path.exists(dir):
      os.makedirs(dir)
      
    tmpZipFile = zipFile + '.tmp'
    if os.path.exists(tmpZipFile):
      os.remove(tmpZipFile)
    
    zf = zipfile.ZipFile(tmpZipFile, 'w')
    try:
      for arcName, fileName in entries:
        zf.write(fileName, arcName, zipfile.ZIP_STORED)
    finally:
      zf.close()
    if os.path.exists(zipFile):
      os.remove(zipFile)
    os.rename(tmpZipFile, zipFile)

  def package(self, files=None):
    print()
    print('Package')
    if os.path.exists('dist'):
      shutil.rmtree('dist')
    os.makedirs('dist')
    t0 = time.time()
    name = '%s-%s' % (self.name, self.version)

    # nocommit call self.buildZip
    zipFile = 'dist/%s.zip' % name
    tmpZipFile = zipFile + '.tmp'

    if files is None:
      files = []
      for file in self.depJars:
        files.append(('%s/lib/%s' % (name, os.path.split(file)[1]), file))
      files.append(('%s/lib/%s' % (name, os.path.split(self.jarFile)[1]), self.jarFile))
      
    zf = zipfile.ZipFile(tmpZipFile, 'w')
    try:
      for arcName, fileName in files:
        zf.write(fileName, arcName, zipfile.ZIP_STORED)
    finally:
      zf.close()
    os.rename(tmpZipFile, zipFile)
    print('  %s: %d bytes' % (zipFile, os.path.getsize(zipFile)))
    print('  %.1f sec' % (time.time()-t0))

  def runTests(self, onlyTest):

    print()
    print('Run tests:')

    testClasses = findTests('build/classes/test/')

    if onlyTest is not None:
      l = []
      for cl in testClasses:
        if cl.endswith(onlyTest[0]):
          l.append((cl, onlyTest[1]))

      if len(l) == 0:
        raise RuntimeError('could not locate test %s' % onlyTest[0])
      testClasses = l

    testClassPath = []
    testClassPath.append(os.path.abspath('build/classes/test'))
    for dep in self.runTestDeps:
      if type(dep) is tuple:
        testClassPath.append(getJAR(*(LUCENE_ROOT,)+dep))
      else:
        testClassPath.append(os.path.abspath(dep))
    testClassPath = dedup(testClassPath)
    return runTests(testClassPath, testClasses, self.jvmCount, self.seed, self.locale, self.tz, self.fileencoding)

