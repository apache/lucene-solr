import types
import re
import time
import os
import shutil
import sys
import cPickle
import datetime

# TODO
#   - build wiki/random index as needed (balanced or not, varying # segs, docs)
#   - verify step
#   - run searches
#   - get all docs query in here

if sys.platform.lower().find('darwin') != -1:
  osName = 'osx'
elif sys.platform.lower().find('win') != -1:
  osName = 'windows'
elif sys.platform.lower().find('linux') != -1:
  osName = 'linux'
else:
  osName = 'unix'

TRUNK_DIR = '/lucene/clean'
FLEX_DIR = '/lucene/flex.branch'

DEBUG = False

# let shell find it:
JAVA_COMMAND = 'java -Xms2048M -Xmx2048M -Xbatch -server'
#JAVA_COMMAND = 'java -Xms1024M -Xmx1024M -Xbatch -server -XX:+AggressiveOpts -XX:CompileThreshold=100 -XX:+UseFastAccessorMethods'

INDEX_NUM_THREADS = 1

INDEX_NUM_DOCS = 5000000

LOG_DIR = 'logs'

DO_BALANCED = False

if osName == 'osx':
  WIKI_FILE = '/x/lucene/enwiki-20090724-pages-articles.xml.bz2'
  INDEX_DIR_BASE = '/lucene'
else:
  WIKI_FILE = '/x/lucene/enwiki-20090724-pages-articles.xml.bz2'
  INDEX_DIR_BASE = '/x/lucene'

if DEBUG:
  NUM_ROUND = 0
else:
  NUM_ROUND = 7

if 0:
  print 'compile...'
  if '-nocompile' not in sys.argv:
    if os.system('ant compile > compile.log 2>&1') != 0:
      raise RuntimeError('compile failed (see compile.log)')

BASE_SEARCH_ALG = '''
analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
work.dir = $INDEX$
search.num.hits = $NUM_HITS$
query.maker=org.apache.lucene.benchmark.byTask.feeds.FileBasedQueryMaker
file.query.maker.file = queries.txt
print.hits.field = $PRINT_FIELD$
log.queries=true
log.step=100000

$OPENREADER$
{"XSearchWarm" $SEARCH$}

# Turn off printing, after warming:
SetProp(print.hits.field,)

$ROUNDS$
CloseReader 
RepSumByPrefRound XSearch
'''

BASE_INDEX_ALG = '''
analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer

$OTHER$
deletion.policy = org.apache.lucene.index.NoDeletionPolicy
doc.tokenized = false
doc.body.tokenized = true
doc.stored = true
doc.body.stored = false
doc.term.vector = false
log.step.AddDoc=10000

directory=FSDirectory
autocommit=false
compound=false

work.dir=$WORKDIR$

{ "BuildIndex"
  - CreateIndex
  $INDEX_LINE$
  - CommitIndex(dp0)
  - CloseIndex
  $DELETIONS$
}

RepSumByPrefRound BuildIndex
'''

class RunAlgs:

  def __init__(self, resultsPrefix):
    self.counter = 0
    self.results = []
    self.fOut = open('%s.txt' % resultsPrefix, 'wb')
    
  def makeIndex(self, label, dir, source, numDocs, balancedNumSegs=None, deletePcts=None):

    if source not in ('wiki', 'random'):
      raise RuntimeError('source must be wiki or random')

    if dir is not None:
      fullDir = '%s/contrib/benchmark' % dir
      if DEBUG:
        print '  chdir %s' % fullDir
      os.chdir(fullDir)

    indexName = '%s.%s.nd%gM' % (source, label, numDocs/1000000.0)
    if balancedNumSegs is not None:
      indexName += '_balanced%d' % balancedNumSegs
    fullIndexPath = '%s/%s' % (INDEX_DIR_BASE, indexName)
    
    if os.path.exists(fullIndexPath):
      print 'Index %s already exists...' % fullIndexPath
      return indexName

    print 'Now create index %s...' % fullIndexPath

    s = BASE_INDEX_ALG

    if source == 'wiki':
      other = '''doc.index.props = true
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
docs.file=%s
''' % WIKI_FILE
      #addDoc = 'AddDoc(1024)'
      addDoc = 'AddDoc'
    else:
      other = '''doc.index.props = true
content.source=org.apache.lucene.benchmark.byTask.feeds.SortableSingleDocSource
'''
      addDoc = 'AddDoc'
    if INDEX_NUM_THREADS > 1:
      #other += 'doc.reuse.fields=false\n'
      s = s.replace('$INDEX_LINE$', '[ { "AddDocs" %s > : %s } : %s' % \
                    (addDoc, numDocs/INDEX_NUM_THREADS, INDEX_NUM_THREADS))
    else:
      s = s.replace('$INDEX_LINE$', '{ "AddDocs" %s > : %s' % \
                    (addDoc, numDocs))

    s = s.replace('$WORKDIR$', fullIndexPath)

    if deletePcts is not None:
      dp = '# Do deletions\n'
      dp += 'OpenReader(false)\n'
      for pct in deletePcts:
        if pct != 0:
          dp += 'DeleteByPercent(%g)\n' % pct
          dp += 'CommitIndex(dp%g)\n' % pct
      dp += 'CloseReader()\n'
    else:
      dp = ''

    s = s.replace('$DELETIONS$', dp)

    if balancedNumSegs is not None:
      other += '''  merge.factor=1000
  max.buffered=%d
  ram.flush.mb=2000
  ''' % (numDocs/balancedNumSegs)
    else:
      if source == 'random':
        other += 'ram.flush.mb=1.0\n'
      else:
        other += 'ram.flush.mb=32.0\n'

    s = s.replace('$OTHER$', other)

    try:
      self.runOne(dir, s, 'index_%s' % indexName, isIndex=True)
    except:
      if os.path.exists(fullIndexPath):
        shutil.rmtree(fullIndexPath)
      raise
    return indexName
    
  def getLogPrefix(self, **dArgs):
    l = dArgs.items()
    l.sort()
    s = '_'.join(['%s=%s' % tup for tup in l])
    s = s.replace(' ', '_')
    s = s.replace('"', '_')
    return s
             
  def runOne(self, dir, alg, logFileName, expectedMaxDocs=None, expectedNumDocs=None, queries=None, verify=False, isIndex=False):

    fullDir = '%s/contrib/benchmark' % dir
    if DEBUG:
      print '  chdir %s' % fullDir
    os.chdir(fullDir)
               
    if queries is not None:
      if type(queries) in types.StringTypes:
        queries = [queries]
      open('queries.txt', 'wb').write('\n'.join(queries))

    if DEBUG:
      algFile = 'tmp.alg'
    else:
      algFile = 'tmp.%s.alg' % os.getpid()
    open(algFile, 'wb').write(alg)

    fullLogFileName = '%s/contrib/benchmark/%s/%s' % (dir, LOG_DIR, logFileName)
    print '  log: %s' % fullLogFileName
    if not os.path.exists(LOG_DIR):
      print '  mkdir %s' % LOG_DIR
      os.makedirs(LOG_DIR)

    command = '%s -classpath ../../build/classes/java:../../build/classes/demo:../../build/contrib/highlighter/classes/java:lib/commons-digester-1.7.jar:lib/commons-collections-3.1.jar:lib/commons-compress-1.0.jar:lib/commons-logging-1.0.4.jar:lib/commons-beanutils-1.7.0.jar:lib/xerces-2.10.0.jar:lib/xml-apis-2.10.0.jar:../../build/contrib/benchmark/classes/java org.apache.lucene.benchmark.byTask.Benchmark %s > "%s" 2>&1' % (JAVA_COMMAND, algFile, fullLogFileName)

    if DEBUG:
      print 'command=%s' % command
      
    try:
      t0 = time.time()
      if os.system(command) != 0:
        raise RuntimeError('FAILED')
      t1 = time.time()
    finally:
      if not DEBUG:
        os.remove(algFile)

    if isIndex:
      s = open(fullLogFileName, 'rb').read()
      if s.find('Exception in thread "') != -1 or s.find('at org.apache.lucene') != -1:
        raise RuntimeError('alg hit exceptions')
      return

    else:

      # Parse results:
      bestQPS = None
      count = 0
      nhits = None
      numDocs = None
      maxDocs = None
      warmTime = None
      r = re.compile('^  ([0-9]+): (.*)$')
      topN = []

      for line in open(fullLogFileName, 'rb').readlines():
        m = r.match(line.rstrip())
        if m is not None:
          topN.append(m.group(2))
        if line.startswith('totalHits = '):
          nhits = int(line[12:].strip())
        if line.startswith('maxDoc()  = '):
          maxDocs = int(line[12:].strip())
        if line.startswith('numDocs() = '):
          numDocs = int(line[12:].strip())
        if line.startswith('XSearchWarm'):
          v = line.strip().split()
          warmTime = float(v[5])
        if line.startswith('XSearchReal'):
          v = line.strip().split()
          # print len(v), v
          upto = 0
          i = 0
          qps = None
          while i < len(v):
            if v[i] == '-':
              i += 1
              continue
            else:
              upto += 1
              i += 1
              if upto == 5:
                qps = float(v[i-1].replace(',', ''))
                break

          if qps is None:
            raise RuntimeError('did not find qps')

          count += 1
          if bestQPS is None or qps > bestQPS:
            bestQPS = qps

      if not verify:
        if count != NUM_ROUND:
          raise RuntimeError('did not find %s rounds (got %s)' % (NUM_ROUND, count))
        if warmTime is None:
          raise RuntimeError('did not find warm time')
      else:
        bestQPS = 1.0
        warmTime = None

      if nhits is None:
        raise RuntimeError('did not see "totalHits = XXX"')

      if maxDocs is None:
        raise RuntimeError('did not see "maxDoc() = XXX"')

      if maxDocs != expectedMaxDocs:
        raise RuntimeError('maxDocs() mismatch: expected %s but got %s' % (expectedMaxDocs, maxDocs))

      if numDocs is None:
        raise RuntimeError('did not see "numDocs() = XXX"')

      if numDocs != expectedNumDocs:
        raise RuntimeError('numDocs() mismatch: expected %s but got %s' % (expectedNumDocs, numDocs))
      
      return nhits, warmTime, bestQPS, topN

  def getAlg(self, indexPath, searchTask, numHits, deletes=None, verify=False, printField=''):

    s = BASE_SEARCH_ALG
    s = s.replace('$PRINT_FIELD$', 'doctitle')

    if not verify:
      s = s.replace('$ROUNDS$',
  '''                
  { "Rounds"
    { "Run"
      { "TestSearchSpeed"
        { "XSearchReal" $SEARCH$ > : 3.0s
      }
      NewRound
    } : %d
  } 
  ''' % NUM_ROUND)
    else:
      s = s.replace('$ROUNDS$', '')

    if deletes is None:
      s = s.replace('$OPENREADER$', 'OpenReader')
    else:
      s = s.replace('$OPENREADER$', 'OpenReader(true,dp%g)' % deletes)
    s = s.replace('$INDEX$', indexPath)
    s = s.replace('$SEARCH$', searchTask)
    s = s.replace('$NUM_HITS$', str(numHits))
    
    return s

  def compare(self, baseline, new, *params):

    if new[0] != baseline[0]:
      raise RuntimeError('baseline found %d hits but new found %d hits' % (baseline[0], new[0]))

    qpsOld = baseline[2]
    qpsNew = new[2]
    pct = 100.0*(qpsNew-qpsOld)/qpsOld
    print '  diff: %.1f%%' % pct
    self.results.append((qpsOld, qpsNew, params))

    self.fOut.write('|%s|%.2f|%.2f|%.1f%%|\n' % \
                    ('|'.join(str(x) for x in params),
                     qpsOld, qpsNew, pct))
    self.fOut.flush()

  def save(self, name):
    f = open('%s.pk' % name, 'wb')
    cPickle.dump(self.results, f)
    f.close()

def verify(r1, r2):
  if r1[0] != r2[0]:
    raise RuntimeError('different total hits: %s vs %s' % (r1[0], r2[0]))
                       
  h1 = r1[3]
  h2 = r2[3]
  if len(h1) != len(h2):
    raise RuntimeError('different number of results')
  else:
    for i in range(len(h1)):
      s1 = h1[i].replace('score=NaN', 'score=na').replace('score=0.0', 'score=na')
      s2 = h2[i].replace('score=NaN', 'score=na').replace('score=0.0', 'score=na')
      if s1 != s2:
        raise RuntimeError('hit %s differs: %s vs %s' % (i, s1 ,s2))

def usage():
  print
  print 'Usage: python -u %s -run <name> | -report <name>' % sys.argv[0]
  print
  print '  -run <name> runs all tests, saving results to file <name>.pk'
  print '  -report <name> opens <name>.pk and prints Jira table'
  print '  -verify confirm old & new produce identical results'
  print
  sys.exit(1)

def main():

  if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

  if '-run' in sys.argv:
    i = sys.argv.index('-run')
    mode = 'run'
    if i < len(sys.argv)-1:
      name = sys.argv[1+i]
    else:
      usage()
  elif '-report' in sys.argv:
    i = sys.argv.index('-report')
    mode = 'report'
    if i < len(sys.argv)-1:
      name = sys.argv[1+i]
    else:
      usage()
  elif '-verify' in sys.argv:
    mode = 'verify'
    name = None
  else:
    usage()

  if mode in ('run', 'verify'):
    run(mode, name)
  else:
    report(name)

def report(name):

  print '||Query||Deletes %||Tot hits||QPS old||QPS new||Pct change||'

  results = cPickle.load(open('%s.pk' % name))
  for qpsOld, qpsNew, params in results:
    pct = 100.0*(qpsNew-qpsOld)/qpsOld
    if pct < 0.0:
      c = 'red'
    else:
      c = 'green'

    params = list(params)

    query = params[0]
    if query == '*:*':
      query = '<all>'
    params[0] = query
    
    pct = '{color:%s}%.1f%%{color}' % (c, pct)
    print '|%s|%.2f|%.2f|%s|' % \
          ('|'.join(str(x) for x in params),
           qpsOld, qpsNew, pct)

def run(mode, name):

  for dir in (TRUNK_DIR, FLEX_DIR):
    dir = '%s/contrib/benchmark' % dir
    print '"ant compile" in %s...' % dir
    os.chdir(dir)
    if os.system('ant compile') != 0:
      raise RuntimeError('ant compile failed')
  
  r = RunAlgs(name)

  if not os.path.exists(WIKI_FILE):
    print
    print 'ERROR: wiki source file "%s" does not exist' % WIKI_FILE
    print
    sys.exit(1)

  print
  print 'JAVA:\n%s' % os.popen('java -version 2>&1').read()
    
  print
  if osName != 'windows':
    print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
  else:
    print 'OS:\n%s' % sys.platform

  deletePcts = (0.0, 0.1, 1.0, 10)

  indexes = {}
  for rev in ('baseline', 'flex'):
    if rev == 'baseline':
      dir = TRUNK_DIR
    else:
      dir = FLEX_DIR
    source = 'wiki'
    indexes[rev] = r.makeIndex(rev, dir, source, INDEX_NUM_DOCS, deletePcts=deletePcts)

  doVerify = mode == 'verify'
  source = 'wiki'
  numHits = 10

  queries = (
    'body:[tec TO tet]',
    'real*',
    '1',
    '2',
    '+1 +2',
    '+1 -2',
    '1 2 3 -4',
    '"world economy"')

  for query in queries:

    for deletePct in deletePcts:

      print '\nRUN: query=%s deletes=%g%% nhits=%d' % \
            (query, deletePct, numHits)

      maxDocs = INDEX_NUM_DOCS
      numDocs = int(INDEX_NUM_DOCS * (1.0-deletePct/100.))

      prefix = r.getLogPrefix(query=query, deletePct=deletePct)
      indexPath = '%s/%s' % (INDEX_DIR_BASE, indexes['baseline'])

      # baseline (trunk)
      s = r.getAlg(indexPath,
                   'Search',
                   numHits,
                   deletes=deletePct,
                   verify=doVerify,
                   printField='doctitle')
      baseline = r.runOne(TRUNK_DIR, s, 'baseline_%s' % prefix, maxDocs, numDocs, query, verify=doVerify)

      # flex
      indexPath = '%s/%s' % (INDEX_DIR_BASE, indexes['flex'])
      s = r.getAlg(indexPath,
                   'Search',
                   numHits,
                   deletes=deletePct,
                   verify=doVerify,
                   printField='doctitle')
      flex = r.runOne(FLEX_DIR, s, 'flex_%s' % prefix, maxDocs, numDocs, query, verify=doVerify)

      print '  %d hits' % flex[0]

      verify(baseline, flex)

      if mode == 'run' and not DEBUG:
        r.compare(baseline, flex,
                  query, deletePct, baseline[0])
        r.save(name)

def cleanScores(l):
  for i in range(len(l)):
    pos = l[i].find(' score=')
    l[i] = l[i][:pos].strip()

if __name__ == '__main__':
  main()
