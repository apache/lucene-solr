import re
import copy
import os
import codecs
import random
import time
import socket
import threading
import subprocess
import pickle
import traceback
import sys
import signal

import util

# nocommit ok?
#signal.signal(signal.SIGCHLD, signal.SIG_IGN)

"""
One instance of this runs, per machine that will launch tests.  This
instance spawns multiple sub-processes and manages communications with
them, back to the main server.
"""

def unescape(chunk):
  chunk = chunk.replace('\\n', '\n')
  chunk = chunk.replace('\\t', '\t')
  chunk = chunk.replace('%0A', '\n')
  chunk = chunk.replace('%09', '\t')
  chunk = chunk.replace('\\\"', '"')
  return chunk

reReproduceWith = re.compile('NOTE: reproduce with: ant test (.*?)$', re.MULTILINE)
reArg = re.compile('^-D(.*?)=(.*?)$')

testArgMap = {
  'testcase': 'test',
  'tests.method': 'method',
  'tests.seed': 'seed',
  'tests.locale': 'locale',
  'tests.timezone': 'tz',
  'tests.file.encoding': 'fileencoding',
  }
  
def fixReproduceWith(s):
  upto = 0
  l = []
  while True:
    m = reReproduceWith.search(s, upto)
    if m is None:
      l.append(s[upto:])
      break

    s0 = m.group(1).strip()
    #print 'FIX: %s' % s0
    args = s0.split()
    testClass = None
    testMethod = None
    newArgs = []
    for a in args:
      m2 = reArg.match(a)
      if m2 is None:
        raise RuntimeError('failed to parse "%s" from output "%s"' % (a, s0))
      newArg = testArgMap.get(m2.group(1))
      value = m2.group(2)
      if newArg is None:
        raise RuntimeError('failed to map test arg "%s" from output "%s"' % (a, s0))
      if newArg == 'test':
        testClass = value
      elif newArg == 'method':
        testMethod = value
      else:
        newArgs.append('-%s %s' % (newArg, value))

    if testMethod is not None:
      testClass += '.' + testMethod 

    l.append('NOTE: reproduce with: python -u build.py %s %s' % (testClass, ' '.join(newArgs)))
    upto = m.end(0)

  return ''.join(l)

class Child(threading.Thread) :

  """
  Interacts with one child test runner.
  """

  def __init__(self, id, doPrintDirect, parent):
    threading.Thread.__init__(self)
    self.id = id
    self.doPrintDirect = doPrintDirect
    self.parent = parent

  def run(self):

    jvmDir = 'build/test/J%s' % self.id
    if not os.path.exists(jvmDir):
      os.makedirs(jvmDir)

    eventsFile = os.path.abspath('build/test/J%s/events' % self.id)
    if os.path.exists(eventsFile):
      os.remove(eventsFile)

    cmd = '%s -eventsfile %s' % (self.parent.command.replace('-DtempDir=.', '-DtempDir=. -Djunit4.childvm.cwd=. -Djunit4.childvm.id=%s' % self.id), eventsFile)

    if util.VERBOSE:
      print('TEST cmd: %s' % cmd)
      print('CLASSPATH: %s' % self.parent.env['CLASSPATH'])

    #print 'cmd %s' % cmd
    #print 'CP %s' % self.parent.env['CLASSPATH']
    
    # TODO
    #   - add -Dtests.seed=XXX, eg -Dtests.seed=771F118CC53F329
    #   - add -eventsfile /l/lucene.trunk/lucene/build/core/test/junit4-J0-0819129977b5076df.events @/l/lucene.trunk/lucene/build/core/test/junit4-J0-1916253054fa0d84f.suites

    try:
      #self.parent.remotePrint('C%d init' % self.id)

      # TODO
      p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, env=self.parent.env, cwd=jvmDir)
      #self.parent.remotePrint('C%d subprocess started' % self.id)

      events = ReadEvents(p, eventsFile, self.parent)
      #self.parent.remotePrint('C%d startup0 done' % self.id)
      events.waitIdle()

      #self.parent.remotePrint('startup done C%d' % self.id)
      
      while True:
        job = self.parent.nextJob()
        if job is None:
          #self.parent.remotePrint('C%d no more jobs' % self.id)
          #p.stdin.close()
          p.kill()
          break
        #self.parent.remotePrint('C%d: job %s' % (self.id, job))
        p.stdin.write((job + '\n').encode('utf-8'))
        p.stdin.flush()
        #print('J%s: start job %s' % (self.id, job))

        endSuite = False
        output = []
        failed = False
        msec = None
        while True:
          l = events.readline()
          if l.find('"IDLE",') != -1:
            #print 'J%s: idle @ %s' % (self.id, events.f.tell())
            break

          #if l.find('"chunk": ') != -1 or l.find('"bytes": ') != -1:
          if l.find('"chunk": ') != -1:
            l = l.strip()[10:-1]
            chunk = l.strip()
            chunk = unescape(chunk)
            chunk = fixReproduceWith(chunk)
            if self.doPrintDirect:
              print('  %s' % chunk.replace('\n', '\n  ').strip())
            output.append(chunk.strip())
              
          if l.find('"trace": ') != -1:
            chunk = l.strip().replace('"trace": "', '')[:-2]
            chunk = unescape(chunk)
            chunk = fixReproduceWith(chunk)
            if self.doPrintDirect:
              print('  %s' % chunk.replace('\n', '\n  ').strip())
            output.append(chunk)
            if chunk.find('AssumptionViolatedException') == -1:
              failed = True
            
          if l.find('"SUITE_COMPLETED"') != -1:
            endSuite = True
          elif endSuite and l.find('"executionTime"') != -1:
            msec = int(l.strip()[:-1].split()[1])
            #break

        self.parent.sendResult((self.id, job, msec, output, failed))

      stdout, stderr = p.communicate()
      if stdout is not None and len(stdout) != 0:
        self.parent.remotePrint('CHILD had stdout/err:\n%s' % stdout)
        
    except:
      self.parent.remotePrint('C%d: EXC:\n%s' % (self.id, traceback.format_exc()))

class ReadEvents:

  def __init__(self, process, fileName, parent):
    self.process = process
    self.fileName = fileName
    self.parent = parent
    while True:
      try:
        self.f = open(self.fileName, 'rb')
      except IOError:
        time.sleep(.001)
      else:
        break
    self.f.seek(0)
    
  def readline(self):
    while True:
      pos = self.f.tell()
      l = self.f.readline().decode('utf-8')
      if l == '' or not l.endswith('\n'):
        time.sleep(.01)
        p = self.process.poll()
        if p is not None:
          raise RuntimeError('process exited with status %s' % str(p))
        self.f.seek(pos)
      else:
        #self.parent.remotePrint('readline=%s' % l.strip())
        return l

  def waitIdle(self):
    lines = []
    while True:
      l = self.readline()
      if l.find('"IDLE",') != -1:
        return lines
      else:
        lines.append(l)
    
class Parent:

  def __init__(self, processCount, env, command, jobs):
    #os.chdir('build/test')
    self.env = env
    self.command = command
    self.children = []
    self.jobLock = threading.Lock()
    self.jobUpto = 0
    self.jobs = jobs
    self.anyFail = False

    for childID in range(processCount):
      child = Child(childID, processCount == 1, self)
      child.start()
      # Silly: subprocess.Popen seems to hang if we launch too quickly
      time.sleep(.001)
      self.children.append(child)

    for child in self.children:
      child.join()

    # self.remotePrint('all children done')

  def remotePrint(self, message):
    with self.jobLock:
      print(message)

  def sendResult(self, result):
    childID, job, msec, output, failed = result
    job = job.replace('org.apache.lucene.server.', '')
    with self.jobLock:
      if not failed:
        print()
        if msec is None:
          print('PASS [J%s]: %s' % (childID, job))
        else:
          print('PASS [J%s]: %s [%.1f sec]' % (childID, job, msec/1000.))
      else:
        self.anyFail = True
        print()
        if msec is not None:
          print('FAIL [J%s]: %s [%.1f sec]' % (childID, job, msec/1000.))
        else:
          print('FAIL [J%s]: %s' % (childID, job))
      if len(self.children) > 1:
        for x in output:
          print('  %s' % x.replace('\n', '\n  ').strip())
      
  def nextJob(self):
    with self.jobLock:
      if self.jobUpto == len(self.jobs):
        job = None
      else:
        job = self.jobs[self.jobUpto]
        self.jobUpto += 1
      return job
