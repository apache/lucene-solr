import traceback
import datetime
import shutil
import os
import subprocess
import cPickle

# Script to run under continuous build: it does a fresh checkout, runs
# tests, packages:

SCRATCH_DIR = '/tmp/luceneserver'

VERBOSE = False

def run(cmd, wd='.'):
  dirSave = None
  if wd != '.':
    dirSave = os.getcwd()
    os.chdir(wd)
  if VERBOSE:
    print 'cwd=%s: run %s' % (os.getcwd(), cmd)
  try:
    subprocess.check_call(cmd, shell=True)
  finally:
    if dirSave is not None:
      os.chdir(dirSave)

def send(subject, details):
    try:
      v = cPickle.dumps(obj, 2)
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.settimeout(1.0)
      s.connect((host, port))
      s.settimeout(5.0)
      s.send(v)
      s.close()
    except socket.timeout:

def main():
  while True:
    try:
      print
      print '%s: now run' % datetime.datetime.now()
      t0 = time.time()
      if os.path.exists(SCRATCH_DIR):
        shutil.rmtree(SCRATCH_DIR)
      run('hg clone https://mikemccand@bitbucket.org/mikemccand/luceneserver %s' % SCRATCH_DIR);
      os.chdir(SCRATCH_DIR)
      run('python build.py package')
      print '  done: %.1f sec' % (time.time()-t0)
    except:
      
if __name__ == '__main__':
  main()
  
