import time
import threading
import sys
import json
import asyncore
import socket
import random

class RollingStats:

  def __init__(self, count):
    self.buffer = [0] * count
    self.sum = 0
    self.upto = 0

  def add(self, value):
    if value < 0:
      raise RuntimeError('values should be positive')
    idx = self.upto % len(self.buffer)
    self.sum += value - self.buffer[idx]
    self.buffer[idx] = value
    self.upto += 1

  def get(self):
    if self.upto == 0:
      return -1.0
    else:
      if self.upto < len(self.buffer):
        v = float(self.sum)/self.upto
      else:
        v = float(self.sum)/len(self.buffer)
      # Don't let roundoff error manifest as -0.0:
      return max(0.0, v)

class HTTPClient(asyncore.dispatcher):

  def __init__(self, host, port, path, data, doneCallback):
    self.startTime = time.time()
    asyncore.dispatcher.__init__(self)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    l = ['POST %s HTTP/1.0' % path]
    l.append('Content-Length: %d' % len(data))
    self.buffer = '\r\n'.join(l) + '\r\n\r\n' + data
    self.result = []
    self.doneCallback = doneCallback
    self.connect((host, port))

  def handle_connect(self):
    pass
    
  def handle_close(self):
    self.close()
    self.doneCallback(time.time() - self.startTime)
      
  def handle_read(self):
    self.result.append(self.recv(8192))
    print('read %s' % self.result[-1])
    
  def writable(self):
    return (len(self.buffer) > 0)

  def handle_write(self):
    sent = self.send(self.buffer)
    self.buffer = self.buffer[sent:]

class Stats:

  def __init__(self):
    self.actualQPSStats = RollingStats(5)
    self.totalTimeStats = RollingStats(100)
    self.lastIntSec = None
    self.startTime = time.time()

  def queryDone(self, t):
    self.totalTimeStats.add(t)
    intSec = int(time.time()-self.startTime)
    if intSec != self.lastIntSec:
      if self.lastIntSec is not None:
        self.actualQPSStats.add(self.queriesThisSec)
      self.queriesThisSec = 0
      self.lastIntSec = intSec
    self.queriesThisSec += 1

def loop():
  while True:
    asyncore.loop()
    
def main(targetQPS):

  t = threading.Thread(target=loop, args=())
  t.setDaemon(True)
  t.start()

  r = random.Random(0)

  queryText = '1'
  query = {'indexName': 'wiki',
           'queryText': queryText,
           'facets': [{'path': 'dateFacet', 'topN': 10}]}
  data = json.dumps(query)

  targetTime = time.time()

  stats = Stats()
  lastPrintTime = time.time()
  while True:

    targetTime += r.expovariate(targetQPS)
    now = time.time()
    if now - lastPrintTime > 1.0:
      print('%.1f QPS, %.1f msec' % (stats.actualQPSStats.get(), 1000*stats.totalTimeStats.get()))
      lastPrintTime = time.time()
      
    pause = targetTime - time.time()
    if pause > 0:
      time.sleep(pause)
        
    HTTPClient('10.17.4.91', 4001, '/search', data, stats.queryDone)
    

if __name__ == '__main__':
  main(int(sys.argv[1]))
