import json
import threading
import http.client
import urllib.request, urllib.error, urllib.parse
import subprocess
import shutil

shutil.rmtree('index')
shutil.rmtree('taxonomy')

def send(command, data):
  u = urllib.request.urlopen('http://localhost:4000/%s' % command, data)
  print('did open')
  print(u.read())

def readServerOutput(p, startupEvent, failureEvent):
  while True:
    l = p.stdout.readline()
    if l == '':
      failureEvent.set()
      startupEvent.set()
      raise RuntimeError('Server failed to start')
    if l.find('listening on port 4000') != -1:
      startupEvent.set()
    print('SVR: %s' % l.rstrip())

startupEvent = threading.Event()
failureEvent = threading.Event()

p = subprocess.Popen('java -cp /home/mike/.ivy2/cache/net.minidev/json-smart/jars/json-smart-1.1.1.jar:/home/mike/.ivy2/cache/org.apache.lucene/lucene-facet/jars/lucene-facet-4.1-SNAPSHOT.jar:/home/mike/.ivy2/cache/org.apache.lucene/lucene-highlighter/jars/lucene-highlighter-4.1-SNAPSHOT.jar:/home/mike/.ivy2/cache/io.netty/netty/bundles/netty-3.5.11.Final.jar:/home/mike/.ivy2/cache/org.apache.lucene/lucene-core/jars/lucene-core-4.1-SNAPSHOT.jar:/home/mike/.ivy2/cache/org.apache.lucene/lucene-analyzers-common/jars/lucene-analyzers-common-4.1-SNAPSHOT.jar:/l/server/build/luceneserver.jar org.apache.lucene.server.Server', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

t = threading.Thread(target=readServerOutput, args=(p, startupEvent, failureEvent))
t.setDaemon(True)
t.start()

startupEvent.wait()
if failureEvent.isSet():
  sys.exit(0)

print('Server started...')

fields = {'date': {'type': 'atom',
                   'index': True,
                   'store': True}}

send('registerFields', json.dumps(fields))

print('Done register')

h = http.client.HTTPConnection('localhost', 4000)
h.request('POST', '/addDocument')
h.endheaders()

for i in range(100):
  h.send('{"date": "foobar"}')
r = h.getresponse()
print(r.read())
