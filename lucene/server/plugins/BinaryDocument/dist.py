import sys
import re
import zipfile
import os
import shutil

sys.path.insert(0, '../../src/python')
import util

util.run('ant jar')

if os.path.exists('dist'):
  shutil.rmtree('dist')
  
os.makedirs('dist')

reComment = re.compile('<!--.*?-->', re.DOTALL)
reDep = re.compile('<dependency(.*?)/>')

jars = []
s = open('ivy.xml', 'rb').read()
s = reComment.sub('', s)
for m in reDep.findall(s):
  print 'HERE: %s' % m

with zipfile.ZipFile('dist/BinaryDocument.zip', 'w') as zf:
  zf.write('build/BinaryDocumentPlugin.jar', 'BinaryDocument/BinaryDocumentPlugin.jar')
