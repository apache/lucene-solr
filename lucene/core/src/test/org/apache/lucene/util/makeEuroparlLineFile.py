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

import sys
import glob
import datetime
import tarfile
import re

try:
  sys.argv.remove('-verbose')
  VERBOSE = True
except ValueError:
  VERBOSE = False

try:
  sys.argv.remove('-docPerParagraph')
  docPerParagraph = True
except ValueError:
  docPerParagraph = False

reChapterOnly = re.compile('^<CHAPTER ID=.*?>$')
reTagOnly = re.compile('^<.*?>$')
reNumberOnly = re.compile(r'^\d+\.?$')

maxDoc = 0
didEnglish = False

def write(date, title, pending, fOut):
  global maxDoc
  body = ' '.join(pending).replace('\t', ' ').strip()
  if len(body) > 0:
    line = '%s\t%s\t%s\n' % (title, date, body)
    fOut.write(line)
    maxDoc += 1
    del pending[:]
    if VERBOSE:
      print len(body)

def processTar(fileName, fOut):

  global didEnglish

  t = tarfile.open(fileName, 'r:gz')
  for ti in t:
    if ti.isfile() and (not didEnglish or ti.name.find('/en/') == -1):

      tup = ti.name.split('/')
      lang = tup[1]
      year = int(tup[2][3:5])
      if year < 20:
        year += 2000
      else:
        year += 1900

      month = int(tup[2][6:8])
      day = int(tup[2][9:11])
      date = datetime.date(year=year, month=month, day=day)

      if VERBOSE:
        print
        print '%s: %s' % (ti.name, date)
      nextIsTitle = False
      title = None
      pending = []
      for line in t.extractfile(ti).readlines():
        line = line.strip()
        if reChapterOnly.match(line) is not None:
          if title is not None:
            write(date, title, pending, fOut)
          nextIsTitle = True
          continue
        if nextIsTitle:
          if not reNumberOnly.match(line) and not reTagOnly.match(line):
            title = line
            nextIsTitle = False
            if VERBOSE:
              print '  title %s' % line
          continue
        if line.lower() == '<p>':
          if docPerParagraph:
            write(date, title, pending, fOut)
          else:
            pending.append('PARSEP')
        elif not reTagOnly.match(line):
          pending.append(line)
      if title is not None and len(pending) > 0:
        write(date, title, pending, fOut)

  didEnglish = True
  
# '/x/lucene/data/europarl/all.lines.txt'
dirIn = sys.argv[1]
fileOut = sys.argv[2]
  
fOut = open(fileOut, 'wb')

for fileName in glob.glob('%s/??-??.tgz' % dirIn):
  if fileName.endswith('.tgz'):
    print 'process %s; %d docs so far...' % (fileName, maxDoc)
    processTar(fileName, fOut)

print 'TOTAL: %s' % maxDoc

#run something like this:
"""

# Europarl V5 makes 76,917 docs, avg 38.6 KB per
python -u europarl.py /x/lucene/data/europarl /x/lucene/data/europarl/tmp.lines.txt
shuf /x/lucene/data/europarl/tmp.lines.txt > /x/lucene/data/europarl/full.lines.txt
rm /x/lucene/data/europarl/tmp.lines.txt

# Run again, this time each paragraph is a doc:
# Europarl V5 makes 5,607,746 paragraphs (one paragraph per line), avg 620 bytes per:
python -u europarl.py /x/lucene/data/europarl /x/lucene/data/europarl/tmp.lines.txt -docPerParagraph
shuf /x/lucene/data/europarl/tmp.lines.txt > /x/lucene/data/europarl/para.lines.txt
rm /x/lucene/data/europarl/tmp.lines.txt

# ~5.5 MB gzip'd:
head -200 /x/lucene/data/europarl/full.lines.txt > tmp.txt
head -10000 /x/lucene/data/europarl/para.lines.txt >> tmp.txt
shuf tmp.txt > europarl.subset.txt
rm -f tmp.txt
gzip --best europarl.subset.txt
"""
