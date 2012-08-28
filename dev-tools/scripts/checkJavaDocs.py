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
import os
import re

reHREF = re.compile('<a.*?>(.*?)</a>', re.IGNORECASE)

reMarkup = re.compile('<.*?>')
reDivBlock = re.compile('<div class="block">(.*?)</div>', re.IGNORECASE)
reCaption = re.compile('<caption><span>(.*?)</span>', re.IGNORECASE)
reTDLastNested = re.compile('<td class="colLast"><code><strong><a href="[^>]*\.([^>]*?)\.html" title="class in[^>]*">', re.IGNORECASE)
reTDLast = re.compile('<td class="colLast"><code><strong><a href="[^>]*#([^>]*?)">', re.IGNORECASE)
reColOne = re.compile('<td class="colOne"><code><strong><a href="[^>]*#([^>]*?)">', re.IGNORECASE)

def cleanHTML(s):
  s = reMarkup.sub('', s)
  s = s.replace('&nbsp;', ' ')
  s = s.replace('&lt;', '<')
  s = s.replace('&gt;', '>')
  s = s.replace('&amp;', '&')
  return s.strip()

def checkClass(fullPath):
  # TODO: only works with java7 generated javadocs now!
  f = open(fullPath, encoding='UTF-8')
  
  missing = []
  inThing = False
  lastCaption = None
  lastItem = None

  desc = None
  
  for line in f.readlines():
    m = reCaption.search(line)
    if m is not None:
      lastCaption = m.group(1)
      #print('    caption %s' % lastCaption)
    m = reTDLastNested.search(line)
    if m is not None:
      # nested classes
      lastItem = m.group(1)
      #print('      item %s' % lastItem)
    else:
      m = reTDLast.search(line)
      if m is not None:
        # methods etc
        # TODO: this is actually the link anchor for the method, which we must
        # somehow defer and only later check if the list at that anchor does not contain
        # the text 'specified by' (in which case its an overridden method from an external library)
        lastItem = m.group(1)
      else:
        # ctors etc
        m = reColOne.search(line)
        if m is not None:
          lastItem = m.group(1)
          #print('      item %s' % lastItem)

    lineLower = line.strip().lower()

    if lineLower.find('<tr class="') != -1:
      inThing = True
      hasDesc = False
      continue

    if inThing:
      if lineLower.find('</tr>') != -1:
        if not hasDesc:
          missing.append((lastCaption, lastItem))
        inThing = False
        continue
      else:
        if line.find('<div class="block">') != -1:
          desc = []
        if desc is not None:
          desc.append(line)
          if line.find('</div>') != -1:
            desc = ''.join(desc)
            desc = desc.replace('<div class="block">', '')
            desc = desc.replace('</div>', '')
            desc = desc.strip()
            hasDesc = len(desc) > 0
            desc = None
  f.close()
  if len(missing) > 0:
    print()
    print(fullPath)
    for (caption, item) in missing:
      print('  missing %s: %s' % (caption, item))
    return True
  else:
    return False
  
def checkSummary(fullPath):
  printed = False
  f = open(fullPath, encoding='UTF-8')
  anyMissing = False
  sawPackage = False
  desc = []
  lastHREF = None
  for line in f.readlines():
    lineLower = line.strip().lower()
    if desc is not None:
      # TODO: also detect missing description in overview-summary
      if lineLower.startswith('package ') or lineLower.startswith('<h1 title="package" '):
        sawPackage = True
      elif sawPackage:
        if lineLower.startswith('<table ') or lineLower.startswith('<b>see: '):
          desc = ' '.join(desc)
          desc = reMarkup.sub(' ', desc)
          desc = desc.strip()
          if desc == '':
            if not printed:
              print()
              print(fullPath)
              printed = True
            print('  no package description (missing package.html in src?)')
            anyMissing = True
          desc = None
        else:
          desc.append(lineLower)
      
    if lineLower in ('<td>&nbsp;</td>', '<td></td>', '<td class="collast">&nbsp;</td>'):
      if not printed:
        print()
        print(fullPath)
        printed = True
      print('  missing: %s' % unescapeHTML(lastHREF))
      anyMissing = True
    elif lineLower.find('licensed to the apache software foundation') != -1 or lineLower.find('copyright 2004 the apache software foundation') != -1:
      if not printed:
        print()
        print(fullPath)
        printed = True
      print('  license-is-javadoc: %s' % unescapeHTML(lastHREF))
      anyMissing = True
    m = reHREF.search(line)
    if m is not None:
      lastHREF = m.group(1)
  if desc is not None and fullPath.find('/overview-summary.html') == -1:
    raise RuntimeError('BUG: failed to locate description in %s' % fullPath)
  f.close()
  return anyMissing

def unescapeHTML(s):
  s = s.replace('&lt;', '<')
  s = s.replace('&gt;', '>')
  s = s.replace('&amp;', '&')
  return s

def checkPackageSummaries(root, level='class'):
  """
  Just checks for blank summary lines in package-summary.html; returns
  True if there are problems.
  """

  if level != 'class' and level != 'package' and level != 'method':
    print('unsupported level: %s, must be "class" or "package" or "method"' % level)
    sys.exit(1)
  
  #for dirPath, dirNames, fileNames in os.walk('%s/lucene/build/docs/api' % root):

  if False:
    os.chdir(root)
    print()
    print('Run "ant javadocs" > javadocs.log...')
    if os.system('ant javadocs > javadocs.log 2>&1'):
      print('  FAILED')
      sys.exit(1)
    
  anyMissing = False
  for dirPath, dirNames, fileNames in os.walk(root):

    if dirPath.find('/all/') != -1:
      # These are dups (this is a bit risk, eg, root IS this /all/ directory..)
      continue

    if 'package-summary.html' in fileNames:
      if level != 'package' and checkSummary('%s/package-summary.html' % dirPath):
        anyMissing = True
      if level == 'method': 
        for fileName in fileNames:
          fullPath = '%s/%s' % (dirPath, fileName)
          if not fileName.startswith('package-') and fileName.endswith('.html') and os.path.isfile(fullPath):
            if checkClass(fullPath):
               anyMissing = True
    if 'overview-summary.html' in fileNames:        
      if checkSummary('%s/overview-summary.html' % dirPath):
        anyMissing = True

  return anyMissing

if __name__ == '__main__':
  if len(sys.argv) < 2 or len(sys.argv) > 3:
    print('usage: %s <dir> [class|package|method]' % sys.argv[0])
    sys.exit(1)
  if len(sys.argv) == 2:
    level = 'class'
  else:
    level = sys.argv[2]
  if checkPackageSummaries(sys.argv[1], level):
    print()
    print('Missing javadocs were found!')
    sys.exit(1)
  sys.exit(0)
