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
reJ8Caption = re.compile('<h3>(.*?) Summary</h3>')
reTDLastNested = re.compile('^<td class="colLast"><code><strong><a href="[^>]*\.([^>]*?)\.html" title="class in[^>]*">', re.IGNORECASE)
reTDLast = re.compile('^<td class="colLast"><code><strong><a href="[^>]*#([^>]*?)">', re.IGNORECASE)
reColOne = re.compile('^<td class="colOne"><code><strong><a href="[^>]*#([^>]*?)">', re.IGNORECASE)
reMemberNameLink = re.compile('^<td class="colLast"><code><span class="memberNameLink"><a href="[^>]*#([^>]*?)"', re.IGNORECASE)
reNestedClassMemberNameLink = re.compile('^<td class="colLast"><code><span class="memberNameLink"><a href="[^>]*?".*?>(.*?)</a>', re.IGNORECASE)
reMemberNameOneLink = re.compile('^<td class="colOne"><code><span class="memberNameLink"><a href="[^>]*#([^>]*?)"', re.IGNORECASE)

# the Method detail section at the end
reMethodDetail = re.compile('^<h3>Method Detail</h3>$', re.IGNORECASE)
reMethodDetailAnchor = re.compile('^(?:</a>)?<a name="([^>]*?)">$', re.IGNORECASE)
reMethodOverridden = re.compile('^<dt><strong>(Specified by:|Overrides:)</strong></dt>$', re.IGNORECASE)

reTag = re.compile("(?i)<(\/?\w+)((\s+\w+(\s*=\s*(?:\".*?\"|'.*?'|[^'\">\s]+))?)+\s*|\s*)\/?>")

def verifyHTML(s):

  stack = []
  upto = 0
  while True:
    m = reTag.search(s, upto)
    if m is None:
      break
    tag = m.group(1)
    upto = m.end(0)

    if tag[:1] == '/':
      justTag = tag[1:]
    else:
      justTag = tag
      
    if justTag.lower() in ('br', 'li', 'p', 'col'):
      continue

    if tag[:1] == '/':
      if len(stack) == 0:
        raise RuntimeError('saw closing "%s" without opening <%s...>' % (m.group(0), tag[1:]))
      elif stack[-1][0] != tag[1:].lower():
        raise RuntimeError('closing "%s" does not match opening "%s"' % (m.group(0), stack[-1][1]))
      stack.pop()
    else:
      stack.append((tag.lower(), m.group(0)))

  if len(stack) != 0:
    raise RuntimeError('"%s" was never closed' % stack[-1][1])

def cleanHTML(s):
  s = reMarkup.sub('', s)
  s = s.replace('&nbsp;', ' ')
  s = s.replace('&lt;', '<')
  s = s.replace('&gt;', '>')
  s = s.replace('&amp;', '&')
  return s.strip()

reH3 = re.compile('^<h3>(.*?)</h3>', re.IGNORECASE | re.MULTILINE)
reH4 = re.compile('^<h4>(.*?)</h4>', re.IGNORECASE | re.MULTILINE)
reDetailsDiv = re.compile('<div class="details">')
reEndOfClassData = re.compile('<!--.*END OF CLASS DATA.*-->')
reBlockList = re.compile('<ul class="blockList(?:Last)?">')
reCloseUl = re.compile('</ul>')

def checkClassDetails(fullPath):
  """
  Checks for invalid HTML in the full javadocs under each field/method.
  """

  # TODO: only works with java7 generated javadocs now!
  with open(fullPath, encoding='UTF-8') as f:
    desc = []
    cat = None
    item = None
    errors = []
    inDetailsDiv = False
    blockListDepth = 0
    for line in f.readlines():
      # Skip content up until  <div class="details">
      if not inDetailsDiv:
        if reDetailsDiv.match(line) is not None:
          inDetailsDiv = True
        continue

      # Stop looking at content at closing details </div>, which is just before <!-- === END OF CLASS DATA === -->
      if reEndOfClassData.match(line) is not None:
        if len(desc) != 0:
          try:
            verifyHTML(''.join(desc))
          except RuntimeError as re:
            #print('    FAILED: %s' % re)
            errors.append((cat, item, str(re)))
        break

      # <ul class="blockList(Last)"> is the boundary between items
      if reBlockList.match(line) is not None:
        blockListDepth += 1
        if len(desc) != 0:
          try:
            verifyHTML(''.join(desc))
          except RuntimeError as re:
            #print('    FAILED: %s' % re)
            errors.append((cat, item, str(re)))
          del desc[:]

      if blockListDepth == 3:
        desc.append(line)

      if reCloseUl.match(line) is not None:
        blockListDepth -= 1
      else:
        m = reH3.search(line)
        if m is not None:
          cat = m.group(1)
        else:
          m = reH4.search(line)
          if m is not None:
            item = m.group(1)

  if len(errors) != 0:
    print()
    print(fullPath)
    for cat, item, message in errors:
      print('  broken details HTML: %s: %s: %s' % (cat, item, message))
    return True
  else:
    return False

def checkClassSummaries(fullPath):
  #print("check %s" % fullPath)

  # TODO: only works with java7 generated javadocs now!
  f = open(fullPath, encoding='UTF-8')

  missing = []
  broken = []
  inThing = False
  lastCaption = None
  lastItem = None

  desc = None

  foundMethodDetail = False
  lastMethodAnchor = None
  lineCount = 0
  
  for line in f.readlines():
    m = reMethodDetail.search(line)
    lineCount += 1
    if m is not None:
      foundMethodDetail = True
      #print('  got method detail')
      continue

    # prune methods that are just @Overrides of other interface/classes,
    # they should be specified elsewhere, if they are e.g. jdk or 
    # external classes we cannot inherit their docs anyway
    if foundMethodDetail:
      m = reMethodDetailAnchor.search(line)
      if m is not None:
        lastMethodAnchor = m.group(1)
        continue
      isOverrides = '>Overrides:<' in line or '>Specified by:<' in line
      #print('check for removing @overridden method: %s; %s; %s' % (lastMethodAnchor, isOverrides, missing))
      if isOverrides and ('Methods', lastMethodAnchor) in missing:
        #print('removing @overridden method: %s' % lastMethodAnchor)
        missing.remove(('Methods', lastMethodAnchor))

    m = reCaption.search(line)
    if m is not None:
      lastCaption = m.group(1)
      #print('    caption %s' % lastCaption)
    else:
      m = reJ8Caption.search(line)
      if m is not None:
        lastCaption = m.group(1)
        if not lastCaption.endswith('s'):
          lastCaption += 's'
        #print('    caption %s' % lastCaption)

    # Try to find the item in question (method/member name):
    for matcher in (reTDLastNested, # nested classes
                    reTDLast, # methods etc.
                    reColOne, # ctors etc.
                    reMemberNameLink, # java 8
                    reNestedClassMemberNameLink, # java 8, nested class
                    reMemberNameOneLink): # java 8 ctors
      m = matcher.search(line)
      if m is not None:
        lastItem = m.group(1)
        #print('  found item %s; inThing=%s' % (lastItem, inThing))
        break

    lineLower = line.strip().lower()

    if lineLower.find('<tr class="') != -1 or lineLower.find('<tr id="') != -1:
      inThing = True
      hasDesc = False
      continue

    if inThing:
      if lineLower.find('</tr>') != -1:
        #print('  end item %s; hasDesc %s' % (lastItem, hasDesc))
        if not hasDesc:
          if lastItem is None:
            raise RuntimeError('failed to locate javadoc item in %s, line %d? last line: %s' % (fullPath, lineCount, line.rstrip()))
          missing.append((lastCaption, unEscapeURL(lastItem)))
          #print('    add missing; now %d: %s' % (len(missing), str(missing)))
        inThing = False
        continue
      else:
        if line.find('<div class="block">') != -1:
          desc = []
        if desc is not None:
          desc.append(line)
          if line.find('</div>') != -1:
            desc = ''.join(desc)

            try:
              verifyHTML(desc)
            except RuntimeError as e:
              broken.append((lastCaption, lastItem, str(e)))
              #print('FAIL: %s: %s: %s: %s' % (lastCaption, lastItem, e, desc))
                            
            desc = desc.replace('<div class="block">', '')
            desc = desc.replace('</div>', '')
            desc = desc.strip()
            hasDesc = len(desc) > 0
            #print('   thing %s: %s' % (lastItem, desc))

            desc = None
  f.close()
  if len(missing) > 0 or len(broken) > 0:
    print()
    print(fullPath)
    for (caption, item) in missing:
      print('  missing %s: %s' % (caption, item))
    for (caption, item, why) in broken:
      print('  broken HTML: %s: %s: %s' % (caption, item, why))
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
        if lineLower.startswith('<table ') or lineLower.startswith('<b>see: ') or lineLower.startswith('<p>see:'):
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
      print('  missing description: %s' % unescapeHTML(lastHREF))
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

def unEscapeURL(s):
  # Not exhaustive!!
  s = s.replace('%20', ' ')
  return s

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

  if level != 'class' and level != 'package' and level != 'method' and level != 'none':
    print('unsupported level: %s, must be "class" or "package" or "method" or "none"' % level)
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
  if not os.path.isdir(root):
    checkClassSummaries(root)
    checkClassDetails(root)
    sys.exit(0)
    
  for dirPath, dirNames, fileNames in os.walk(root):

    if dirPath.find('/all/') != -1:
      # These are dups (this is a bit risk, eg, root IS this /all/ directory..)
      continue

    if 'package-summary.html' in fileNames:
      if (level == 'class' or level == 'method') and checkSummary('%s/package-summary.html' % dirPath):
        anyMissing = True
      for fileName in fileNames:
        fullPath = '%s/%s' % (dirPath, fileName)
        if not fileName.startswith('package-') and fileName.endswith('.html') and os.path.isfile(fullPath):
          if level == 'method':
            if checkClassSummaries(fullPath):
              anyMissing = True
          # always look for broken html, regardless of level supplied
          if checkClassDetails(fullPath):
            anyMissing = True
              
    if 'overview-summary.html' in fileNames:        
      if level != 'none' and checkSummary('%s/overview-summary.html' % dirPath):
        anyMissing = True

  return anyMissing

if __name__ == '__main__':
  if len(sys.argv) < 2 or len(sys.argv) > 3:
    print('usage: %s <dir> [none|package|class|method]' % sys.argv[0])
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
