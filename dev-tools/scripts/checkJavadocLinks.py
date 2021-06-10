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

import traceback
import os
import sys
import re
from html.parser import HTMLParser
import urllib.parse as urlparse

reHyperlink = re.compile(r'<a(\s+.*?)>', re.I)
reAtt = re.compile(r"""(?:\s+([a-z]+)\s*=\s*("[^"]*"|'[^']?'|[^'"\s]+))+""", re.I)

# Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF] /* any Unicode character, excluding the surrogate blocks, FFFE, and FFFF. */
reValidChar = re.compile("^[^\u0000-\u0008\u000B-\u000C\u000E-\u001F\uFFFE\uFFFF]*$")

# silly emacs: '

class FindHyperlinks(HTMLParser):

  def __init__(self, baseURL):
    HTMLParser.__init__(self)
    self.stack = []
    self.anchors = set()
    self.links = []
    self.baseURL = baseURL
    self.printed = False

  def handle_starttag(self, tag, attrs):
    # NOTE: I don't think 'a' should be in here. But try debugging 
    # NumericRangeQuery.html. (Could be javadocs bug, it's a generic type...)
    if tag not in ('link', 'meta', 'frame', 'br', 'wbr', 'hr', 'p', 'li', 'img', 'col', 'a'):
      self.stack.append(tag)
    if tag == 'a':
      name = None
      href = None
      for attName, attValue in attrs:
        if attName == 'name':
          name = attValue
        elif attName == 'href':
          href = attValue

      if name is not None:
        assert href is None
        if name in self.anchors:
          if name in ('serializedForm',
                      'serialized_methods',
                      'readObject(java.io.ObjectInputStream)',
                      'writeObject(java.io.ObjectOutputStream)') \
                      and self.baseURL.endswith('/serialized-form.html'):
            # Seems like a bug in Javadoc generation... you can't have
            # same anchor name more than once...
            pass
          else:
            self.printFile()
            raise RuntimeError('anchor "%s" appears more than once' % name)
        else:
          self.anchors.add(name)
      elif href is not None:
        assert name is None
        href = href.strip()
        self.links.append(urlparse.urljoin(self.baseURL, href))
      else:
        if self.baseURL.endswith('/AttributeSource.html'):
          # LUCENE-4010: AttributeSource's javadocs has an unescaped <A> generics!!  Seems to be a javadocs bug... (fixed in Java 7)
          pass
        else:
          raise RuntimeError('couldn\'t find an href nor name in link in %s: only got these attrs: %s' % (self.baseURL, attrs))

  def handle_endtag(self, tag):
    if tag in ('link', 'meta', 'frame', 'br', 'hr', 'p', 'li', 'img', 'col', 'a'):
      return
    
    if len(self.stack) == 0:
      raise RuntimeError('%s %s:%s: saw </%s> no opening <%s>' % (self.baseURL, self.getpos()[0], self.getpos()[1], tag, self.stack[-1]))

    if self.stack[-1] == tag:
      self.stack.pop()
    else:
      raise RuntimeError('%s %s:%s: saw </%s> but expected </%s>' % (self.baseURL, self.getpos()[0], self.getpos()[1], tag, self.stack[-1]))

  def printFile(self):
    if not self.printed:
      print()
      print('  ' + self.baseURL)
      self.printed = True
                   
def parse(baseURL, html):
  global failures
  # look for broken unicode
  if not reValidChar.match(html):
    print(' WARNING: invalid characters detected in: %s' % baseURL)
    failures = True
    return [], []

  parser = FindHyperlinks(baseURL)
  try:
    parser.feed(html)
    parser.close()
  except:
    # TODO: Python's html.parser is now always lenient, which is no good for us: we want correct HTML in our javadocs
    parser.printFile()
    print('  WARNING: failed to parse %s:' % baseURL)
    traceback.print_exc(file=sys.stdout)
    failures = True
    return [], []
  
  #print '    %d links, %d anchors' % \
  #      (len(parser.links), len(parser.anchors))
  return parser.links, parser.anchors

failures = False

def checkAll(dirName):
  """
  Checks *.html (recursively) under this directory.
  """

  global failures

  # Find/parse all HTML files first
  print()
  print('Crawl/parse...')
  allFiles = {}

  if os.path.isfile(dirName):
    root, fileName = os.path.split(dirName)
    iter = ((root, [], [fileName]),)
  else:
    iter = os.walk(dirName)

  for root, dirs, files in iter:
    for f in files:
      main, ext = os.path.splitext(f)
      ext = ext.lower()

      # maybe?:
      # and main not in ('serialized-form'):
      if ext in ('.htm', '.html') and \
         not f.startswith('.#') and \
         main not in ('deprecated-list',):
        # Somehow even w/ java 7 generaged javadocs,
        # deprecated-list.html can fail to escape generics types
        fullPath = os.path.join(root, f).replace(os.path.sep,'/')
        fullPath = 'file:%s' % urlparse.quote(fullPath)
        # parse and unparse the URL to "normalize" it
        fullPath = urlparse.urlunparse(urlparse.urlparse(fullPath))
        #print '  %s' % fullPath
        allFiles[fullPath] = parse(fullPath, open('%s/%s' % (root, f), encoding='UTF-8').read())

  # ... then verify:
  print()
  print('Verify...')
  for fullPath, (links, anchors) in allFiles.items():
    #print fullPath
    printed = False
    for link in links:

      origLink = link

      # TODO: use urlparse?
      idx = link.find('#')
      if idx != -1:
        anchor = link[idx+1:]
        link = link[:idx]
      else:
        anchor = None

      # remove any whitespace from the middle of the link
      link = ''.join(link.split())

      idx = link.find('?')
      if idx != -1:
        link = link[:idx]
        
      # TODO: normalize path sep for windows...
      if link.startswith('http://') or link.startswith('https://'):
        # don't check external links

        if link.find('lucene.apache.org/java/docs/mailinglists.html') != -1:
          # OK
          pass
        elif link == 'http://lucene.apache.org/core/':
          # OK
          pass
        elif link == 'http://lucene.apache.org/solr/':
          # OK
          pass
        elif link == 'http://lucene.apache.org/solr/resources.html':
          # OK
          pass
        elif link.find('lucene.apache.org/java/docs/discussion.html') != -1:
          # OK
          pass
        elif link.find('lucene.apache.org/core/discussion.html') != -1:
          # OK
          pass
        elif link.find('lucene.apache.org/solr/mirrors-solr-latest-redir.html') != -1:
          # OK
          pass
        elif link.find('lucene.apache.org/solr/guide/') != -1:
          # OK
          pass
        elif link.find('lucene.apache.org/solr/downloads.html') != -1:
          # OK
          pass
        elif (link.find('svn.apache.org') != -1
              or link.find('lucene.apache.org') != -1)\
             and os.path.basename(fullPath) != 'Changes.html':
          if not printed:
            printed = True
            print()
            print(fullPath)
          print('  BAD EXTERNAL LINK: %s' % link)
      elif link.startswith('mailto:'):
        if link.find('@lucene.apache.org') == -1 and link.find('@apache.org') != -1:
          if not printed:
            printed = True
            print()
            print(fullPath)
          print('  BROKEN MAILTO (?): %s' % link)
      elif link.startswith('javascript:'):
        # ok...?
        pass
      elif 'org/apache/solr/client/solrj/beans/Field.html' in link:
        # see LUCENE-4011: this is a javadocs bug for constants 
        # on annotations it seems?
        pass
      elif link.startswith('file:'):
        if link not in allFiles:
          filepath = urlparse.unquote(urlparse.urlparse(link).path)
          if not (os.path.exists(filepath) or os.path.exists(filepath[1:])):
            if not printed:
              printed = True
              print()
              print(fullPath)
            print('  BROKEN LINK: %s' % link)
      elif anchor is not None and anchor not in allFiles[link][1]:
        if not printed:
          printed = True
          print()
          print(fullPath)
        print('  BROKEN ANCHOR: %s' % origLink)
      else:
        if not printed:
          printed = True
          print()
          print(fullPath)
        print('  BROKEN URL SCHEME: %s' % origLink)
    failures = failures or printed

  return failures   

if __name__ == '__main__':
  if checkAll(sys.argv[1]):
    print()
    print('Broken javadocs links were found! Common root causes:')
    # please feel free to add to this list
    print('* A typo of some sort for manually created links.')
    print('* Public methods referencing non-public classes in their signature.')
    sys.exit(1)
  sys.exit(0)
  
