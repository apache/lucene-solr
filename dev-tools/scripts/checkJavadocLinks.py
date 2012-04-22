import traceback
import os
import sys
import re
from HTMLParser import HTMLParser, HTMLParseError
import urlparse

reHyperlink = re.compile(r'<a(\s+.*?)>', re.I)
reAtt = re.compile(r"""(?:\s+([a-z]+)\s*=\s*("[^"]*"|'[^']?'|[^'"\s]+))+""", re.I)

# silly emacs: '

class FindHyperlinks(HTMLParser):

  def __init__(self, baseURL):
    HTMLParser.__init__(self)
    self.anchors = set()
    self.links = []
    self.baseURL = baseURL
    self.printed = False

  def handle_starttag(self, tag, attrs):
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
            print '    WARNING: anchor "%s" appears more than once' % name
        else:
          self.anchors.add(name)
      elif href is not None:
        assert name is None
        self.links.append(urlparse.urljoin(self.baseURL, href))
      else:
        if self.baseURL.endswith('/AttributeSource.html'):
          # LUCENE-4010: AttributeSource's javadocs has an unescaped <A> generics!!  Seems to be a javadocs bug... (fixed in Java 7)
          pass
        else:
          raise RuntimeError('BUG: %s' % attrs)

  def printFile(self):
    if not self.printed:
      print
      print '  ' + self.baseURL
      self.printed = True
                   
def parse(baseURL, html):
  parser = FindHyperlinks(baseURL)
  try:
    parser.feed(html)
    parser.close()
  except HTMLParseError:
    parser.printFile()
    print '  WARNING: failed to parse:'
    traceback.print_exc()
    return [], []
  
  #print '    %d links, %d anchors' % \
  #      (len(parser.links), len(parser.anchors))
  return parser.links, parser.anchors

def checkAll(dirName):
  """
  Checks *.html (recursively) under this directory.
  """

  # Find/parse all HTML files first
  print
  print 'Crawl/parse...'
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
        fullPath = os.path.join(root, f)
        #print '  %s' % fullPath
        allFiles[fullPath] = parse(fullPath, open('%s/%s' % (root, f)).read())

  # ... then verify:
  print
  print 'Verify...'
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

      idx = link.find('?')
      if idx != -1:
        link = link[:idx]
        
      # TODO: normalize path sep for windows...
      if link.startswith('http://') or link.startswith('https://'):
        # don't check external links
        pass
      elif link not in allFiles:
        # We only load HTML... so if the link is another resource (eg
        # SweetSpotSimilarity refs
        # lucene/build/docs/misc/org/apache/lucene/misc/doc-files/ss.gnuplot) then it's OK:
        if not os.path.exists(link):
          if not printed:
            printed = True
            print
            print fullPath
          print '  BROKEN LINK: %s' % link
      elif anchor is not None and anchor not in allFiles[link][1]:
        if not printed:
          printed = True
          print
          print fullPath
        print '  BROKEN ANCHOR: %s' % origLink
        
if __name__ == '__main__':
  checkAll(sys.argv[1])
  
