"""
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
 
import os
import re

reClassJavadoc = re.compile(r'/\*\*(.*?)\*/\s+public\s(?:final\s)?class\s+(.*?)\s+extends\s+(.*?)\s+', re.DOTALL)
reNewlineStar = re.compile(r'\n\s*\*\s+', re.MULTILINE)
rePTag = re.compile(r'<p>', re.IGNORECASE)
reSentenceEnd = re.compile(r'\.\s')
reSpaces = re.compile(r'\s+')
reAt = re.compile(r'{@[^\s]+ (.*?)}')

def getFirstSentence(doc):
  doc = reNewlineStar.sub('\n', doc)
  doc = doc.strip()
  m = rePTag.search(doc, 2)
  if m is not None:
    doc = doc[:m.start(0)]
  m = reSentenceEnd.search(doc)
  if m is not None:
    doc = doc[:m.start(0)]
  doc = doc.strip()
  doc = doc.replace('\n', ' ')
  doc = reSpaces.sub(' ', doc)
  doc = reAt.sub(r'\1', doc)
  doc = doc.replace('"', '')
  return doc
  
def find(rootPath, classes):
  # TODO: crawl src jar instead?
  for root, dirs, files in os.walk(rootPath):

    if root.find('src/test/') != -1 or root.find('src/test-framework/') != -1:
      continue
    
    for file in files:
      if file.endswith('.java') and file.find('Test') == -1 and not file.startswith('.#'):
        className = file[:-5]
        subPath = root[len(rootPath)+1:].split('/')
        if subPath[0] == 'analysis':
          module = 'analyzers-%s' % subPath[1]
          package = '.'.join(subPath[4:])
        else:
          module = subPath[0]
          package = '.'.join(subPath[3:])
        
        #print 'CHECK: %s/%s' % (root[len(rootPath)+1:], file)
        #print '  %s: %s' % (module, package)
        s = open('%s/%s' % (root, file)).read()
        matches = reClassJavadoc.findall(s)
        
        if len(matches) > 0:
          if False and len(matches) != 1:
            raise RuntimeError('file %s has %d matches' % (file, len(matches)))
          if matches[0][0].find('Copyright 2004') != -1:
            i = s.find('Copyright 2004')
            matches = reClassJavadoc.findall(s[i:])
            
          if matches[0][1] != className:
            #print 'WARNING: skip %s/%s: class name mismatch' % (root, file)
            continue
          
          extendsClass = matches[0][-1]
          if extendsClass in classes:
            doc = getFirstSentence(matches[0][0])
            if False:
              print()
              print('%s (extends %s): %s' % (className, extendsClass, doc))
              print('  %s/%s' % (root, file))
            if className in classes[extendsClass]:
              raise RuntimeError('duplicate class %s' % className)
            classes[extendsClass][className] = (doc, module, package)
        else:
          #print '  no match'
          pass
          
rePolyEntry = re.compile(r'^\s+new PolyEntry\("(.*?)",\s*"(.*?)"[,\)]')
rePolyDecl = re.compile(r'^\s+new PolyType\((.*?)\.class,')

def main():
  analyzers = {}
  tokenizers = {}
  tokenFilters = {}
  similarities = {}
  suggesters = {}
  queries = {}
  classes = {'TokenFilter': tokenFilters,
             'FilteringTokenFilter': tokenFilters,
             'Query': queries,
             'MultiTermQuery': queries,
             'AutomatonQuery': queries,
             'KeywordMarkerFilter': tokenFilters,
             'Tokenizer': tokenizers,
             'CharTokenizer': tokenizers,
             'Lookup': suggesters,
             'AnalyzingSuggester': suggesters,
             'Analyzer': analyzers,
             'StopwordAnalyzerBase': analyzers,
             'Similarity': similarities,
             'TFIDFSimilarity': similarities,
             }

  # Get first-sentence desc from Lucene sources:
  find('/l/4x.pybooks/lucene', classes)
  find('src/java', classes)

  for root, dirs, files in os.walk('src/java'):
    for file in files:
      if file.endswith('.java') and not file.startswith('.#'):
        s = open('%s/%s' % (root, file)).read()
        lines = s.split('\n')
        changed = False
        for i in range(len(lines)):
          line = lines[i]
          m = rePolyDecl.search(line)
          if m is not None:
            clBase = m.group(1)
            if clBase == 'Object':
              print('WARNING: skip poly %s from %s' % (line.strip(), file))

          m = rePolyEntry.search(line)
          if m is not None and clBase != 'Object':
            className = m.group(1)
            if className in classes[clBase]:
              print('Add %s' % className)
              doc, module, package = classes[clBase][className]
              doc = '%s (see @lucene:%s:%s.%s)' % (doc, module, package, className)
              newLine = line[:m.start(2)] + doc + line[m.end(2):]
              if newLine != lines[i]:
                lines[i] = newLine
                changed = True
              del classes[clBase][className]
            else:
              #raise RuntimeError('base class %s doesn\'t have subclass %s' % (clBase, className))
              print('WARNING: skip subClass "%s" of base class %s' % (className, clBase))

        if changed:
          print('Rewrite: %s' % file)
          open('%s/%s' % (root, file), 'w').write('\n'.join(lines))

  # TODO: report on all the classes we are missing

  l = list(analyzers.keys())
  l.sort()
  print()
  print('Missing Analyzer:')
  for k in l:
    print('  %s' % k)

  l = list(tokenFilters.keys())
  l.sort()
  print()
  print('Missing TokenFilter:')
  for k in l:
    print('  %s' % k)

  l = list(queries.keys())
  l.sort()
  print()
  print('Missing Query:')
  for k in l:
    print('  %s' % k)
  
if __name__ == '__main__':
  main()

