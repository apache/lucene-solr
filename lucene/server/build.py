import zipfile
import shutil
import time
import types
import sys
import os

# TODO
#   - package should have README
#   - can i send randomized runner output to stdout?
#   - windows

sys.path.insert(0, 'src/python')
import util

deps = [
  ('io.netty', 'netty', '3.6.5.Final'),
  # For Base64 encode/decode:
  ('commons-codec', 'commons-codec', '1.7'),
  ('org.apache.lucene', 'lucene-queryparser', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-misc', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-queries', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-highlighter', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-suggest', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-grouping', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-analyzers-common', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-analyzers-icu', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-facet', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-core', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-codecs', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-join', util.LUCENE_VERSION),
  ('com.ibm.icu', 'icu4j', '49.1'),
  ('net.minidev', 'json-smart', '1.1.1'),
  ('org.codehaus.jackson', 'jackson-core-asl', '1.9.12'),
  ('org.codehaus.jackson', 'jackson-mapper-asl', '1.9.12'),
  ]

compileTestDeps = [
  'build/lucene-server-%s.jar' % util.VERSION,
  ('io.netty', 'netty', '3.6.5.Final'),
  # For Base64 encode/decode:
  ('commons-codec', 'commons-codec', '1.7'),
  ('org.apache.lucene', 'lucene-test-framework', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-core', util.LUCENE_VERSION),
  ('net.minidev', 'json-smart', '1.1.1'),
  ('org.codehaus.jackson', 'jackson-core-asl', '1.9.12'),
  ('org.codehaus.jackson', 'jackson-mapper-asl', '1.9.12'),
  ('junit', 'junit', '4.10'),
  ('org.apache.lucene', 'lucene-suggest', util.LUCENE_VERSION),
  ('com.carrotsearch.randomizedtesting', 'junit4-ant', '2.0.9'),
  ('com.carrotsearch.randomizedtesting', 'randomizedtesting-runner', '2.0.9'),
  ]

runTestDeps = compileTestDeps + deps + [
  ('org.apache.lucene', 'lucene-codecs', util.LUCENE_VERSION),
  ]

def main():
  b = util.Build('lucene-server', util.VERSION, deps, compileTestDeps,
                 runTestDeps,
                 ('plugins/BinaryDocument',))
  b.addTestPackage('build/test/MockPlugin-0.1.zip',
                   [('Mock/org/apache/lucene/MockPlugin.class', 'build/classes/test/org/apache/lucene/server/MockPlugin.class'),
                    ('Mock/lucene-server-plugin.properties', 'src/test/org/apache/lucene/server/MockPlugin-lucene-server-plugin.properties'),
                    ('Mock/site/hello.txt', 'src/test/org/apache/lucene/server/MockPlugin-hello.txt')])
               
  b.run()
  
if __name__ == '__main__':
  main()
