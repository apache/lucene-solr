import sys
import os
import shutil

VERSION = '0.1-SNAPSHOT'

sys.path.insert(0, '../../src/python')
import util

SERVER_JAR = '../../build/lucene-server-%s.jar' % util.VERSION

deps = [
  ('org.apache.tika', 'tika-app', '1.3'),
  ('net.minidev', 'json-smart', '1.1.1'),
  ('org.codehaus.jackson', 'jackson-core-asl', '1.9.12'),
  ('org.codehaus.jackson', 'jackson-mapper-asl', '1.9.12'),
  SERVER_JAR,
  ]

compileTestDeps = [
  ('junit', 'junit', '4.10'),
  ('com.carrotsearch.randomizedtesting', 'junit4-ant', '2.0.9'),
  ('com.carrotsearch.randomizedtesting', 'randomizedtesting-runner', '2.0.9'),
  ('org.apache.lucene', 'lucene-codecs', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-test-framework', util.LUCENE_VERSION),
  '../../build/classes/test/',
  ]

compileTestDeps.extend((
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
  ('org.apache.lucene', 'lucene-facet', util.LUCENE_VERSION),
  ('org.apache.lucene', 'lucene-core', util.LUCENE_VERSION),
  ('net.minidev', 'json-smart', '1.1.1'),
  ('org.codehaus.jackson', 'jackson-core-asl', '1.9.12'),
  ('org.codehaus.jackson', 'jackson-mapper-asl', '1.9.12'),
  ))

compileTestDeps.append(SERVER_JAR)

coreDeps = [
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
  ('org.apache.lucene', 'lucene-join', util.LUCENE_VERSION),
  ('com.ibm.icu', 'icu4j', '49.1'),
  ('net.minidev', 'json-smart', '1.1.1'),
  ('org.codehaus.jackson', 'jackson-core-asl', '1.9.12'),
  ('org.codehaus.jackson', 'jackson-mapper-asl', '1.9.12'),
  ]

coreCompileTestDeps = [
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

runTestDeps = coreCompileTestDeps + coreDeps + [
  ('org.apache.lucene', 'lucene-codecs', util.LUCENE_VERSION),
  SERVER_JAR,
  '../../build/classes/test',
  ]

def main():

  b = util.Build('BinaryDocument', VERSION, deps, compileTestDeps, runTestDeps,
                 packageFiles=(
                     ('BinaryDocument/BinaryDocument-%s.jar' % VERSION, 'build/BinaryDocument-%s.jar' % VERSION),
                     ('BinaryDocument/lib/tika-app-1.3.jar', util.getJAR(util.LUCENE_ROOT, 'org.apache.tika', 'tika-app', '1.3')),
                     ))

  # We always package if running tests because tests install the
  # plugin from the zip file:
  if b.doTest:
    b.doPackage = True
  
  if not b.coreDone:
    util.pushDir('../..')
    util.run('python -u build.py compile-test')
    util.popDir()

  b.run()

if __name__ == '__main__':
  main()
