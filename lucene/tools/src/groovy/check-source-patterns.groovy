/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Task script that is called by Ant's build.xml file:
 * Checks that there are no @author javadoc tags, tabs, 
 * svn keywords, javadoc-style licenses, or nocommits.
 */

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.rat.Defaults;
import org.apache.rat.document.impl.FileDocument;
import org.apache.rat.api.MetaData;

def extensions = [
  'java', 'jflex', 'py', 'pl', 'g4', 'jj', 'html', 'js',
  'css', 'xml', 'xsl', 'vm', 'sh', 'cmd', 'bat', 'policy',
  'properties', 'mdtext', 'groovy',
  'template', 'adoc', 'json', 'md'
];
def invalidPatterns = [
  (~$/@author\b/$) : '@author javadoc tag',
  (~$/(?i)\bno(n|)commit\b/$) : 'nocommit',
  (~$/\bTOOD:/$) : 'TOOD instead TODO',
  (~$/\t/$) : 'tabs instead spaces',
  (~$/[\u202A-\u202E\u2066-\u2069]/$) : 'misuse of RTL/LTR (https://trojansource.codes)',
  (~$/\Q/**\E((?:\s)|(?:\*))*\Q{@inheritDoc}\E((?:\s)|(?:\*))*\Q*/\E/$) : '{@inheritDoc} on its own is unnecessary',
  (~$/\$$(?:LastChanged)?Date\b/$) : 'svn keyword',
  (~$/\$$(?:(?:LastChanged)?Revision|Rev)\b/$) : 'svn keyword',
  (~$/\$$(?:LastChangedBy|Author)\b/$) : 'svn keyword',
  (~$/\$$(?:Head)?URL\b/$) : 'svn keyword',
  (~$/\$$Id\b/$) : 'svn keyword',
  (~$/\$$Header\b/$) : 'svn keyword',
  (~$/\$$Source\b/$) : 'svn keyword',
  (~$/^\uFEFF/$) : 'UTF-8 byte order mark',
  (~$/import java\.lang\.\w+;/$) : 'java.lang import is unnecessary'
]

// Python and others merrily use var declarations, this is a problem _only_ in Java at least for 8x where we're forbidding var declarations
def invalidJavaOnlyPatterns = [
  (~$/\n\s*var\s+.*=.*<>.*/$) : 'Diamond operators should not be used with var',
  (~$/\n\s*var\s+/$) : 'var is not allowed in until we stop development on the 8x code line'
]


def baseDir = properties['basedir'];
def baseDirLen = baseDir.length() + 1;

def found = 0;
def violations = new TreeSet();
def reportViolation = { f, name ->
  task.log(name + ': ' + f.toString().substring(baseDirLen).replace(File.separatorChar, (char)'/'), Project.MSG_ERR);
  violations.add(name);
  found++;
}

def javadocsPattern = ~$/(?sm)^\Q/**\E(.*?)\Q*/\E/$;
def javaCommentPattern = ~$/(?sm)^\Q/*\E(.*?)\Q*/\E/$;
def xmlCommentPattern = ~$/(?sm)\Q<!--\E(.*?)\Q-->\E/$;
def lineSplitter = ~$/[\r\n]+/$;
def singleLineSplitter = ~$/\r?\n/$;
def licenseMatcher = Defaults.createDefaultMatcher();
def validLoggerPattern = ~$/(?s)\b(private\s|static\s|final\s){3}+\s*Logger\s+\p{javaJavaIdentifierStart}+\s+=\s+\QLoggerFactory.getLogger(MethodHandles.lookup().lookupClass());\E/$;
def validLoggerNamePattern = ~$/(?s)\b(private\s|static\s|final\s){3}+\s*Logger\s+log+\s+=\s+\QLoggerFactory.getLogger(MethodHandles.lookup().lookupClass());\E/$;
def packagePattern = ~$/(?m)^\s*package\s+org\.apache.*;/$;
def xmlTagPattern = ~$/(?m)\s*<[a-zA-Z].*/$;
def sourceHeaderPattern = ~$/\[source\b.*/$;
def blockBoundaryPattern = ~$/----\s*/$;
def blockTitlePattern = ~$/\..*/$;
def unescapedSymbolPattern = ~$/(?<=[^\\]|^)([-=]>|<[-=])/$; // SOLR-10883
def extendsLuceneTestCasePattern = ~$/public.*?class.*?extends.*?LuceneTestCase[^\n]*?\n/$;
def validSPINameJavadocTag = ~$/(?s)\s*\*\s*@lucene\.spi\s+\{@value #NAME\}/$;

def isLicense = { matcher, ratDocument ->
  licenseMatcher.reset();
  return lineSplitter.split(matcher.group(1)).any{ licenseMatcher.match(ratDocument, it) };
}

def checkLicenseHeaderPrecedes = { f, description, contentPattern, commentPattern, text, ratDocument ->
  def contentMatcher = contentPattern.matcher(text);
  if (contentMatcher.find()) {
    def contentStartPos = contentMatcher.start();
    def commentMatcher = commentPattern.matcher(text);
    while (commentMatcher.find()) {
      if (isLicense(commentMatcher, ratDocument)) {
        if (commentMatcher.start() < contentStartPos) {
          break; // This file is all good, so break loop: license header precedes 'description' definition
        } else {
          reportViolation(f, description+' declaration precedes license header');
        }
      }
    }
  }
}

def checkMockitoAssume = { f, text ->
  if (text.contains("mockito") && !text.contains("assumeWorkingMockito()")) {
    reportViolation(f, 'File uses Mockito but has no assumeWorkingMockito() call');
  }
}

def checkForUnescapedSymbolSubstitutions = { f, text ->
  def inCodeBlock = false;
  def underSourceHeader = false;
  def lineNumber = 0;
  singleLineSplitter.split(text).each {
    ++lineNumber;
    if (underSourceHeader) { // This line is either a single source line, or the boundary of a code block
      inCodeBlock = blockBoundaryPattern.matcher(it).matches();
      if ( ! blockTitlePattern.matcher(it).matches()) {
        underSourceHeader = false;
      }
    } else {
      if (inCodeBlock) {
        inCodeBlock = ! blockBoundaryPattern.matcher(it).matches();
      } else {
        underSourceHeader = sourceHeaderPattern.matcher(it).lookingAt();
        if ( ! underSourceHeader) {
          def unescapedSymbolMatcher = unescapedSymbolPattern.matcher(it);
          if (unescapedSymbolMatcher.find()) {
            reportViolation(f, 'Unescaped symbol "' + unescapedSymbolMatcher.group(1) + '" on line #' + lineNumber);
          }
        }
      }
    }
  }
}

ant.fileScanner{
  fileset(dir: baseDir){
    extensions.each{
      include(name: 'lucene/**/*.' + it)
      include(name: 'solr/**/*.' + it)
      include(name: 'dev-tools/**/*.' + it)
      include(name: '*.' + it)
    }
    // TODO: For now we don't scan txt files, so we
    // check licenses in top-level folders separately:
    include(name: '*.txt')
    include(name: '*/*.txt')
    // excludes:
    exclude(name: '**/build/**')
    exclude(name: '**/dist/**')
    exclude(name: 'lucene/benchmark/work/**')
    exclude(name: 'lucene/benchmark/temp/**')
    exclude(name: '**/CheckLoggingConfiguration.java')
    exclude(name: 'lucene/tools/src/groovy/check-source-patterns.groovy') // ourselves :-)
    exclude(name: 'solr/core/src/test/org/apache/hadoop/**')
  }
}.each{ f ->
  task.log('Scanning file: ' + f, Project.MSG_VERBOSE);
  def text = f.getText('UTF-8');
  invalidPatterns.each{ pattern,name ->
    if (pattern.matcher(text).find()) {
      reportViolation(f, name);
    }
  }
  def javadocsMatcher = javadocsPattern.matcher(text);
  def ratDocument = new FileDocument(f);
  while (javadocsMatcher.find()) {
    if (isLicense(javadocsMatcher, ratDocument)) {
      reportViolation(f, String.format(Locale.ENGLISH, 'javadoc-style license header [%s]',
        ratDocument.getMetaData().value(MetaData.RAT_URL_LICENSE_FAMILY_NAME)));
    }
  }
  if (f.name.endsWith('.java')) {
    if (text.contains('org.slf4j.LoggerFactory') && ! f.name.equals("ErrorLogMuter.java")) {
      if (!validLoggerPattern.matcher(text).find()) {
        reportViolation(f, 'invalid logging pattern [not private static final, uses static class name]');
      }
      if (!validLoggerNamePattern.matcher(text).find()) {
        reportViolation(f, 'invalid logger name [log, uses static class name, not specialized logger]')
      }
    }
    // make sure that SPI names of all tokenizers/charfilters/tokenfilters are documented
    if (!f.name.contains("Test") && !f.name.contains("Mock") && !text.contains("abstract class") &&
        !f.name.equals("TokenizerFactory.java") && !f.name.equals("CharFilterFactory.java") && !f.name.equals("TokenFilterFactory.java") &&
        (f.name.contains("TokenizerFactory") && text.contains("extends TokenizerFactory") ||
            f.name.contains("CharFilterFactory") && text.contains("extends CharFilterFactory") ||
            f.name.contains("FilterFactory") && text.contains("extends TokenFilterFactory"))) {
      if (!validSPINameJavadocTag.matcher(text).find()) {
        reportViolation(f, 'invalid spi name documentation')
      }
    }
    checkLicenseHeaderPrecedes(f, 'package', packagePattern, javaCommentPattern, text, ratDocument);
    if (f.name.contains("Test")) {
      checkMockitoAssume(f, text);
    }

    if (f.path.substring(baseDirLen).contains("solr/")
        && f.name.equals("SolrTestCase.java") == false
        && f.name.equals("TestXmlQParser.java") == false) {
      if (extendsLuceneTestCasePattern.matcher(text).find()) {
        reportViolation(f, "Solr test cases should extend SolrTestCase rather than LuceneTestCase");
      }
    }
    invalidJavaOnlyPatterns.each { pattern,name ->
      if (pattern.matcher(text).find()) {
        reportViolation(f, name);
      }
    }
  }
  if (f.name.endsWith('.xml') || f.name.endsWith('.xml.template')) {
    checkLicenseHeaderPrecedes(f, '<tag>', xmlTagPattern, xmlCommentPattern, text, ratDocument);
  }
  if (f.name.endsWith('.adoc')) {
    checkForUnescapedSymbolSubstitutions(f, text);
  }
};

if (found) {
  throw new BuildException(String.format(Locale.ENGLISH, 'Found %d violations in source files (%s).',
    found, violations.join(', ')));
}
