package org.apache.lucene.gradle.checks
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

/**
 * Task that checks that there are no @author javadoc tags, tabs, 
 * svn keywords, javadoc-style licenses, or nocommits.
 */

import java.lang.invoke.MethodHandles

import org.apache.rat.Defaults
import org.apache.rat.api.MetaData
import org.apache.rat.document.impl.FileDocument
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.stream.Stream
import java.util.concurrent.atomic.AtomicInteger

class CheckSourcePatterns extends DefaultTask {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @InputDirectory
  String baseDir
  
  @TaskAction
  void check() {
    ant.lifecycleLogLevel = "INFO"
    def scanner = ant.fileScanner{
      fileset(dir: baseDir) {
        exts.each{
          include(name: '**/*.' + it)
        }
        // TODO: For now we don't scan txt files, so we
        // check licenses in top-level folders separately:
        include(name: '*.txt')
        include(name: '*/*.txt')
        // excludes:
        exclude(name: '**/build/**')
        exclude(name: '**/bin/**')
        exclude(name: '**/dist/**')
        exclude(name: '**/.out/**')
        exclude(name: '**/.settings/**')
        exclude(name: '**/work/**')
        exclude(name: '**/temp/**')
        exclude(name: '**/CheckLoggingConfiguration.java')
        exclude(name: '**/CheckSourcePatterns.groovy') // ourselves :-)
        exclude(name: '**/src/test/org/apache/hadoop/**')
      }
    }
    
    List<File> files = new ArrayList<>()
    scanner.each {files.add(it)}
    
    Stream.of(files.toArray())
    .parallel()
    .forEach({ f ->
      log.info ('Scanning file: ' + f)
      def text = f.getText('UTF-8')
      invalidPatterns.each{ pattern,name ->
        if (!f.name.endsWith(".txt") && pattern.matcher(text).find()) {
          reportViolation(f, name)
        }
      }
      def javadocsMatcher = javadocsPattern.matcher(text)
      def ratDocument = new FileDocument(f)
      while (javadocsMatcher.find()) {
        if (isLicense(javadocsMatcher, ratDocument)) {
          reportViolation(f, String.format(Locale.ENGLISH, 'javadoc-style license header [%s]',
              ratDocument.getMetaData().value(MetaData.RAT_URL_LICENSE_FAMILY_NAME)))
        }
      }
      if (f.name.endsWith('.java')) {
        if (text.contains('org.slf4j.LoggerFactory')) {
          if (!validLoggerPattern.matcher(text).find()) {
            reportViolation(f, 'invalid logging pattern [not private static final, uses static class name]')
          }
          if (!validLoggerNamePattern.matcher(text).find()) {
            reportViolation(f, 'invalid logger name [log, uses static class name, not specialized logger]')
          }
        }
        checkLicenseHeaderPrecedes(f, 'package', packagePattern, javaCommentPattern, text, ratDocument)
        if (f.name.contains("Test")) {
          checkMockitoAssume(f, text)
        }
        
        if (f.path.substring(baseDir.length() + 1).contains("solr/")
        && f.name.equals("SolrTestCase.java") == false
        && f.name.equals("TestXmlQParser.java") == false) {
          if (extendsLuceneTestCasePattern.matcher(text).find()) {
            reportViolation(f, "Solr test cases should extend SolrTestCase rather than LuceneTestCase")
          }
        }
      }
      if (f.name.endsWith('.xml') || f.name.endsWith('.xml.template')) {
        checkLicenseHeaderPrecedes(f, '<tag>', xmlTagPattern, xmlCommentPattern, text, ratDocument)
      }
      if (f.name.endsWith('.adoc')) {
        checkForUnescapedSymbolSubstitutions(f, text)
      }
    })
    
    if (found) {
      throw new GradleException(String.format(Locale.ENGLISH, 'Found %d violations in source files (%s).',
      found, violations.join(', ')))
    }
  }
  
  def static exts = [
    'java',
    'jflex',
    'py',
    'pl',
    'g4',
    'jj',
    'html',
    'js',
    'css',
    'xml',
    'xsl',
    'vm',
    'sh',
    'cmd',
    'bat',
    'policy',
    'properties',
    'mdtext',
    'groovy',
    'template',
    'adoc',
    'json',
  ]

  def static invalidPatterns = [
    (~$/@author\b/$) : '@author javadoc tag',
    (~$/(?i)\bno(n|)commit\b/$) : 'nocommit',
    (~$/\bTOOD:/$) : 'TOOD instead TODO',
    (~$/\t/$) : 'tabs instead spaces',
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
  
  protected AtomicInteger found = new AtomicInteger()
  protected def violations = Collections.synchronizedSet(new TreeSet())
  protected def reportViolation = { f, name ->
    log.error(name + ': ' + f.toString().substring(baseDir.length() + 1).replace(File.separatorChar, (char)'/'))
    violations.add(name)
    found.incrementAndGet()
  }

  protected def javadocsPattern = ~$/(?sm)^\Q/**\E(.*?)\Q*/\E/$
  protected def javaCommentPattern = ~$/(?sm)^\Q/*\E(.*?)\Q*/\E/$
  protected def xmlCommentPattern = ~$/(?sm)\Q<!--\E(.*?)\Q-->\E/$
  protected def lineSplitter = ~$/[\r\n]+/$
  protected def singleLineSplitter = ~$/\n\r?/$
  protected def licenseMatcher = Defaults.createDefaultMatcher()
  protected def validLoggerPattern = ~$/(?s)\b(private\s|static\s|final\s){3}+\s*Logger\s+\p{javaJavaIdentifierStart}+\s+=\s+\QLoggerFactory.getLogger(MethodHandles.lookup().lookupClass());\E/$
  protected def validLoggerNamePattern = ~$/(?s)\b(private\s|static\s|final\s){3}+\s*Logger\s+log+\s+=\s+\QLoggerFactory.getLogger(MethodHandles.lookup().lookupClass());\E/$
  protected def packagePattern = ~$/(?m)^\s*package\s+org\.apache.*;/$
  protected def xmlTagPattern = ~$/(?m)\s*<[a-zA-Z].*/$
  protected def sourceHeaderPattern = ~$/\[source\b.*/$
  protected def blockBoundaryPattern = ~$/----\s*/$
  protected def blockTitlePattern = ~$/\..*/$
  protected def unescapedSymbolPattern = ~$/(?<=[^\\]|^)([-=]>|<[-=])/$ // SOLR-10883
  protected def extendsLuceneTestCasePattern = ~$/public.*?class.*?extends.*?LuceneTestCase[^\n]*?\n/$
  
  protected def isLicense = { matcher, ratDocument ->
    licenseMatcher.reset()
    return lineSplitter.split(matcher.group(1)).any{ licenseMatcher.match(ratDocument, it) }
  }

  protected def checkLicenseHeaderPrecedes = { f, description, contentPattern, commentPattern, text, ratDocument ->
    def contentMatcher = contentPattern.matcher(text)
    if (contentMatcher.find()) {
      def contentStartPos = contentMatcher.start()
      def commentMatcher = commentPattern.matcher(text)
      while (commentMatcher.find()) {
        if (isLicense(commentMatcher, ratDocument)) {
          if (commentMatcher.start() < contentStartPos) {
            break // This file is all good, so break loop: license header precedes 'description' definition
          } else {
            reportViolation(f, description+' declaration precedes license header')
          }
        }
      }
    }
  }
  
  protected def checkMockitoAssume = { f, text ->
    if (text.contains("mockito") && !text.contains("assumeWorkingMockito()")) {
      reportViolation(f, 'File uses Mockito but has no assumeWorkingMockito() call')
    }
  }
  
  protected def checkForUnescapedSymbolSubstitutions = { f, text ->
    def inCodeBlock = false
    def underSourceHeader = false
    def lineNumber = 0
    singleLineSplitter.split(text).each {
      ++lineNumber
      if (underSourceHeader) { // This line is either a single source line, or the boundary of a code block
        inCodeBlock = blockBoundaryPattern.matcher(it).matches()
        if ( ! blockTitlePattern.matcher(it).matches()) {
          underSourceHeader = false
        }
      } else {
        if (inCodeBlock) {
          inCodeBlock = ! blockBoundaryPattern.matcher(it).matches()
        } else {
          underSourceHeader = sourceHeaderPattern.matcher(it).lookingAt()
          if ( ! underSourceHeader) {
            def unescapedSymbolMatcher = unescapedSymbolPattern.matcher(it)
            if (unescapedSymbolMatcher.find()) {
              reportViolation(f, 'Unescaped symbol "' + unescapedSymbolMatcher.group(1) + '" on line #' + lineNumber)
            }
          }
        }
      }
    }
  }
}

