package org.apache.lucene.gradle
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
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

class JavaCC extends DefaultTask {
  
  @InputFile
  File inputFile
  
  @OutputDirectory
  File target
  
  @TaskAction
  void javacc() {
    
    String javaCCHome
    String javaCCJarName
    
    project.project(":buildSrc").configurations.javacc.files.each {
      if (it.getName().startsWith("javacc") && it.getName().endsWith(".jar")) {
        javaCCHome = it.getParentFile().getAbsolutePath()
        javaCCJarName = it.getName()
      }
    }
    
    project.sync {
      from (javaCCHome + "/" + javaCCJarName)
      into (javaCCHome)
      rename { String fileName ->
        fileName = "javacc.jar"
      }
    }
    
    ant.taskdef(classname: 'org.apache.tools.ant.taskdefs.optional.javacc.JavaCC',
    name: 'javacc',
    classpath: project.project(":buildSrc").configurations.javacc.asPath)

    project.mkdir("src/java/org/apache/solr/parser")
    
    ant.delete() {
      ant.fileset(dir: "${target}", includes: "*.java") {
        ant.containsregexp(expression: "Generated.*By.*JavaCC")
      }
    }
    
    ant.javacc(target: inputFile.getAbsolutePath(), outputDirectory: target.getAbsolutePath(), javacchome: javaCCHome)
    
    
    // Change the incorrect public ctors for QueryParser to be protected instead
    ant.replaceregexp(file:"src/java/org/apache/solr/parser/QueryParser.java",
    byline:  "true",
    match:   "public QueryParser\\(CharStream ",
    replace: "protected QueryParser(CharStream ")
    ant.replaceregexp(file:"src/java/org/apache/solr/parser/QueryParser.java",
    byline: "true",
    match:  "public QueryParser\\(QueryParserTokenManager ",
    replace:"protected QueryParser(QueryParserTokenManager ")
    // change an exception used for signaling to be static
    ant.replaceregexp(file: "src/java/org/apache/solr/parser/QueryParser.java",
    byline: "true",
    match: "final private LookaheadSuccess jj_ls =",
    replace: "static final private LookaheadSuccess jj_ls =")
    ant.replace(token: "StringBuffer", value: "StringBuilder", encoding: "UTF-8") {
      ant.fileset(dir: "src/java/org/apache/solr/parser", includes: "ParseException.java TokenMgrError.java")
    }
    
    ant.fixcrlf(srcdir: "${target}", includes: "*.java", encoding: "UTF-8") {
      ant.containsregexp(expression: "Generated.*By.*JavaCC")
    }
  }
}


