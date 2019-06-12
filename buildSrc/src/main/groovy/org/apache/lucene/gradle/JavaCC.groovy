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
    String javaCCJarHome
    
    javaCCHome = project.mfile(project.buildDir, 'javacc')
    project.mkdir(javaCCHome)
    
    project.project(':buildSrc').configurations.javacc.files.each {
      if (it.getName().startsWith('javacc') && it.getName().endsWith('.jar')) {
        javaCCJarHome = it.getParentFile().getAbsolutePath()
        javaCCJarName = it.getName()
      }
    }

    project.copy {
      from (javaCCJarHome + '/' + javaCCJarName)
      into (javaCCHome)
      rename { String fileName ->
        fileName = 'javacc.jar'
      }
    }
    
    ant.taskdef(classname: 'org.apache.tools.ant.taskdefs.optional.javacc.JavaCC',
    name: 'javacc',
    classpath: project.project(':buildSrc').configurations.javacc.asPath)

    project.mkdir(target)
    
    ant.delete() {
      ant.fileset(dir: target.getAbsolutePath(), includes: '*.java') {
        ant.containsregexp(expression: 'Generated.*By.*JavaCC')
      }
    }
    
    ant.javacc(target: inputFile.getAbsolutePath(), outputDirectory: target.getAbsolutePath(), javacchome: javaCCHome)
    
    // Remove debug stream (violates forbidden-apis)
    ant.replaceregexp(match: "/\\*\\* Debug output.*?Set debug output.*?ds; }", replace: '', flags: 's', encoding: 'UTF-8') {
      ant.fileset(dir: target.getAbsolutePath(), includes: "*TokenManager.java")
    }
    // Add warnings supression
    ant.replaceregexp(match:"^\\Qpublic class\\E", replace: "@SuppressWarnings(\"cast\")${System.getProperty('line.separator')}\\0", flags:"m", encoding: "UTF-8") {
      ant.fileset(dir: target.getAbsolutePath(), includes:"*TokenManager.java")
    }
    // StringBuffer -> StringBuilder 
    ant.replace(token: "StringBuffer", value: "StringBuilder", encoding: "UTF-8") {
       ant.fileset(dir: target.getAbsolutePath(), includes: "ParseException.java TokenMgrError.java")
    }

    ant.fixcrlf(srcdir: target.getAbsolutePath(), includes: '*.java', encoding: 'UTF-8') {
      ant.containsregexp(expression: 'Generated.*By.*JavaCC')
    }
  }
}


