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

import org.gradle.api.artifacts.ResolvedArtifact
import org.gradle.api.artifacts.ResolvedDependency
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.RelativePath
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.specs.Spec
import org.gradle.api.specs.Specs
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

import java.nio.file.Files
import java.util.stream.Stream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.ZipFile
import java.util.zip.ZipException

class MissingDeps extends DefaultTask {
  protected static Pattern pattern = Pattern.compile("->\\s\\\"([^\\s]*?)\\s")
  protected static Pattern srcJar = Pattern.compile("(.*?)-sources.jar")
  protected static Pattern dotFilePattern = Pattern.compile("(.*?).jar.dot")
  
  @InputDirectory
  File inputDirectory
  
  @Input
  @Optional
  List<String> depExcludes
  
  @Input
  @Optional
  List<String> classExcludes
  
  protected configuration = "runtimeClasspath"
  
  @Inject
  public MissingDeps(File inputDirectory, List<String> depExcludes, List<String> classExcludes) {
    this.inputDirectory = inputDirectory
    this.depExcludes = depExcludes
    this.classExcludes = classExcludes
  }
  
  @TaskAction
  void execute() {
    
    
    if (project.hasProperty('useConfiguration')) {
      configuration = project.useConfiguration
    }
    
    if (!project.configurations.hasProperty(configuration)) {
      return
    }
    
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    
    def topLvlProject = getTopLvlProject(project)
    
    File dotFile = project.mfile(inputDirectory, 'jdepsDir/' + topLvlProject.name +  "/" + "${project.name}-${project.version}/${project.name}-${project.version}.jar.dot")
    
    println ''
    println 'Possibly missing deps:'
    println ''
    
    boolean nothingFound = true
    project.fileTree(project.mfile(inputDirectory, 'jdepsDir')) {
      
      println 'depExcludes ' + depExcludes
      println 'classExcludes ' + classExcludes
      for (String ex : depExcludes) {
        exclude ex
      }
      include "**/*.dot"
      exclude "**/summary.dot"
    }.each {
      if (it.text.contains("(not found)")) {
        println ''
        println it.name + ':'
        
        String ourArtifactNameAndVersion = ''
        Matcher dotFileNameMatcher = dotFilePattern.matcher(it.name)
        
        if(dotFileNameMatcher.matches()) {
          ourArtifactNameAndVersion = dotFileNameMatcher.group(1)
        }
        
        def lines = it.readLines()
        for (String line : lines) {
          if (line.contains('(not found)')) {
            String className
            AtomicBoolean foundInsrc = new AtomicBoolean(false)
            boolean excluded
            Matcher m = pattern.matcher(line)
            if (m.find()) {
              className = m.group(1)
              for (String classExclude : classExcludes) {
                Matcher m2 = Pattern.compile(classExclude).matcher(className)
                if (m2.find()) {
                  excluded = true
                  break
                }
              }
            }
            if (!excluded) {
              nothingFound = false
              println line
            }
            
          }
        }
      }
    }
    
    if (nothingFound) {
      println ''
      println 'No potential missing deps found!'
    }
  }
  
  protected Project getTopLvlProject(Project proj) {
    def topLvlProject
    if (proj.group ==~ /.*?\.lucene(?:\.\w+)?/) {
      topLvlProject = project.project(":lucene")
    } else if (proj.group ==~ /.*?\.solr(?:\.\w+)?/) {
      topLvlProject = project.project(":solr")
    } else {
      throw new GradleException("Could not determine top level project for " + proj)
    }
    return topLvlProject
  }
}


