package org.apache.lucene.gradle.deps
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
import org.gradle.api.tasks.util.PatternSet
import java.nio.file.Files
import java.util.stream.Stream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.ZipFile
import java.util.zip.ZipException

class MissingDeps extends DefaultTask {
  protected static Pattern pattern = Pattern.compile("\\\"(.*?)\\\"\\s+->\\s\\\"([^\\s]*?)\\s")
  protected static Pattern srcJar = Pattern.compile("(.*?)-sources.jar")
  protected static Pattern dotFilePattern = Pattern.compile("(.*?).jar.dot")
  

  File inputDirectory

  private List<String> depExcludes = new ArrayList<>()
  private List<String> exportedDepExcludes = new ArrayList<>()
  private List<String> classExcludes = new ArrayList<>()
  private List<String> exportedClassExcludes = new ArrayList<>()
  private List<String> foundInClassExcludes = new ArrayList<>()
  private List<String> exportedFoundInClassExcludes = new ArrayList<>()
  
  protected configuration = "runtimeClasspath"
  
  class NotFoundResult {
    StringBuilder sb = new StringBuilder()
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
    
    def topLvlProject = project.getTopLvlProject()
    
    File dotFile = project.mfile(inputDirectory, 'jdepsDir/' + topLvlProject.name +  "/" + "${project.name}-${project.version}/${project.name}-${project.version}.jar.dot")
    
    StringBuilder sb = new StringBuilder()
    
    sb.append '\n'
    sb.append 'Possibly missing deps (if the dot file is listed with no violations, it has violations that were excluded):\n'
    sb.append '\n'
    
    List<NotFoundResult> notFoundResults = new ArrayList<>()
    
    boolean nothingFound = true
    project.fileTree(project.mfile(inputDirectory, 'jdepsDir')) {

      for (String ex : depExcludes) {
        exclude ex
      }
      include "**/*.dot"
      exclude "**/summary.dot"
    }.each {
      NotFoundResult nfr = new NotFoundResult()
      
      if (it.text.contains("(not found)")) {
        nfr.sb.append '\n'
        nfr.sb.append it.getParentFile().name + '/' + it.name + ':'
        
        String ourArtifactNameAndVersion = ''
        Matcher dotFileNameMatcher = dotFilePattern.matcher(it.name)
        
        if(dotFileNameMatcher.matches()) {
          ourArtifactNameAndVersion = dotFileNameMatcher.group(1)
        }
        
        def lines = it.readLines()
        for (String line : lines) {
          if (line.contains('(not found)')) {
            String className
            String classFoundInName
            AtomicBoolean foundInsrc = new AtomicBoolean(false)
            boolean excluded
            Matcher m = pattern.matcher(line)
            if (m.find()) {
              classFoundInName = m.group(1)
              
              for (String foundInClassExclude : foundInClassExcludes) {
                Matcher m2 = Pattern.compile(foundInClassExclude).matcher(classFoundInName)
                if (m2.matches()) {
                  excluded = true
                  break
                }
              }
              
              className = m.group(2)
              for (String classExclude : classExcludes) {
                Matcher m2 = Pattern.compile(classExclude).matcher(className)
                if (m2.matches()) {
                  excluded = true
                  break
                }
              }
            }
            if (!excluded) {
              nothingFound = false
              nfr.sb.append line + '\n'
              notFoundResults.add(nfr)
            }
            
          }
        }
      }
    }
    
    if (nothingFound) {
      sb.append '\n'
      sb.append 'No potential missing deps found!\n'
      //logger.quiet(sb.toString())
    } else {
      logger.error(sb.toString())
      for (NotFoundResult nfr : notFoundResults) {
        logger.error nfr.asBoolean().toString()
      }
      throw new GradleException("Missing dependencies found! Add them or add an exclusion if they are actually not necessary.")
    }
  }
  
  @InputDirectory
  public File getInputDirectory() {
    return inputDirectory
  }
  
  @Input
  public Set<String> getFoundInClassExcludes() {
    return foundInClassExcludes
  }
  
  @Input
  public Set<String> getExportedFoundInClassExcludes() {
    return exportedFoundInClassExcludes
  }
  
  public MissingDeps foundInClassExclude(boolean exported, String... arg0) {
    if (exported) {
      for (String pattern : arg0) {
        exportedFoundInClassExcludes.add(pattern)
      }
    }
    for (String pattern : arg0) {
      foundInClassExcludes.add(pattern)
    }
    return this
  }
  
  public MissingDeps foundInClassExclude(String... arg0) {
    return foundInClassExclude(true, arg0)
  }
  
  @Input
  public Set<String> getClassExcludes() {
    return classExcludes
  }
  
  @Input
  public Set<String> getExportedClassExcludes() {
    return exportedClassExcludes
  }
  
  public MissingDeps classExclude(String... arg0) {
    return classExclude(true, arg0)
  }
  
  public MissingDeps classExclude(boolean exclude, String... arg0) {
    if (exclude) {
      for (String pattern : arg0) {
        exportedClassExcludes.add(pattern)
      }
    }
    for (String pattern : arg0) {
      classExcludes.add(pattern)
    }
    return this
  }
  
  @Input
  public Set<String> getDepExcludes() {
    return depExcludes
  }
  
  @Input
  public Set<String> getExportedDepExcludes() {
    return exportedDepExcludes
  }
  
  public MissingDeps depExclude(String... arg0) {
    return depExclude(true, arg0)
  }
  
  public MissingDeps depExclude(boolean exported, String... arg0) {
    if (exported) {
      for (String pattern : arg0) {
        exportedDepExcludes.add(pattern)
      }
    }
    for (String pattern : arg0) {
      depExcludes.add(pattern)
    }
    return this
  }
  
  public void addExclusionsFrom(Project fromProject) {
    project.evaluationDependsOn(fromProject.path)
    
    MissingDeps to = project.missingDeps
    
    Set<String> depExcludes = fromProject.missingDeps.getExportedDepExcludes()
    for (String exclude : depExcludes) {
      to.depExclude exclude
    }
    Set<String> classExcludes = fromProject.missingDeps.getExportedClassExcludes()
    for (String exclude : classExcludes) {
      to.classExclude exclude
    }
    Set<String> foundInClassExcludes = fromProject.missingDeps.getExportedFoundInClassExcludes()
    for (String exclude : foundInClassExcludes) {
      to.foundInClassExclude exclude
    }
  }
}


