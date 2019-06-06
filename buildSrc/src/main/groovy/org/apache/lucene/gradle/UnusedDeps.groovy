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

import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.RelativePath
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

import java.nio.file.Files
import java.util.regex.Matcher
import java.util.regex.Pattern

// dev util task to help find possible unused deps
class UnusedDeps extends DefaultTask {
  protected static Pattern pattern = Pattern.compile("\\(([^\\s]*?\\.jar)\\)")
  
  protected configuration = "runtimeClasspath"
  protected File tmpDir
  protected File distDir
  protected File jdepsDir
  
  public UnusedDeps() {
    //!project.getPlugins().hasPlugin('java-base') ||
    if (!project.configurations.hasProperty('runtimeClasspath')) {
      return
    }
    
    if (project.hasProperty('unusedDepsConfig')) {
      configuration = project.unusedDepsConfig
    }
    
    Configuration config = project.configurations[this.configuration]
    
    List<Project> buildProjects = new ArrayList()
    buildProjects.add(project)
    config.getAllDependencies().forEach({ dep ->
      if (dep instanceof DefaultProjectDependency) {
        Project dProject = dep.getDependencyProject()
        buildProjects.add(dProject)
      }
    })
    
    project.tasks.create(name: "export", type: org.gradle.api.tasks.Copy) {
      outputs.upToDateWhen { false }
      into({ makeDirs(); distDir })
      buildProjects.each {subproject ->
        project.evaluationDependsOn(subproject.path)
        def topLvlProject
        if (subproject.group ==~ /.*?\.lucene(?:\.\w+)?/) {
          topLvlProject = project.project(":lucene")
        } else if (subproject.group ==~ /.*?\.solr(?:\.\w+)?/) {
          topLvlProject = project.project(":solr")
        }
        
        if (subproject.getPlugins().hasPlugin(PartOfDist) && subproject.tasks.findByName('jar') && subproject.configurations.hasProperty('runtimeClasspath')) {
          from(subproject.jar.outputs.files) {
            include "*.jar"
            into ({topLvlProject.name + '/' + topLvlProject.relativePath(subproject.projectDir)})
          }
          def files = { getFiles(subproject) }
          from(files) {
            include "*.jar"
            into ({topLvlProject.name + '/' + topLvlProject.relativePath(subproject.projectDir) + "/lib"})
          }
        }
      }
      
      includeEmptyDirs = false
    }
    
    dependsOn project.tasks.export
  }
  
  protected void makeDirs() {
    tmpDir = File.createTempDir()
    tmpDir.deleteOnExit()
    tmpDir.mkdirs()
    distDir = new File(tmpDir, 'distDir')
    jdepsDir = new File(tmpDir, 'jdepsDir')
    distDir.mkdirs()
    jdepsDir.mkdirs()
  }
  
  private static Collection getFiles(Project subproject) {
    def files = subproject.configurations.runtimeClasspath.files
    if (!subproject.name.equals('solr-core') && subproject.path.startsWith(":solr:contrib:")) {
      subproject.evaluationDependsOn(subproject.rootProject.project(":solr:solr-core").path)
      files = files - subproject.rootProject.project(":solr:solr-core").configurations.runtimeClasspath.files
      files = files - subproject.rootProject.project(":solr:solr-core").jar.outputs.files
    }
    if (!subproject.name.equals('lucene-core') && subproject.path.startsWith(":lucene:")) {
      subproject.evaluationDependsOn(subproject.rootProject.project(":lucene:lucene-core").path)
      files = files - subproject.rootProject.project(":lucene:lucene-core").configurations.runtimeClasspath.files
      files = files - subproject.rootProject.project(":lucene:lucene-core").jar.outputs.files
    }
    return files
  }
  
  @TaskAction
  void execute() {
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    
    Project topLvlProject
    
    if (project.group ==~ /.*?\.lucene(?:\.\w+)?/) {
      topLvlProject = project.project(":lucene")
    } else if (project.group ==~ /.*?\.solr(?:\.\w+)?/) {
      topLvlProject = project.project(":solr")
    }
    
    def luceneDist  = new File("/data1/mark/tmp", "lucene")
    def solrDist = new File("/data1/mark/tmp", "solr")
    
    Configuration config = project.configurations[this.configuration]
    
    Set<String> usedDepJarNames = getDirectlyUsedDeps(topLvlProject, project, distDir, jdepsDir)
    
    Set<File> ourDeps = getDeps(project, config)
    
    config.getAllDependencies().forEach({ dep ->
      if (dep instanceof DefaultProjectDependency) {
        Project dProject = dep.getDependencyProject()
        def depTopLvlProject
        if (dProject.group ==~ /.*?\.lucene(?:\.\w+)?/) {
          depTopLvlProject = project.project(":lucene")
        } else if (dProject.group ==~ /.*?\.solr(?:\.\w+)?/) {
          depTopLvlProject = project.project(":solr")
        }
        
        Set<String> projectUsedDeps = getDirectlyUsedDeps(depTopLvlProject, dProject, distDir, jdepsDir)
        
        usedDepJarNames += projectUsedDeps
      }
    })
    
    usedDepJarNames -= ["${project.name}-${project.version}.jar"]
    
    Set<String> ourDepJarNames = new HashSet<>()
    ourDeps.forEach( { ourDepJarNames.add(it.getName()) } )
    
    Set<String> unusedJarNames = new HashSet<>()
    unusedJarNames.addAll(ourDepJarNames)
    unusedJarNames -= usedDepJarNames
    unusedJarNames = unusedJarNames.toSorted()
    
    Set<String> foundDeps = new HashSet<>()
    
    File jdepsLucene = new File(jdepsDir, "lucene")
    File jdepsSolr = new File(jdepsDir, "solr")
    
    for (File file : jdepsLucene.listFiles()) {
      lookForDep(file, foundDeps)
    }
    for (File file : jdepsSolr.listFiles()) {
      lookForDep(file, foundDeps)
    }
    
    println ''
    println 'Our classpath dependency count ' + ourDepJarNames.size()
    println 'Our directly used dependency count ' + usedDepJarNames.size()
    println ''
    println 'List of possibly unused jars - they may be used at runtime however (Class.forName or something), this is not definitive.'
    println 'We take our classpath dependenies, substract our direct dependencies and then subtract dependencies used by our direct dependencies'
    println ''
    
    unusedJarNames.forEach({
      if (!foundDeps.contains(it)) {
        println it
      }
    })
    
    project.delete(tmpDir)
  }
  
  protected Set getDeps(Project project, Configuration config) {
    Set<File> ourDeps = new HashSet<>()
    
    if (config.isCanBeResolved()) {
      config.getResolvedConfiguration().getResolvedArtifacts().forEach( { ra -> ourDeps.add(ra.getFile()) })
    }
    return ourDeps
  }
  
  protected Set getDirectlyUsedDeps(Project topLvlProject, Project project, File distDir, File jdepsDir) {
    def distPath = "${distDir}/" + topLvlProject.name + "/" + topLvlProject.relativePath(project.projectDir)
    def dotOutPath = jdepsDir.getAbsolutePath() + "/" + topLvlProject.name +  "/" + "${project.name}-${project.version}"
    
    ant.exec (executable: "jdeps", failonerror: true, resolveexecutable: true) {
      ant.arg(line: '--class-path ' + "${distPath}/lib/" + '*')
      ant.arg(line: '--multi-release 11')
      ant.arg(value: '-recursive')
      ant.arg(value: '-verbose:class')
      ant.arg(line: "-dotoutput ${dotOutPath}")
      ant.arg(value: "${distPath}/${project.name}-${project.version}.jar")
    }
    
    File dotFile = new File(jdepsDir, topLvlProject.name +  "/" + "${project.name}-${project.version}/${project.name}-${project.version}.jar.dot")
    Set<String> usedDepJarNames = getUsedJars(project, dotFile)
    
    return usedDepJarNames
  }
  
  protected Set getUsedJars(Project project, File dotFile) {
    Set<String> usedDepJarNames = new HashSet<>()
    
    def lines = dotFile.readLines()
    String lastName = ""
    for (String line : lines) {
      Matcher m = pattern.matcher(line)
      if (m.find()) {
        String jarName = m.group(1)
        if (!lastName.equals(jarName)) {
          usedDepJarNames.add(jarName)
        }
        lastName = jarName
      }
    }
    return usedDepJarNames
  }
  
  protected void lookForDep(File dir, Set<String> foundDeps) {
    dir.eachFile() {
      Set<String> usedDepJarNames = getUsedJars(project, it)
      foundDeps.addAll(usedDepJarNames)
      
    }
  }
}


