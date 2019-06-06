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
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.RelativePath
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.specs.Spec
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
  protected File distDir
  protected File jdepsDir
  
  @InputDirectory
  File inputDirectory
  
  @Inject
  public UnusedDeps(File inputDirectory) {
    
    if (!project.configurations.hasProperty('runtimeClasspath')) {
      return
    }
    
    this.inputDirectory = inputDirectory
    
    distDir = new File(inputDirectory, 'distDir')
    jdepsDir = new File(inputDirectory, 'jdepsDir')
    
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
  }
  
  @TaskAction
  void execute() {
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    
    def topLvlProject = getTopLvlProject(project)
    
    Configuration config = project.configurations[this.configuration]
    
    Set<String> usedDepJarNames = getDefinedDeps(topLvlProject, project, distDir, jdepsDir)
    
    Set<File> ourDeps = getAllDefinedDeps(project, config)
    
    Set<File> ourImmediatelyDefinedDeps = getOurImmediateDefinedDeps(project, config)
    
    config.getAllDependencies().forEach({ dep ->
      if (dep instanceof DefaultProjectDependency) {
        Project dProject = dep.getDependencyProject()
        def depTopLvlDProject = getTopLvlProject(dProject)
        
        Set<String> projectUsedDeps = getDefinedDeps(depTopLvlDProject, dProject, distDir, jdepsDir)
        
        usedDepJarNames += projectUsedDeps
      }
    })
    
    usedDepJarNames -= [
      "${project.name}-${project.version}.jar"
    ]
    
    Set<String> ourDepJarNames = new HashSet<>()
    ourDeps.forEach( { ourDepJarNames.add(it.getName()) } )
    
    Set<String> unusedJarNames = new HashSet<>()
    unusedJarNames.addAll(ourDepJarNames)
    unusedJarNames -= usedDepJarNames
    unusedJarNames = unusedJarNames.toSorted()
    
    Set<String> depsInDirectUse = new HashSet<>()
    
    File jdepsLucene = new File(jdepsDir, "lucene")
    File jdepsSolr = new File(jdepsDir, "solr")
    
    for (File file : jdepsLucene.listFiles()) {
      lookForDep(file, depsInDirectUse)
    }
    for (File file : jdepsSolr.listFiles()) {
      lookForDep(file, depsInDirectUse)
    }
    
    println ''
    println 'Our classpath dependency count ' + ourDepJarNames.size()
    println 'Our directly used dependency count ' + usedDepJarNames.size()
    println ''
    println 'List of possibly unused jars - they may be used at runtime however (Class.forName on plugins or config text for example). This is not definitive, but helps narrow down what to investigate.'
    println 'We take our classpath dependencies, substract our direct dependencies and then subtract dependencies used by our direct dependencies.'
    println ''
    
    println 'Direct deps that may be unused:'
    unusedJarNames.forEach({
      if (!depsInDirectUse.contains(it) && ourImmediatelyDefinedDeps.contains(it)) {
        println ' - ' + it
      }
    })
    
    println ''
    println 'Deps brought in by other modules that may be unused in this module:'
    unusedJarNames.forEach({
      if (!depsInDirectUse.contains(it) && !ourImmediatelyDefinedDeps.contains(it)) {
        println ' - ' + it
      }
    })
  }
  
  static class NonProjectSpec implements Spec<Dependency> {
    @Override
    public boolean isSatisfiedBy(Dependency dep) {
      return true
    }
  }
  
  protected Set getAllDefinedDeps(Project project, Configuration config) {
    Set<File> ourDeps = new HashSet<>()
    
    if (config.isCanBeResolved()) {
      config.getResolvedConfiguration().getResolvedArtifacts().forEach( { ra -> ourDeps.add(ra.getFile()) })
    }
    
    return ourDeps
  }
  
  protected Set getOurImmediateDefinedDeps(Project project, Configuration config) {
    Set<String> ourDeps = new HashSet<>()
    
    Set<ResolvedDependency> deps = project.configurations.runtimeClasspath.getResolvedConfiguration().getFirstLevelModuleDependencies(new NonProjectSpec())
    
    for (ResolvedDependency dep : deps) {
      dep.getModuleArtifacts().forEach({ourDeps.add(it.file.name)})
    }
    
    return ourDeps
  }
  
  protected Set getDefinedDeps(Project topLvlProject, Project project, File distDir, File jdepsDir) {
    
    File dotFile = new File(jdepsDir, topLvlProject.name +  "/" + "${project.name}-${project.version}/${project.name}-${project.version}.jar.dot")
    Set<String> usedDepJarNames = getDirectlyUsedJars(project, dotFile)
    
    return usedDepJarNames
  }
  
  protected Set getDirectlyUsedJars(Project project, File dotFile) {
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
  
  protected void lookForDep(File dir, Set<String> depsInDirectUse) {
    dir.eachFile() {
      depsInDirectUse.addAll(getDirectlyUsedJars(project, it))
    }
  }
  
  protected Project getTopLvlProject(Project proj) {
    def topLvlProject
    if (proj.group ==~ /.*?\.lucene(?:\.\w+)?/) {
      topLvlProject = project.project(":lucene")
    } else if (proj.group ==~ /.*?\.solr(?:\.\w+)?/) {
      topLvlProject = project.project(":solr")
    }
    return topLvlProject
  }
}


