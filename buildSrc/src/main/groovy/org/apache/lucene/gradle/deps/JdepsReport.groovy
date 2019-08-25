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
import org.gradle.api.artifacts.result.DependencyResult
import org.gradle.api.artifacts.result.ResolvedDependencyResult

import javax.inject.Inject

import org.apache.tools.ant.types.resources.selectors.InstanceOf
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.jvm.tasks.Jar
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleIdentifier
import org.gradle.api.file.RelativePath
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.specs.Spec
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.WorkResult
import java.nio.file.Files
import java.util.regex.Matcher
import java.util.regex.Pattern

import org.apache.lucene.gradle.dist.PartOfDist

class JdepsReport extends DefaultTask {
  
  protected configuration = "runtimeClasspath"

  protected File distDir
  protected File jdepsDir
  
  @OutputDirectory
  File target
  
  @Optional
  @Input
  boolean recursive = true

  public JdepsReport() {
    if (project.hasProperty('useConfiguration')) {
      configuration = project.useConfiguration
    }
    doFirst {
      println "Writing output files to ${target}"
    }
    
    // make sure all the jars are built
    dependsOn project.rootProject.subprojects.collect { it.tasks.withType(Jar) }
    outputs.upToDateWhen { false }
  }
  
  protected void makeDirs() {
    target.mkdirs()
    distDir = new File(target, 'distDir')
    jdepsDir = new File(target, 'jdepsDir')
    distDir.mkdirs()
    jdepsDir.mkdirs()
  }

  @TaskAction
  void execute() {
    if (!project.configurations.hasProperty(configuration)) {
      println 'project does not have the specified configuration, skipping execute ...'
      return
    }
    
    makeDirs()
    
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    
    WorkResult result = project.copy {
      into(distDir)
      
      Configuration config = project.configurations[this.configuration]
      
      List<Project> buildProjects = new ArrayList()
      buildProjects.add(project)
      config.getAllDependencies().forEach({ dep ->
        if (dep instanceof DefaultProjectDependency) {
          Project dProject = dep.getDependencyProject()
          buildProjects.add(dProject)
        }
      })
      
      buildProjects.each {subproject ->

        def topLvlProject = project.getTopLvlProject(subproject)
        
        if (subproject.getPlugins().hasPlugin(PartOfDist) && subproject.tasks.findByName('jar') && subproject.configurations.hasProperty(configuration)) {
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
    
    if (result.getDidWork()) {
      runJdeps(project.getTopLvlProject(project), project, project, distDir, jdepsDir)
      
      Configuration config = project.configurations[this.configuration]
      config.getAllDependencies().forEach({ dep ->
        if (dep instanceof DefaultProjectDependency) {
          Project dProject = dep.getDependencyProject()
          def depTopLvlProject = project.getTopLvlProject(dProject)
          
          runJdeps(depTopLvlProject, dProject, project, distDir, jdepsDir)
        }
      })
    }
  }
  
  protected void runJdeps(Project topLvlProject, Project project, Project libProject, File distDir, File jdepsDir) {
    def distPath1 = "${distDir}/" + topLvlProject.name + "/" + topLvlProject.relativePath(libProject.projectDir)
    def distPath2 = "${distDir}/" + topLvlProject.name + "/" + topLvlProject.relativePath(project.projectDir)
    def dotOutPath = jdepsDir.getAbsolutePath() + "/" + topLvlProject.name +  "/" + "${project.name}-${project.version}"

    ant.exec (executable: "jdeps", failonerror: true, resolveexecutable: true) {
      ant.arg(line: '--class-path ' + "${distPath1}/lib/" + '*')
      ant.arg(line: '--multi-release 11')
      if (this.recursive) ant.arg(value: '-recursive')
      ant.arg(value: '-verbose:class')
      ant.arg(line: "-dotoutput ${dotOutPath}")
      ant.arg(value: "${distPath2}/${project.name}-${project.version}.jar")
    }
  }
  
  private static Collection getFiles(Project subproject) {
    def files = subproject.configurations.runtimeClasspath.files
    
    return files
  }

}


