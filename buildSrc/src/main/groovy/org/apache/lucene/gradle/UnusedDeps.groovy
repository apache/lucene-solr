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
  protected static Pattern pattern = Pattern.compile("\\((.*?\\.jar)\\)")
  
  @Inject
  public UnusedDeps() {

  }
  
  @TaskAction
  void execute() {
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    String configuration = "runtimeClasspath"
    
    if (project.hasProperty('unusedDepsConfig')) {
      configuration = project.unusedDepsConfig
    }
    
    File tmpDir = File.createTempDir()
    tmpDir.deleteOnExit()
    tmpDir.mkdirs()
    
    File distDir = new File(tmpDir, 'distDir')
    distDir.mkdirs()
    File jdepsDir = new File(tmpDir, 'jdepsDir')
    jdepsDir.mkdirs()
    

      Project topLvlProject
      File destinationDir
      
      if (project.group ==~ /.*?\.lucene(?:\.\w+)?/) {
        topLvlProject = project.project(":lucene")
        destinationDir = new File(topLvlProject.projectDir, "dist")
      } else if (project.group ==~ /.*?\.solr(?:\.\w+)?/) {
        topLvlProject = project.project(":solr")
        destinationDir = new File(topLvlProject.projectDir, "dist")
      }
      
     
      ant.untar(src: "${destinationDir}/${topLvlProject.name}-${topLvlProject.version}.tgz", dest: distDir.getAbsolutePath(), compression:"gzip")

      def distPath = "${distDir}/" + topLvlProject.relativePath(project.projectDir)
      
      ant.exec (executable: "jdeps", failonerror: true, resolveexecutable: true) {
        ant.arg(line: '--class-path ' + "${distPath}/lib/" + '*')
        ant.arg(line: '--multi-release 11')
        ant.arg(value: '-recursive')
        ant.arg(value: '-verbose:class')
        ant.arg(line: "-dotoutput ${jdepsDir.getAbsolutePath()}")
        ant.arg(value: "${distPath}/${project.name}-${topLvlProject.version}.jar")
      }
      
      
      Set<String> usedDepJarNames = new HashSet<>()
      File file = new File(jdepsDir, "${project.name}-${topLvlProject.version}.jar.dot")
      def lines = file.readLines()
      
      lines.each { String line ->
        Matcher m = pattern.matcher(line)
        if (m.find()) {
          usedDepJarNames.add(m.group(1))
        }
      }
      
      Set<File> ourDeps = new HashSet<>()
      
      Configuration config = project.configurations[configuration]
      if (config.isCanBeResolved()) {
        config.getResolvedConfiguration().getResolvedArtifacts().forEach( { ra -> ourDeps.add(ra.getFile()) })
        // exclude our jar and jarTest outputs
        def ourJars = new ArrayList()
        project.rootProject.subprojects.each{ subproject ->
          if (subproject.hasProperty('jar')) {
            ourJars.addAll(subproject.jar.outputs.files)
            if (subproject.hasProperty('jarTest')) {
              ourJars.addAll(subproject.jarTest.outputs.files)
            }
          }
        }
        ourDeps = ourDeps - ourJars
      }
      
      Set<String> ourDepJarNames = new HashSet<>()
      ourDeps.forEach( { ourDepJarNames.add(it.getName()) } )
      
      println 'our dep count ' + ourDepJarNames.size()
      println 'used count ' + usedDepJarNames.size()
      
      Set<String> unusedJarNames = new HashSet<>()
      unusedJarNames.addAll(ourDepJarNames)
      unusedJarNames -= usedDepJarNames
      unusedJarNames = unusedJarNames.toSorted()
      unusedJarNames.forEach( { println it } )
      
      project.delete(tmpDir)

  }
}


