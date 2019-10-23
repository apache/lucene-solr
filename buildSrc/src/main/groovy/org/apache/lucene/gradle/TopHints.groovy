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

// we may not end up using this with gradle
class TopHints extends DefaultTask {
  
  
  public TopHints() {
    dependsOn { project(':buildSrc').tasks.setupAntPaths }
  }
  
  @TaskAction
  void execute() {
    def listMax = 10

    ant.taskdef(classname: 'com.carrotsearch.ant.tasks.junit4.balancers.TopHints',
    name: 'topHints',
    classpath: project.rootProject.ext.junit4Path)
    
    
    println "Showing ${listMax} slowest tests according to local stats. (change with -Dmax=...)."
    ant.topHints(max: listMax) {
      println "cache: ${project.rootProject.projectDir}/.caches/test-stats"
      ant.fileset(dir: "${project.rootProject.projectDir}/.caches/test-stats") {
        ant.include(name: "**/*.txt")
      }
    }
    
    println"Showing ${listMax} slowest tests in cached stats. (change with -Dmax=...)."
    ant.topHints(max: listMax) {
      ant.fileset(dir: "${project.rootProject.projectDir}/lucene/tools/junit4", includes: "*.txt")
    }
  }
}


