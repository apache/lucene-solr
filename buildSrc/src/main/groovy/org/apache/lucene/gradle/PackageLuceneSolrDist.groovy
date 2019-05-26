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
import org.gradle.api.tasks.bundling.Compression

class PackageLuceneSolrDist extends DefaultTask {
  
  PackageLuceneSolrDist() {
    def distDir = 'dist'
    
    def standardIncludes = [
      "LICENSE.txt",
      "NOTICE.txt",
      "README.txt",
      "MIGRATE.txt",
      "JRE_VERSION_MIGRATION.txt",
      "SYSTEM_REQUIREMENTS.txt",
      "CHANGES.txt",
      "licenses/**",
      "*/docs/",
      "**/README*"
    ]
    
    def standardExcludes = [
      "**/site/**",
      "**/tools/**",
      "**/dist/**",
      "**/build/**",
      "**/eclipse-build/**",
      "**/build-eclipse/**",
      "**/test-files/**",
      "**/.out/**",
      "**/target/**",
      "**/work/**",
      "**/temp/**"
    ]
    
    project.tasks.create(name: "packZip", type: org.gradle.api.tasks.bundling.Zip) {
      archiveName = "${project.name}-${project.version}.zip"
      into ('/')
      from (project.projectDir) {
        
        standardIncludes.each {
          println "include:" +it
          include it
        }
        standardExcludes.each {
          println "exclude:" +it
          exclude it
        }
        
        into('/')
      }
      
      project.subprojects.each {subproject ->
        project.evaluationDependsOn(subproject.path)
        if (subproject.tasks.findByName('jar')) {
          subproject.jar.outputs.files.each {println "Pack:" + it.name}
          from(subproject.jar.outputs.files) {
            include "*.jar"
            into (project.relativePath(subproject.projectDir))
          }
        }
      }
      
      includeEmptyDirs = false
      destinationDir = new File(project.projectDir, distDir)
      extension = 'zip'
    }
    
    project.tasks.create(name: "packTar", type: org.gradle.api.tasks.bundling.Tar) {
      archiveName = "${project.name}-${project.version}.tgz"
      into ('/')
      from (project.projectDir) {
        
        standardIncludes.each {
          println "include:" +it
          include it
        }
        standardExcludes.each {
          println "exclude:" +it
          exclude it
        }
        
        into('/')
      }
      
      project.subprojects.each {subproject ->
        project.evaluationDependsOn(subproject.path)
        if (subproject.tasks.findByName('jar')) {
          subproject.jar.outputs.files.each {println "Pack:" + it.name}
          from(subproject.jar.outputs.files) {
            include "*.jar"
            into (project.relativePath(subproject.projectDir))
          }
        }
      }
      
      includeEmptyDirs = false
      destinationDir = new File(project.projectDir, distDir)
      extension = 'tar'
      compression = Compression.GZIP
    }
    
    finalizedBy project.tasks.packZip
    
    project.tasks.packZip.dependsOn project.tasks.packTar
  }
  
  @TaskAction
  void pack() {

  }
}


