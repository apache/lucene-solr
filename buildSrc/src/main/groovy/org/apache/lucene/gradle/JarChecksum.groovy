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
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.RelativePath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

import java.nio.file.Files

class JarChecksum extends DefaultTask {
  
  private File inputDir
  
  private File target
  
  @Inject
  public JarChecksum(File inputDir,  File target) {
    
    
    doLast({
      File tmpDir = File.createTempDir()
      tmpDir.deleteOnExit()
      tmpDir.mkdirs()
      
      List<File> deps = new ArrayList<>()
      project.allprojects.each { p ->
        p.configurations.each { Configuration config ->
          if (config.isCanBeResolved()) {
            config.getResolvedConfiguration().getResolvedArtifacts().forEach( { ra -> deps.add(ra.getFile()) })
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
            deps = deps - ourJars
            
            // copy files to tmp dir
            deps.each {
              def file = it
              File destFile = new File(tmpDir, file.name)
              if (file.name.endsWith(".jar") && file.exists() && !destFile.exists()) {
                Files.copy(file.toPath(), destFile.toPath())
              }
            }
          }
        }
      }
      
      project.delete project.fileTree(dir: target.getAbsolutePath(), include: '**/*.jar.sha1')
      
      ant.checksum(algorithm: "SHA1", fileext: ".sha1", todir: target.getAbsolutePath()) {
        ant.fileset(dir: tmpDir.getAbsolutePath())
      }
      
      project.delete(tmpDir)
      
      ant.fixcrlf(srcdir: target.getAbsolutePath(), includes: "**/*.jar.sha1", eol: "lf", fixlast: "true", encoding: "US-ASCII")
      
    })
    
  }
  
  @TaskAction
  void checksums() {
    
  }
}


