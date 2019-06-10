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

class FindInSrc {
  protected static Pattern pattern = Pattern.compile("->\\s\\\"([^\\s]*?)\\s")
  protected static Pattern srcJar = Pattern.compile("(.*?)-sources.jar")
  protected static Pattern dotFilePattern = Pattern.compile("(.*?).jar.dot")

  public FindInSrc() {
  }
  
  
  public boolean find(Project project, String ourArtifactNameAndVersion, String searchText) {
    AtomicBoolean foundInsrc = new AtomicBoolean(false)
    
    def sources = project.configurations.runtimeClasspath.resolvedConfiguration.resolvedArtifacts.collect { artifact ->
      project.dependencies.create( [
        group: artifact.moduleVersion.id.group,
        name: artifact.moduleVersion.id.name,
        version: artifact.moduleVersion.id.version,
        classifier: 'sources'
      ] )
    }
    def srcFiles = project.configurations.detachedConfiguration( sources as Dependency[] )
        .resolvedConfiguration.lenientConfiguration.getFiles( Specs.SATISFIES_ALL )
    List listSrcFiles = new ArrayList()
    listSrcFiles.addAll(srcFiles)
    Stream.of(listSrcFiles.toArray())
        .parallel()
        .forEach( { file ->
          // if (!file.name.endsWith('-sources.jar')) return
         // if (foundInsrc.get()) return // stop after we find first occurence(s)
          Matcher nameMatcher = srcJar.matcher(file.name)
          if (nameMatcher.matches()) {
            String artifactName = nameMatcher.group(1)
            if (ourArtifactNameAndVersion.equals(artifactName)) {
              return
            }
          }
          try {
            ZipFile zip = new ZipFile(file)
            def entries = zip.entries()
            entries.each { entry->
       
                def exts = [
                  '.properties',
                  '.java',
                  '.xml',
                  '.yml',
                  '.json',
                  '.props'
                ]
              boolean searchFile = true
              for (String ext : exts) {
                if (entry.name.endsWith(ext)) {
                  searchFile = true
                  break
                }
              }
              
              if (searchFile) {
                InputStream inputStream
                try {
                  inputStream = zip.getInputStream(entry)
                  if (inputStream.getText('UTF-8').contains(searchText)) {
                    project.println "   -> Found ${searchText} in src: "
                    project.println "   ${zip.name} -> ${entry.name}"
                    foundInsrc.set(true)
                  }
                } finally {
                  inputStream.close()
                }
              }
            }
          } catch (ZipException zipEx) {
            project.println "Unable to open file ${file.name}"
          }
        })
    
    return foundInsrc.get()
  }
}

