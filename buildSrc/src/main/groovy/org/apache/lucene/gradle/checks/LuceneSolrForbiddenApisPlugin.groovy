package org.apache.lucene.gradle.checks
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

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project

class LuceneSolrForbiddenApisPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    project.pluginManager.apply(ForbiddenApisPlugin.class)
    project.forbiddenApis {
      failOnUnsupportedJava = false
      suppressAnnotations = ['**.SuppressForbidden']
    }

    project.tasks.withType(CheckForbiddenApis) { task ->
      task.failOnUnresolvableSignatures = false
      task.bundledSignatures = ['jdk-unsafe', 'jdk-deprecated', 'jdk-non-portable', 'jdk-reflection']
      task.signaturesURLs = [ getClass().getResource('/forbidden/base.txt') ]
      
      if (task.name.endsWith('Test') || project.name ==~ /.*?\btest-framework/) {
        task.signaturesURLs += getClass().getResource('/forbidden/tests.txt')
      } else {
        task.bundledSignatures += 'jdk-system-out'
      }
      
      if (project.group ==~ /.*?\.lucene(?:\.\w+)?/) {
        task.signaturesURLs += getClass().getResource('/forbidden/lucene.txt')
      } else if (project.group ==~ /.*?\.solr(?:\.\w+)?/) {
        task.signaturesURLs += getClass().getResource((project.name == 'solr-solrj') ? '/forbidden/solrj.txt' : '/forbidden/solr.txt')
        
        task.doFirst{
          task.bundledSignatures += [ 'commons-io-unsafe-' + project.getVersion("commons-io:commons-io") ]
          
          // we delay adding the servlet-api checks until we figured out that we have a servlet-api.jar on forbidden's classpath:
          if (task.classpath.filter { it.name ==~ /.*?\bservlet-api\b.*?\.jar/ }.empty == false) {
            task.signaturesURLs += getClass().getResource('/forbidden/servlet-api.txt')
          }
        }
      }
    }
  }
  
}
