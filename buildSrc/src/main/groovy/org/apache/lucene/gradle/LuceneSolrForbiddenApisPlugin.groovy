package org.apache.lucene.gradle

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project

class LuceneSolrForbiddenApisPlugin implements Plugin<Project> {

  // TODO: somehow determine this from versions.props
  static final String COMMONS_IO_VERSION = '2.5'

  @Override
  void apply(Project project) {
    project.pluginManager.apply(ForbiddenApisPlugin.class)
    project.forbiddenApis {
      failOnUnsupportedJava = false
      suppressAnnotations = ['**.SuppressForbidden']
    }

    project.tasks.withType(CheckForbiddenApis) { task ->
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
        task.signaturesURLs += getClass().getResource((project.name == 'solrj') ? '/forbidden/solrj.txt' : '/forbidden/solr.txt')
        task.bundledSignatures += [ 'commons-io-unsafe-' + COMMONS_IO_VERSION ]
        
        // we delay adding the servlet-api checks until we figured out that we have a servlet-api.jar on forbidden's classpath:
        task.doFirst{
          if (task.classpath.filter { it.name ==~ /.*?\bservlet-api\b.*?\.jar/ }.empty == false) {
            task.signaturesURLs += getClass().getResource('/forbidden/servlet-api.txt')
          }
        }
      }
    }
  }
  
}
