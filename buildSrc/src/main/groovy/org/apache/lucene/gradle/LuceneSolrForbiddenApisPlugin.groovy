package org.apache.lucene.gradle

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
            bundledSignatures = ['jdk-unsafe', 'jdk-deprecated', 'jdk-non-portable', 'jdk-reflection']
            signaturesURLs = [ getClass().getResource('/forbidden/base.txt') ]
            if (task.name.endsWith('Test')) {
              task.signaturesURLs += getClass().getResource('/forbidden/tests.txt')
            }
            if (project.group.matches(".*?\\.lucene(?:\\.\\w+)?")) {
              task.signaturesURLs += getClass().getResource('/forbidden/lucene.txt')
            } else if (project.name.equals("solrj")) {
              signaturesURLs += getClass().getResource('/forbidden/solrj.txt')
            } else if (project.group.matches(".*?\\.solr(?:\\.\\w+)?")) {
              task.signaturesURLs += getClass().getResource('/forbidden/solr.txt')
              // TODO:
              task.signaturesURLs += getClass().getResource('/forbidden/servlet-api.txt')
              /* something like this would also work for commons-io - does not yet fully work
              project.configurations.compileOnly.allDependencies.matching { it.name == 'javax.servlet-api' }.all {
                  task.signaturesURLs += getClass().getResource('/forbidden/servlet-api.txt')
              }
              */
            }
            println "group " + project.group + " name:" + project.name + " sigs: " + task.signaturesURLs
        }
    }
}
