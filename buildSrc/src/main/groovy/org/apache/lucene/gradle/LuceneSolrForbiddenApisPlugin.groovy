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
            bundledSignatures = ['jdk-unsafe', 'jdk-deprecated', 'jdk-non-portable', 'jdk-reflection']
            signaturesURLs = [getClass().getResource('/forbidden/base.txt')]
            suppressAnnotations = ['**.SuppressForbidden']
        }

        project.tasks.withType(CheckForbiddenApis) {
            if (name.endsWith('Test') || name.startsWith('Test')) {
                signaturesURLs = project.forbiddenApis.signaturesURLs +
                        [getClass().getResource('/forbidden/tests.txt')]
            }
            if (project.group.equals("org.apache.lucene")) {
                signaturesURLs = project.forbiddenApis.signaturesURLs +
                        [getClass().getResource('/forbidden/lucene.txt')]
            }
            if (project.group.equals("org.apache.solr") ) {
                if (project.name.equals("solrj")) {
                    signaturesURLs = project.forbiddenApis.signaturesURLs +
                            [getClass().getResource('/forbidden/solrj.txt')]
                } else {
                    signaturesURLs = project.forbiddenApis.signaturesURLs +
                            [getClass().getResource('/forbidden/solr.txt'), getClass().getResource('/forbidden/servlet-api.txt')]
                }
            }
        }
    }
}
