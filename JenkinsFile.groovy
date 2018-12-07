pipeline {
    agent any
    environment {
        PROJECT = 'solrMainJob'
    }
    tools {
        jdk 'OpenJDK 8'
    }
    stages {
        stage("Set environment variables") {
            steps {
                script {
                    env.PATH = "${tool 'ant'}/bin:${env.PATH}"
                }
            }
        }
        stage("ivy bootstrap") {
            steps {
                sh "echo 'Bootstrapping"
                sh 'ant ivy-bootstrap'
            }
        }
        stage("Build solr") {
            steps {
                sh "echo 'building solr'"
                sh "ant clean"
                sh "ant create-package -f solr/build.xml"
            }
        }
        stage("Archiving packages") {
            steps {
                archive 'solr/package/**/*.zip'
                archive 'solr/package/**/*.tgz'
            }
        }
    }
}