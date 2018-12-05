pipeline {
    stages {
        stage("Build Solr Package") {
            env.PATH = "${tool 'ant'}/bin:${env.PATH}"
            sh 'ant ivy-bootstrap'
            sh "ant clean"
            sh "ant create-package -f solr/build.xml"
            archive 'solr/package/**/*.zip'
            archive 'solr/package/**/*.tgz'
        }
    }
}