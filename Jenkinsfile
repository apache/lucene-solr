@Library('sfci-pipeline-sharedlib@master') _

env.RELEASE_BRANCHES = ['solr.7.1', 'solr.7.1.seas']

def complianceFlags = [
        enable: true
]

def envDef = [compliance: complianceFlags, buildImage: 'ops0-artifactrepo1-0-prd.data.sfdc.net/solrservice/lucenesolrsfci:latest' ]
 
executePipeline(envDef) {
    stage('Init') {
      echo('Checking out git repo..')
      checkout scm
      def nexusCredsId = 'sfci-nexus'
      withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: nexusCredsId ,
      usernameVariable: 'username', passwordVariable: 'password']]) {
        sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} clean")
        sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} compile")
        dir("solr"){
          sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} dist package-local-src-tgz")
          sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} server")
        }
        // Need a try block to make sure stats are published as failed tests returns a sh failure
        try{
          sh(script: "ant -Dtests.haltonfailure=false -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} test",
             returnStatus: true)
        }
        finally{
          // Publishes the tests to Jenkins
          junit '**/build/**/test/*xml'
        }
      }
    }
}
