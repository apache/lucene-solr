@Library('sfci-pipeline-sharedlib@master') _

env.RELEASE_BRANCHES = ['solr.7.1', 'solr.7.1.seas']

def complianceFlags = [
        enable: true
]

def envDef = [compliance: complianceFlags, buildImage: 'ops0-artifactrepo1-0-prd.data.sfdc.net/solrservice/lucenesolrsfci:solr8blobbase' ]
 
executePipeline(envDef) {
    stage('Init') {
      echo('Checking out git repo..')
      checkout scm
    }

    stage('Clean and Compile') {
      sh("ant clean")

      // withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'sfci-nexus' ,
      // usernameVariable: 'username', passwordVariable: 'password']]) {
      //   sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} compile")
      //   sh("ant -Dbuild.use-salesforce-nexus=true -Dnexus.username=${username} -Dnexus.password=${password} compile-test")
      // }

      // dir("solr"){
      //   sh("ant server")
      // }
    }
    
    // stage('Run tests') {
    //   // Need a try block to make sure stats are published as failed tests returns a sh failure
    //   try{
    //     sh(script: "ant -Dtests.haltonfailure=false test",
    //        returnStatus: true)
    //   }
    //   finally{
    //     // Publishes the tests to Jenkins
    //     junit '**/build/**/test/*xml'
    //   }
    // }
}
