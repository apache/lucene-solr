<div>
  <a href="http://lucene.apache.org/solr/">
    <img src="images/solr.svg" style="width:210px; margin:22px 0px 7px 20px; border:none;" title="Apache Solr Logo" alt="Solr" />
  </a>
  <div style="z-index:100;position:absolute;top:25px;left:226px">
    <span style="font-size: x-small">TM</span>
  </div>
</div>

# Apache Solr™ ${project.version} Documentation

<%
if ("${project.version}" == "${project.baseVersion}") { // compare as strings because of lazy evaluation
  println "Follow [this link to view online documentation](${project.solrDocUrl}) for Apache Solr ${project.version}."
} else {
  println "No online documentation available for custom builds or `SNAPSHOT` versions."
  println "Run `gradlew documentation` from `src.tgz` package to build docs locally."
}
%>
