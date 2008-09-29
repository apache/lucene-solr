package org.apache.solr.core;

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Test;

import java.util.Map;

/**
 * @version $Id$
 */
public class TestSolrDeletionPolicy1 extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig-delpolicy1.xml";
  }

  private void addDocs() {

    assertU(adoc("id", String.valueOf(1),
            "name", "name" + String.valueOf(1)));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 1]"),
            "*[count(//doc)=1]"
    );

    assertU(adoc("id", String.valueOf(2),
            "name", "name" + String.valueOf(2)));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 2]"),
            "*[count(//doc)=2]"
    );

    assertU(adoc("id", String.valueOf(3),
            "name", "name" + String.valueOf(3)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 3]"),
            "*[count(//doc)=3]"
    );

    assertU(adoc("id", String.valueOf(4),
            "name", "name" + String.valueOf(4)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 4]"),
            "*[count(//doc)=4]"
    );

    assertU(adoc("id", String.valueOf(5),
            "name", "name" + String.valueOf(5)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 5]"),
            "*[count(//doc)=5]"
    );

  }

  @Test
  public void testKeepOptimizedOnlyCommits() {

    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    IndexCommit latest = delPolicy.getLatestCommit();
    for (Long version : commits.keySet()) {
      if (commits.get(version) == latest)
        continue;
      assertTrue(commits.get(version).isOptimized());
    }
  }

  @Test
  public void testNumCommitsConfigured() {
    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    assertTrue(commits.size() == ((SolrDeletionPolicy) (delPolicy.getWrappedDeletionPolicy())).getMaxCommitsToKeep());
  }

  @Test
  public void testCommitAge() throws InterruptedException {
    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    addDocs();
    Map<Long, IndexCommit> commits = delPolicy.getCommits();
    IndexCommit ic = delPolicy.getLatestCommit();
    String agestr = ((SolrDeletionPolicy) (delPolicy.getWrappedDeletionPolicy())).getMaxCommitAge().replaceAll("[a-zA-Z]", "").replaceAll("-", "");
    long age = Long.parseLong(agestr) * 1000;
    Thread.sleep(age);

    assertU(adoc("id", String.valueOf(6),
            "name", "name" + String.valueOf(6)));
    assertU(optimize());
    assertQ("return all docs",
            req("id:[0 TO 6]"),
            "*[count(//doc)=6]"
    );

    commits = delPolicy.getCommits();
    assertTrue(!commits.containsKey(ic.getVersion()));
  }

}
