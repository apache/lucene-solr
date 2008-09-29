package org.apache.solr.core;

import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.Test;

/**
 * @version $Id$
 */
public class TestSolrDeletionPolicy2 extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig-delpolicy2.xml";
  }

  @Test
  public void testFakeDeletionPolicyClass() {

    IndexDeletionPolicyWrapper delPolicy = h.getCore().getDeletionPolicy();
    assertTrue(delPolicy.getWrappedDeletionPolicy() instanceof FakeDeletionPolicy);

    FakeDeletionPolicy f = (FakeDeletionPolicy) delPolicy.getWrappedDeletionPolicy();

    assertTrue("value1".equals(f.getVar1()));
    assertTrue("value2".equals(f.getVar2()));

    assertU(adoc("id", String.valueOf(1),
            "name", "name" + String.valueOf(1)));


    assertTrue(System.getProperty("onInit").equals("test.org.apache.solr.core.FakeDeletionPolicy.onInit"));
    assertU(commit());
    assertQ("return all docs",
            req("id:[0 TO 1]"),
            "*[count(//doc)=1]"
    );


    assertTrue(System.getProperty("onCommit").equals("test.org.apache.solr.core.FakeDeletionPolicy.onCommit"));

    System.clearProperty("onInit");
    System.clearProperty("onCommit");
  }

}
