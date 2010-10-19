package org.apache.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * <p> Test disabling components</p>
 *
 * @version $Id$
 * @since solr 1.4
 */
public class TestPluginEnable extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-enableplugin.xml", "schema-replication1.xml");
  }
  
  @Test
  public void testSimple() throws SolrServerException {
    assertNull(h.getCore().getRequestHandler("disabled"));
    assertNotNull(h.getCore().getRequestHandler("enabled"));

  }
}
