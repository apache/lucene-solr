package org.apache.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * <p> Test disabling components</p>
 *
 * @version $Id$
 * @since solr 1.4
 */
public class TestPluginEnable extends AbstractSolrTestCase {


  public void testSimple() throws SolrServerException {
    assertNull(h.getCore().getRequestHandler("disabled"));
    assertNotNull(h.getCore().getRequestHandler("enabled"));

  }


  @Override
  public String getSchemaFile() {
    return "schema-replication1.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig-enableplugin.xml";
  }

}
