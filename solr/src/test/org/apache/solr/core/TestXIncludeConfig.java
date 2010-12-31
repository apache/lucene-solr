package org.apache.solr.core;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.request.SolrRequestHandler;

import javax.xml.parsers.DocumentBuilderFactory;


/**
 *
 *
 **/
public class TestXIncludeConfig extends AbstractSolrTestCase {
  protected boolean supports;

  public String getSchemaFile() {
    return "schema.xml";
  }

  //public String getSolrConfigFile() { return "solrconfig.xml"; }
  public String getSolrConfigFile() {
    return "solrconfig-xinclude.xml";
  }

  @Override
  public void setUp() throws Exception {
    File dest = new File("solrconfig-reqHandler.incl");
    dest.deleteOnExit();
    FileUtils.copyFile(getFile("solr/conf/solrconfig-reqHandler.incl"), dest);
    supports = true;
    javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    try {
      //see whether it even makes sense to run this test
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
      super.setUp();
    } catch (UnsupportedOperationException e) {
      supports = false;
    }
  }

  public void testXInclude() throws Exception {
    //Figure out whether this JVM supports XInclude anyway, if it doesn't then don't run this test????
    // TODO: figure out a better way to handle this.
    if (supports == true){
      SolrCore core = h.getCore();
      SolrRequestHandler solrRequestHandler = core.getRequestHandler("includedHandler");
      assertNotNull("Solr Req Handler is null", solrRequestHandler);
    } else {
      log.info("Didn't run testXInclude, because this XML DocumentBuilderFactory doesn't support it");
    }

  }
}
