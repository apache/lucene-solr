package org.apache.solr.client.solrj.embedded;

import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;

public class SolrExampleStreamingBinaryTest extends SolrExampleTests {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @Override
  public SolrServer createNewSolrServer()
  {
    try {
      // setup the server...
      String url = "http://localhost:"+port+context;
      // smaller queue size hits locks more often
      CommonsHttpSolrServer s = new StreamingUpdateSolrServer( url, 2, 5 ) {
        @Override
        public void handleError(Throwable ex) {
          ex.printStackTrace();
        }
      };
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      s.setParser(new BinaryResponseParser());
      s.setRequestWriter(new BinaryRequestWriter());
      return s;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
}
