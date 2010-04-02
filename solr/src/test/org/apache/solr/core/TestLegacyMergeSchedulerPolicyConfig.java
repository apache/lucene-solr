package org.apache.solr.core;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.AbstractSolrTestCase;

public class TestLegacyMergeSchedulerPolicyConfig extends AbstractSolrTestCase {
  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig-legacy.xml";
  }
  
  public void testLegacy() throws Exception {
    IndexWriter writer = new ExposeWriterHandler().getWriter();
    assertTrue(writer.getMergePolicy().getClass().getName().equals(LogDocMergePolicy.class.getName()));
    assertTrue(writer.getMergeScheduler().getClass().getName().equals(SerialMergeScheduler.class.getName()));
  }
  
  class ExposeWriterHandler extends DirectUpdateHandler2 {
    public ExposeWriterHandler() throws IOException {
      super(h.getCore());
    }

    public IndexWriter getWriter() throws IOException {
      forceOpenWriter();
      return writer;
    }
  }
}
