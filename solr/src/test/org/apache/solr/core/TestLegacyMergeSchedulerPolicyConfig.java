package org.apache.solr.core;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLegacyMergeSchedulerPolicyConfig extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-legacy.xml", "schema.xml");
  }

  @Test
  public void testLegacy() throws Exception {
    ExposeWriterHandler duh = new ExposeWriterHandler();
    IndexWriter writer = duh.getWriter();
    assertTrue(writer.getConfig().getMergePolicy().getClass().getName().equals(LogDocMergePolicy.class.getName()));
    assertTrue(writer.getConfig().getMergeScheduler().getClass().getName().equals(SerialMergeScheduler.class.getName()));
    duh.close();
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
