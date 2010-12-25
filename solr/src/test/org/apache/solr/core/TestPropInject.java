package org.apache.solr.core;

import java.io.IOException;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.AbstractSolrTestCase;

public class TestPropInject extends AbstractSolrTestCase {
  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig-propinject.xml";
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

  public void testMergePolicy() throws Exception {
    ExposeWriterHandler uh = new ExposeWriterHandler();
    IndexWriter writer = uh.getWriter();
    LogByteSizeMergePolicy mp = (LogByteSizeMergePolicy)writer.getConfig().getMergePolicy();
    assertEquals(64.0, mp.getMaxMergeMB(), 0);
    uh.close();
  }
  
  public void testProps() throws Exception {
    ExposeWriterHandler uh = new ExposeWriterHandler();
    IndexWriter writer = uh.getWriter();
    ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler)writer.getConfig().getMergeScheduler();
    assertEquals(2, cms.getMaxThreadCount());
    uh.close();
  }
}
