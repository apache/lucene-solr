/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.EmbeddedSolrServerTestBase;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @since solr 1.3
 */
public abstract class LargeVolumeTestBase extends EmbeddedSolrServerTestBase
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // for real load testing, make these numbers bigger
  static final int numdocs = 100; //1000 * 1000;
  static final int threadCount = 5;

  @Test
  public void testMultiThreaded() throws Exception {
    SolrClient client = this.getSolrClient();
    client.deleteByQuery("*:*"); // delete everything!
    
    DocThread[] threads = new DocThread[threadCount];
    for (int i=0; i<threadCount; i++) {
      threads[i] = new DocThread( "T"+i+":" );
      threads[i].setName("DocThread-" + i);
      threads[i].start();
      log.info("Started thread: " + i);
    }
    for (int i=0; i<threadCount; i++) {
      threads[i].join();
    }

    // some of the commits could have failed because maxWarmingSearchers exceeded,
    // so do a final commit to make sure everything is visible.
    // This should no longer be true as of SOLR-9712 (Solr 6.4)
    // client.commit();
    
    query(threadCount * numdocs);
    log.info("done");
  }

  private void query(int count) throws SolrServerException, IOException {
    SolrClient client = this.getSolrClient();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = client.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(count, response.getResults().getNumFound());
  }

  public class DocThread extends Thread {
    
    final SolrClient client;
    final String name;
    
    public DocThread( String name )
    {
      client = createNewSolrClient();
      this.name = name;
    }
    
    @Override
    public void run() {
      try {
        UpdateResponse resp = null;
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < numdocs; i++) {
          if (i > 0 && i % 200 == 0) {
            resp = client.add(docs);
            assertEquals(0, resp.getStatus());
            docs = new ArrayList<>();
          }
          if (i > 0 && i % 5000 == 0) {
            log.info(getName() + " - Committing " + i);
            resp = client.commit();
            assertEquals(0, resp.getStatus());
          }
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", name+i );
          doc.addField("cat", "foocat");
          docs.add(doc);
        }
        resp = client.add(docs);
        assertEquals(0, resp.getStatus());

        try {
        resp = client.commit();
        assertEquals(0, resp.getStatus());
        resp = client.optimize();
        assertEquals(0, resp.getStatus());
        } catch (Exception e) {
          // a commit/optimize can fail with a too many warming searchers exception
          log.info("Caught benign exception during commit: " + e.getMessage());
        }
        if (!(client instanceof EmbeddedSolrServer)) {
          client.close();
        }

      } catch (Exception e) {
        e.printStackTrace();
        fail( getName() + "---" + e.getMessage() );
      }
    }
  }
}
