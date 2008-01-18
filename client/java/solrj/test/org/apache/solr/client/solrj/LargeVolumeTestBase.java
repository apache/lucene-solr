/**
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
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

/**
 * @version $Id$
 * @since solr 1.3
 */
public abstract class LargeVolumeTestBase extends SolrExampleTestBase 
{
  SolrServer gserver = null;
  
  // for real load testing, make these numbers bigger
  static final int numdocs = 100; //1000 * 1000;
  static final int threadCount = 5;
  
  public void testMultiThreaded() throws Exception {
    gserver = this.getSolrServer();
    gserver.deleteByQuery( "*:*" ); // delete everything!
    
    DocThread[] threads = new DocThread[threadCount];
    for (int i=0; i<threadCount; i++) {
      threads[i] = new DocThread( "T"+i+":" );
      threads[i].setName("DocThread-" + i);
      threads[i].start();
      System.out.println("Started thread: " + i);
    }
    for (int i=0; i<threadCount; i++) {
      threads[i].join();
    }
    
    query(threadCount * numdocs);
    System.out.println("done");
  }

  private void query(int count) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = gserver.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(count, response.getResults().getNumFound());
  }

  public class DocThread extends Thread {
    
    final SolrServer tserver;
    final String name;
    
    public DocThread( String name )
    {
      tserver = createNewSolrServer();
      this.name = name;
    }
    
    @Override
    public void run() {
      try {
        UpdateResponse resp = null;
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        for (int i = 0; i < numdocs; i++) {
          if (i > 0 && i % 200 == 0) {
            resp = tserver.add(docs);
            assertEquals(0, resp.getStatus());
            docs = new ArrayList<SolrInputDocument>();
          }
          if (i > 0 && i % 5000 == 0) {
            System.out.println(getName() + " - Committing " + i);
            resp = tserver.commit();
            assertEquals(0, resp.getStatus());
          }
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", name+i );
          doc.addField("cat", "foocat");
          docs.add(doc);
        }
        resp = tserver.add(docs);
        assertEquals(0, resp.getStatus());
        resp = tserver.commit();
        assertEquals(0, resp.getStatus());
        resp = tserver.optimize();
        assertEquals(0, resp.getStatus());

      } catch (Exception e) {
        e.printStackTrace();
        fail( getName() + "---" + e.getMessage() );
      }
    }
  }
}
