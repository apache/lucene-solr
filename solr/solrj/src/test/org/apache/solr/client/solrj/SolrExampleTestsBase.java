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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class SolrExampleTestsBase extends SolrJettyTestBase {
  private static Logger log = LoggerFactory
      .getLogger(SolrExampleTestsBase.class);
  
  /**
   * query the example
   */
  @Test
  public void testCommitWithinOnAdd() throws Exception {
    // make sure it is empty...
    SolrServer server = getSolrServer();
    server.deleteByQuery("*:*");// delete everything!
    server.commit();
    QueryResponse rsp = server.query(new SolrQuery("*:*"));
    Assert.assertEquals(0, rsp.getResults().getNumFound());
    
    // Now try a timed commit...
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", "id3", 1.0f);
    doc3.addField("name", "doc3", 1.0f);
    doc3.addField("price", 10);
    UpdateRequest up = new UpdateRequest();
    up.add(doc3);
    up.setCommitWithin(500); // a smaller commitWithin caused failures on the
                             // following assert
    up.process(server);
    
    rsp = server.query(new SolrQuery("*:*"));
    Assert.assertEquals(0, rsp.getResults().getNumFound());
    
    // TODO: not a great way to test this - timing is easily out
    // of whack due to parallel tests and various computer specs/load
    Thread.sleep(1000); // wait 1 sec
    
    // now check that it comes out...
    rsp = server.query(new SolrQuery("id:id3"));
    
    int cnt = 0;
    while (rsp.getResults().getNumFound() == 0) {
      // wait and try again for slower/busier machines
      // and/or parallel test effects.
      
      if (cnt++ == 10) {
        break;
      }
      
      Thread.sleep(2000); // wait 2 seconds...
      
      rsp = server.query(new SolrQuery("id:id3"));
    }
    
    Assert.assertEquals(1, rsp.getResults().getNumFound());
    
    // Now test the new convenience parameter on the add() for commitWithin
    SolrInputDocument doc4 = new SolrInputDocument();
    doc4.addField("id", "id4", 1.0f);
    doc4.addField("name", "doc4", 1.0f);
    doc4.addField("price", 10);
    server.add(doc4, 500);
    
    Thread.sleep(1000); // wait 1 sec
    
    // now check that it comes out...
    rsp = server.query(new SolrQuery("id:id4"));
    
    cnt = 0;
    while (rsp.getResults().getNumFound() == 0) {
      // wait and try again for slower/busier machines
      // and/or parallel test effects.
      
      if (cnt++ == 10) {
        break;
      }
      
      Thread.sleep(2000); // wait 2 seconds...
      
      rsp = server.query(new SolrQuery("id:id3"));
    }
    
    Assert.assertEquals(1, rsp.getResults().getNumFound());
  }
  
  @Test
  public void testCommitWithinOnDelete() throws Exception {
    // make sure it is empty...
    SolrServer server = getSolrServer();
    server.deleteByQuery("*:*");// delete everything!
    server.commit();
    QueryResponse rsp = server.query(new SolrQuery("*:*"));
    Assert.assertEquals(0, rsp.getResults().getNumFound());
    
    // Now add one document...
    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", "id3", 1.0f);
    doc3.addField("name", "doc3", 1.0f);
    doc3.addField("price", 10);
    server.add(doc3);
    server.commit();
    
    // now check that it comes out...
    rsp = server.query(new SolrQuery("id:id3"));
    Assert.assertEquals(1, rsp.getResults().getNumFound());
    
    // now test commitWithin on a delete
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(1000);
    up.deleteById("id3");
    up.process(server);
    
    // the document should still be there
    rsp = server.query(new SolrQuery("id:id3"));
    Assert.assertEquals(1, rsp.getResults().getNumFound());
    
    // check if the doc has been deleted every 250 ms for 30 seconds
    long timeout = System.currentTimeMillis() + 30000;
    do {
      Thread.sleep(250); // wait 250 ms
      
      rsp = server.query(new SolrQuery("id:id3"));
      if (rsp.getResults().getNumFound() == 0) {
        return;
      }
    } while (System.currentTimeMillis() < timeout);
    
    Assert.fail("commitWithin failed to commit");
  }
  
  @Test
  public void testAddDelete() throws Exception {
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery("*:*");// delete everything!
    
    SolrInputDocument[] doc = new SolrInputDocument[3];
    for (int i = 0; i < 3; i++) {
      doc[i] = new SolrInputDocument();
      doc[i].setField("id", i + " & 222", 1.0f);
    }
    String id = (String) doc[0].getField("id").getFirstValue();
    
    server.add(doc[0]);
    server.commit();
    assertNumFound("*:*", 1); // make sure it got in
    
    // make sure it got in there
    server.deleteById(id);
    server.commit();
    assertNumFound("*:*", 0); // make sure it got out
    
    // add it back
    server.add(doc[0]);
    server.commit();
    assertNumFound("*:*", 1); // make sure it got in
    server.deleteByQuery("id:\"" + ClientUtils.escapeQueryChars(id) + "\"");
    server.commit();
    assertNumFound("*:*", 0); // make sure it got out
    
    // Add two documents
    for (SolrInputDocument d : doc) {
      server.add(d);
    }
    server.commit();
    assertNumFound("*:*", 3); // make sure it got in
    
    // should be able to handle multiple delete commands in a single go
    List<String> ids = new ArrayList<String>();
    for (SolrInputDocument d : doc) {
      ids.add(d.getFieldValue("id").toString());
    }
    server.deleteById(ids);
    server.commit();
    assertNumFound("*:*", 0); // make sure it got out
  }
  
  @Test
  public void testStreamingRequest() throws Exception {
    SolrServer server = getSolrServer();
    // Empty the database...
    server.deleteByQuery("*:*");// delete everything!
    server.commit();
    assertNumFound("*:*", 0); // make sure it got in
    
    // Add some docs to the index
    UpdateRequest req = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "" + i);
      doc.addField("cat", "foocat");
      req.add(doc);
    }
    req.setAction(ACTION.COMMIT, true, true);
    req.process(server);
    
    // Make sure it ran OK
    SolrQuery query = new SolrQuery("*:*");
    query.set(CommonParams.FL, "id,score,_docid_");
    QueryResponse response = server.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(10, response.getResults().getNumFound());
    
    // Now make sure each document gets output
    final AtomicInteger cnt = new AtomicInteger(0);
    server.queryAndStreamResponse(query, new StreamingResponseCallback() {
      
      @Override
      public void streamDocListInfo(long numFound, long start, Float maxScore) {
        assertEquals(10, numFound);
      }
      
      @Override
      public void streamSolrDocument(SolrDocument doc) {
        cnt.incrementAndGet();
        
        // Make sure the transformer works for streaming
        Float score = (Float) doc.get("score");
        assertEquals("should have score", new Float(1.0), score);
      }
      
    });
    assertEquals(10, cnt.get());
  }
  
  protected void assertNumFound(String query, int num)
      throws SolrServerException, IOException {
    QueryResponse rsp = getSolrServer().query(new SolrQuery(query));
    if (num != rsp.getResults().getNumFound()) {
      fail("expected: " + num + " but had: " + rsp.getResults().getNumFound()
          + " :: " + rsp.getResults());
    }
  }
}
