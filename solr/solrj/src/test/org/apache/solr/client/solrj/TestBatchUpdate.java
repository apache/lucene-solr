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

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Test for SOLR-1038
 *
 * @since solr 1.4
 *
 */
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestBatchUpdate extends SolrJettyTestBase {

  @Before
  public void setUp() throws Exception {
    jetty = createAndStartJetty(legacyExampleCollection1SolrHome());
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  static final int numdocs = 1000;  


  @Test
  public void testWithXml() throws Exception {
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.setRequestWriter(new RequestWriter());
    client.deleteByQuery("*:*"); // delete everything!
    doIt(client);
  }

  @Test
  public void testWithBinary()throws Exception{
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.setRequestWriter(new BinaryRequestWriter());
    client.deleteByQuery("*:*"); // delete everything!
    doIt(client);
  }

  @Test
  public void testWithBinaryBean()throws Exception{
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.setRequestWriter(new BinaryRequestWriter());
    client.deleteByQuery("*:*"); // delete everything!
    final int[] counter = new int[1];
    counter[0] = 0;
    client.addBeans(new Iterator<Bean>() {

      @Override
      public boolean hasNext() {
        return counter[0] < numdocs;
      }

      @Override
      public Bean next() {
        Bean bean = new Bean();
        bean.id = "" + (++counter[0]);
        bean.cat = "foocat";
        return bean;
      }

      @Override
      public void remove() {
        //do nothing
      }
    });
    client.commit();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = client.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(numdocs, response.getResults().getNumFound());
  }

  public static class Bean{
    @Field
    String id;
    @Field
    String cat;
  }
       
  private void doIt(Http2SolrClient client) throws SolrServerException, IOException {
    final int[] counter = new int[1];
    counter[0] = 0;
    client.add(new Iterator<SolrInputDocument>() {

      @Override
      public boolean hasNext() {
        return counter[0] < numdocs;
      }

      @Override
      public SolrInputDocument next() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "" + (++counter[0]));
        doc.addField("cat", "foocat");
        return doc;
      }

      @Override
      public void remove() {
        //do nothing

      }
    });
    client.commit();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = client.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(numdocs, response.getResults().getNumFound());
  }
}
