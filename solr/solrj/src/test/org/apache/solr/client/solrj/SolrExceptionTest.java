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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

/**
 * 
 *
 * @since solr 1.3
 */
public class SolrExceptionTest extends LuceneTestCase {

  public void testSolrException() throws Throwable {
    // test a connection to a solr server that probably doesn't exist
    // this is a very simple test and most of the test should be considered verified 
    // if the compiler won't let you by without the try/catch
    boolean gotExpectedError = false;
    try {
      // switched to a local address to avoid going out on the net, ns lookup issues, etc.
      // set a 1ms timeout to let the connection fail faster.
      HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
      httpClient.getParams().setParameter("http.connection.timeout", new Integer(1));
      SolrServer client = new CommonsHttpSolrServer("http://localhost:11235/solr/", httpClient);
      SolrQuery query = new SolrQuery("test123");
      client.query(query);
    } catch (SolrServerException sse) {
      gotExpectedError = true;
      /***
      assertTrue(UnknownHostException.class == sse.getRootCause().getClass()
              //If one is using OpenDNS, then you don't get UnknownHostException, instead you get back that the query couldn't execute
              || (sse.getRootCause().getClass() == SolrException.class && ((SolrException) sse.getRootCause()).code() == 302 && sse.getMessage().equals("Error executing query")));
      ***/
    }
    assertTrue(gotExpectedError);
  }
  
}
