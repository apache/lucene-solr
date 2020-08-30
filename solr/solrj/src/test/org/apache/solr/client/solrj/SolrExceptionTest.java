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

import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.Test;

/**
 * 
 *
 * @since solr 1.3
 */
@ThreadLeakLingering(linger = 250) // TODO something about this test very oddly allows an http2client-scheduler-1 to leak - it's in processworkerExit, tryTerminate, so
// let's just try waiting for a moment until we can nail that down
public class SolrExceptionTest extends SolrTestCase {

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testSolrException() throws Throwable {
    // test a connection to a solr server that probably doesn't exist
    // this is a very simple test and most of the test should be considered verified 
    // if the compiler won't let you by without the try/catch
    boolean gotExpectedError = false;

    try {
      // switched to a local address to avoid going out on the net, ns lookup issues, etc.
      // set a 1ms timeout to let the connection fail faster.
      try (Http2SolrClient client = getHttpSolrClient("http://" + SolrTestCaseJ4.DEAD_HOST_1 + "/solr/", 1)) {
        SolrQuery query = new SolrQuery("test123");
        client.query(query);
      }
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
