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

package org.apache.solr.client.solrj.embedded;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;

public class SolrExampleStreamingHttp2Test extends SolrExampleTests {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public SolrClient createNewSolrClient()
  {
    // setup the server...
    String url = jetty.getBaseUrl().toString() + "/collection1";
    // smaller queue size hits locks more often
    Http2SolrClient solrClient = new Http2SolrClient.Builder()
        .build();
    solrClient.setParser(new XMLResponseParser());
    solrClient.setRequestWriter(new RequestWriter());
    ConcurrentUpdateHttp2SolrClient concurrentClient = new ErrorTrackingConcurrentUpdateSolrClient.Builder(url, solrClient)
        .withQueueSize(2)
        .withThreadCount(5)
        .build();
    return concurrentClient;
  }

  public void testWaitOptions() throws Exception {
    // SOLR-3903
    final List<Throwable> failures = new ArrayList<>();
    final String serverUrl = jetty.getBaseUrl().toString() + "/collection1";
    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         ConcurrentUpdateHttp2SolrClient concurrentClient = new FailureRecordingConcurrentUpdateSolrClient.Builder(serverUrl, http2Client)
             .withQueueSize(2)
             .withThreadCount(2)
             .build()) {
      int docId = 42;
      for (UpdateRequest.ACTION action : EnumSet.allOf(UpdateRequest.ACTION.class)) {
        for (boolean waitSearch : Arrays.asList(true, false)) {
          for (boolean waitFlush : Arrays.asList(true, false)) {
            UpdateRequest updateRequest = new UpdateRequest();
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", docId++);
            updateRequest.add(document);
            updateRequest.setAction(action, waitSearch, waitFlush);
            concurrentClient.request(updateRequest);
          }
        }
      }
      concurrentClient.commit();
      concurrentClient.blockUntilFinished();
    }

    if (0 != failures.size()) {
      assertEquals(failures.size() + " Unexpected Exception, starting with...",
          null, failures.get(0));
    }
  }

  static class FailureRecordingConcurrentUpdateSolrClient extends ConcurrentUpdateHttp2SolrClient {
    private final List<Throwable> failures = new ArrayList<>();

    public FailureRecordingConcurrentUpdateSolrClient(Builder builder) {
      super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
      failures.add(ex);
    }

    static class Builder extends ConcurrentUpdateHttp2SolrClient.Builder {
      public Builder(String baseSolrUrl, Http2SolrClient http2Client) {
        super(baseSolrUrl, http2Client);
      }

      @Override
      public FailureRecordingConcurrentUpdateSolrClient build() {
        return new FailureRecordingConcurrentUpdateSolrClient(this);
      }
    }
  }

  public static class ErrorTrackingConcurrentUpdateSolrClient extends ConcurrentUpdateHttp2SolrClient {
    public Throwable lastError = null;

    public ErrorTrackingConcurrentUpdateSolrClient(Builder builder) {
      super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
      lastError = ex;
    }

    public static class Builder extends ConcurrentUpdateHttp2SolrClient.Builder {

      public Builder(String baseSolrUrl, Http2SolrClient http2Client) {
        super(baseSolrUrl, http2Client, true);
      }

      @Override
      public ErrorTrackingConcurrentUpdateSolrClient build() {
        return new ErrorTrackingConcurrentUpdateSolrClient(this);
      }
    }
  }
}
