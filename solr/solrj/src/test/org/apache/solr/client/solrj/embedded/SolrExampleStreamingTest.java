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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * 
 *
 * @since solr 1.3
 */
@Slow
public class SolrExampleStreamingTest extends SolrExampleTests {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public SolrClient createNewSolrClient()
  {
    try {
      // setup the server...
      String url = jetty.getBaseUrl().toString() + "/collection1";
      // smaller queue size hits locks more often
      ConcurrentUpdateSolrClient concurrentClient = new ErrorTrackingConcurrentUpdateSolrClient.Builder(url)
          .withQueueSize(2)
          .withThreadCount(5)
          .build();
      concurrentClient.setParser(new XMLResponseParser());
      concurrentClient.setRequestWriter(new RequestWriter());
      return concurrentClient;
    }
    
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }

  public void testWaitOptions() throws Exception {
    // SOLR-3903
    final List<Throwable> failures = new ArrayList<>(); // TODO these failures are not the same as recorded by the client
    final String serverUrl = jetty.getBaseUrl().toString() + "/collection1";
    try (ConcurrentUpdateSolrClient concurrentClient = new FailureRecordingConcurrentUpdateSolrClient.Builder(serverUrl)
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

  static class FailureRecordingConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {
    private final List<Throwable> failures = new ArrayList<>();
    
    public FailureRecordingConcurrentUpdateSolrClient(Builder builder) {
      super(builder);
    }
    
    @Override
    public void handleError(Throwable ex) {
      failures.add(ex);
    }
    
    static class Builder extends ConcurrentUpdateSolrClient.Builder {
      public Builder(String baseSolrUrl) {
        super(baseSolrUrl);
      }
   
      @Override
      public FailureRecordingConcurrentUpdateSolrClient build() {
        return new FailureRecordingConcurrentUpdateSolrClient(this);
      }
    }
  }
  
  public static class ErrorTrackingConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {
    public Throwable lastError = null;
    
    public ErrorTrackingConcurrentUpdateSolrClient(Builder builder) {
      super(builder);
    }
        
    @Override
    public void handleError(Throwable ex) {
      lastError = ex;
    }
    
    public static class Builder extends ConcurrentUpdateSolrClient.Builder {

      public Builder(String baseSolrUrl) {
        super(baseSolrUrl);
      }
      
      @Override
      public ErrorTrackingConcurrentUpdateSolrClient build() {
        return new ErrorTrackingConcurrentUpdateSolrClient(this);
      }
    }
  }
}
