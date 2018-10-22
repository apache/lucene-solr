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
package org.apache.solr;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.handler.component.TrackingShardHandlerFactory;
import org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams;
import org.apache.solr.handler.component.TrackingShardHandlerFactory.RequestTrackingQueue;

import java.util.List;
import java.util.Collections;

/**
 * super simple sanity check that SimpleTrackingShardHandler can be used in a 
 * {@link BaseDistributedSearchTestCase} subclass
 */
public class TestSimpleTrackingShardHandler extends BaseDistributedSearchTestCase {

  @Override
  protected String getSolrXml() {
    return "solr-trackingshardhandler.xml";
  }

  public void testSolrXmlOverrideAndCorrectShardHandler() throws Exception {
    RequestTrackingQueue trackingQueue = new RequestTrackingQueue();
    
    TrackingShardHandlerFactory.setTrackingQueue(jettys, trackingQueue);
    // sanity check that our control jetty has the correct configs as well
    TrackingShardHandlerFactory.setTrackingQueue(Collections.singletonList(controlJetty), trackingQueue);
    
    QueryResponse ignored = query("q","*:*", "fl", "id", "sort", "id asc");

    int numShardRequests = 0;
    for (List<ShardRequestAndParams> shard : trackingQueue.getAllRequests().values()) {
      for (ShardRequestAndParams shardReq : shard) {
        numShardRequests++;
      }
    }
    TrackingShardHandlerFactory.setTrackingQueue(jettys, null);
    TrackingShardHandlerFactory.setTrackingQueue(Collections.singletonList(controlJetty), null);
  }
}
