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
package org.apache.solr.update.processor;

import java.io.IOException;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;
import org.apache.solr.store.blob.process.CoreUpdateTracker;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.update.CommitUpdateCommand;

public class DistributedZkUpdateProcessorTest extends AbstractZkTestCase {
  /**
   * Tests commit from a NRT replica doesn't trigger sharedStoreTracking.
   * 
   * @throws IOException
   */
  @Test
  public void testNRTReplicaUpdatesZk() throws IOException {

    SolrCore core = h.getCore();
    boolean isZkAware = core.getCoreContainer().isZooKeeperAware();
    assertTrue(isZkAware);
    Replica.Type replicaType = core.getCoreDescriptor().getCloudDescriptor().getReplicaType();
    assertEquals(replicaType, Replica.Type.NRT);
    
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), new ModifiableSolrParams());
    SolrQueryResponse rsp = new SolrQueryResponse();
    CoreUpdateTracker tracker = Mockito.mock(CoreUpdateTracker.class);
    DistributedZkUpdateProcessor processor = new DistributedZkUpdateProcessor(req, rsp, null, tracker);
    
    processor.processCommit(new CommitUpdateCommand(req, false));
    processor.finish();
    verify(tracker, never()).updatingCore(any(PushPullData.class));

  }

  /**
   * TODO: add test yo check SHARED replica triggers sharedStoreTracking.
   * 
   * @throws IOException
   */
}
