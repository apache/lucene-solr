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
package org.apache.solr.cloud.api.collections;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.OverseerCollectionConfigSetProcessor;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TimeOut;
import org.junit.Test;

public class SimpleCollectionCreateDeleteTest extends AbstractFullDistribZkTestBase {

  public SimpleCollectionCreateDeleteTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void test() throws Exception {
    String overseerNode = OverseerCollectionConfigSetProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    String notOverseerNode = null;
    for (CloudJettyRunner cloudJetty : cloudJettys) {
      if (!overseerNode.equals(cloudJetty.nodeName)) {
        notOverseerNode = cloudJetty.nodeName;
        break;
      }
    }
    String collectionName = "SimpleCollectionCreateDeleteTest";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,1,1)
            .setCreateNodeSet(overseerNode)
            .setStateFormat(2);

    NamedList<Object> request = create.process(cloudClient).getResponse();

    if (request.get("success") != null) {
      assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

      CollectionAdminRequest delete = CollectionAdminRequest.deleteCollection(collectionName);
      cloudClient.request(delete);

      assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
      
      // currently, removing a collection does not wait for cores to be unloaded
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while (true) {
        
        if( timeout.hasTimedOut() ) {
          throw new TimeoutException("Timed out waiting for all collections to be fully removed.");
        }
        
        boolean allContainersEmpty = true;
        for(JettySolrRunner jetty : jettys) {
          
          Collection<SolrCore> cores = jetty.getCoreContainer().getCores();
          for (SolrCore core : cores) {
            CoreDescriptor cd = core.getCoreDescriptor();
            if (cd != null) {
              if (cd.getCloudDescriptor().getCollectionName().equals(collectionName)) {
                allContainersEmpty = false;
              }
            }
          }
        }
        if (allContainersEmpty) {
          break;
        }
      }

      // create collection again on a node other than the overseer leader
      create = CollectionAdminRequest.createCollection(collectionName,1,1)
              .setCreateNodeSet(notOverseerNode)
              .setStateFormat(2);
      request = create.process(cloudClient).getResponse();
      assertTrue("Collection creation should not have failed", request.get("success") != null);
    }
  }
}
