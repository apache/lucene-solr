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
package org.apache.solr.client.solrj.impl;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

public class TestCloudSolrClientConnections extends SolrTestCaseJ4 {

  @Test
  public void testCloudClientUploads() throws Exception {

    SolrException e = LuceneTestCase.expectThrows(SolrException.class, () -> {
      MiniSolrCloudCluster cluster = null;
      try  {
         cluster = new MiniSolrCloudCluster(0, SolrTestUtil.createTempDir(), buildJettyConfig("/solr"));
      } finally {
        if (cluster != null) cluster.shutdown();
      }
    });
    assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("cluster not found/not ready"));
  }

  @Test
  public void testAlreadyClosedClusterStateProvider() throws Exception {
    
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(1, SolrTestUtil.createTempDir(),
                                                                  buildJettyConfig("/solr"));
    // from a client perspective the behavior of ZkClientClusterStateProvider should be
    // consistent regardless of wether it's constructed with a zkhost or an existing ZkStateReader
    try {
      final ZkClientClusterStateProvider zkHost_provider
        = new ZkClientClusterStateProvider(cluster.getSolrClient().getZkStateReader());

      checkAndCloseProvider(zkHost_provider);
      
      final ZkStateReader reusedZkReader = new ZkStateReader(cluster.getZkClient());
      try {
        reusedZkReader.createClusterStateWatchersAndUpdate();
        final ZkClientClusterStateProvider reader_provider = new ZkClientClusterStateProvider(reusedZkReader);
        checkAndCloseProvider(reader_provider);
        
        // but in the case of a reused StateZkReader,
        // closing the provider must not have closed the ZkStateReader...
        assertEquals(false, reusedZkReader.isClosed());
        
      } finally {
        reusedZkReader.close();
      }
    } finally {
      cluster.shutdown();
    }
  }

  /** NOTE: will close the provider and assert it starts throwing AlreadyClosedException */
  private void checkAndCloseProvider(final ZkClientClusterStateProvider provider) throws Exception {
    if (random().nextBoolean()) {
      // calling connect should be purely optional and affect nothing
      provider.connect();
    }
    assertNotNull(provider.getClusterState());

    provider.close();

    if (random().nextBoolean()) {
      LuceneTestCase.expectThrows(AlreadyClosedException.class, () -> {
          provider.connect();
        });
    }
    LuceneTestCase.expectThrows(AlreadyClosedException.class, () -> {
        Object ignored = provider.getClusterState();
      });

  }

}
