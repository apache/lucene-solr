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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.StoppableIndexingThread;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slow
@Ignore
public class CreateCollectionsIndexAndRestartTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void deleteCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void start() throws Exception {
    List<Future> futures = new ArrayList<>();
    List<Future> indexFutures = new ArrayList<>();
    for (int i = 0; i < 10; i ++) {
      final String collectionName = "testCollection" + i;
      ParWork.getRootSharedExecutor().submit(() -> {
        try {
          CollectionAdminRequest.createCollection(collectionName, "conf", 1, 6)
              .setMaxShardsPerNode(100)
              .process(cluster.getSolrClient());
          StoppableIndexingThread indexThread;
          for (int j = 0; j < 2; j++) {
            indexThread = new StoppableIndexingThread(null, cluster.getSolrClient(), Integer.toString(j), false, 100, 10, false);
            indexThread.setCollection(collectionName);
            indexFutures.add(ParWork.getRootSharedExecutor().submit(indexThread));
          }
        } catch (Exception e) {
          log.error("", e);
        }
      });


    }

    for (Future future : futures) {
      future.get(20, TimeUnit.SECONDS);
    }

    for (Future future : indexFutures) {
      future.get(20, TimeUnit.SECONDS);
    }


    for (int i = 0; i < 10; i ++) {
      final String collectionName = "testCollection" + i;
      cluster.waitForActiveCollection(collectionName, 1, 6);
    }


    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      log.info("Restarting {}", runner);
      runner.stop();
    }

    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      log.info("Restarting {}", runner);
      runner.start();
    }


    for (int r = 0; r < 2; r++) {
      for (int i = 0; i < 10; i++) {
        final String collectionName = "testCollection" + i;
        cluster.waitForActiveCollection(collectionName, 1, 6);
      }
    }
  }

}
