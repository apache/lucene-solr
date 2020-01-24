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
package org.apache.solr.cloud;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.SnapShooter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.Slow
public class CleanupOldIndexTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    // we restart jetty and expect to find on disk data - need a local fs directory
    useFactory(null);
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void afterClass() throws Exception {

    if (null != cluster && suiteFailureMarker.wasSuccessful()) {
      zkClient().printLayoutToStream(System.out);
    }

  }

  private static final String COLLECTION = "oldindextest";

  @Test
  public void test() throws Exception {

    CollectionAdminRequest.createCollection(COLLECTION, "conf1", 1, 2)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().setDefaultCollection(COLLECTION); // TODO make this configurable on StoppableIndexingThread

    int[] maxDocList = new int[] {300, 500, 700};
    int maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];

    StoppableIndexingThread indexThread = new StoppableIndexingThread(null, cluster.getSolrClient(), "1", true, maxDoc, 1, true);
    indexThread.start();

    // give some time to index...
    int[] waitTimes = new int[] {3000, 4000};
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);

    // create some "old" index directories
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    CoreContainer coreContainer = jetty.getCoreContainer();
    File dataDir = null;
    try (SolrCore solrCore = coreContainer.getCore(coreContainer.getCoreDescriptors().get(0).getName())) {
      dataDir = new File(solrCore.getDataDir());
    }
    assertTrue(dataDir.isDirectory());

    long msInDay = 60*60*24L;
    String timestamp1 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date(1*msInDay));
    String timestamp2 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date(2*msInDay));
    File oldIndexDir1 = new File(dataDir, "index."+timestamp1);
    FileUtils.forceMkdir(oldIndexDir1);
    File oldIndexDir2 = new File(dataDir, "index."+timestamp2);
    FileUtils.forceMkdir(oldIndexDir2);

    // verify the "old" index directories exist
    assertTrue(oldIndexDir1.isDirectory());
    assertTrue(oldIndexDir2.isDirectory());

    // bring shard replica down
    jetty.stop();

    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);

    // bring shard replica up
    jetty.start();

    // make sure replication can start
    Thread.sleep(3000);

    // stop indexing threads
    indexThread.safeStop();
    indexThread.join();

    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 2));

    assertTrue(!oldIndexDir1.isDirectory());
    assertTrue(!oldIndexDir2.isDirectory());
  }


}
