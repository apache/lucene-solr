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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean.Category;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * Tests the Cloud Collections API.
 */
public class CollectionsAPIDistributedZkTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static volatile String configSet = "cloud-minimal";
  protected static String getConfigSet() {
    return configSet;
  }

  @Before
  public void setupCluster() throws Exception {
    // we don't want this test to have zk timeouts
    System.setProperty("zkClientTimeout", "60000");
    if (TEST_NIGHTLY) {
      System.setProperty("createCollectionWaitTimeTillActive", "10");
      TestInjection.randomDelayInCoreCreation = "true:5";
    } else {
      System.setProperty("createCollectionWaitTimeTillActive", "5");
      TestInjection.randomDelayInCoreCreation = "true:1";
    }

    configureCluster(4)
        .addConfig("conf", configset(getConfigSet()))
        .addConfig("conf2", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();
  }
  
  @After
  public void tearDownCluster() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    String collectionName = "out_of_sync_collection";

    // manually create a collections zknode
    cluster.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    CollectionAdminRequest.deleteCollection(collectionName)
        .process(cluster.getSolrClient());

    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient())
                  .contains(collectionName));

    assertFalse(cluster.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void testBadActionNames() {
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testMissingRequiredParameters() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("numShards", 2);
    // missing required collection parameter
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

}
