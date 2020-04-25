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

package org.apache.solr.cloud.overseer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
@SolrTestCaseJ4.SuppressSSL
public class ZkCollectionPropsCachingTest extends SolrCloudTestCase {
  //
  // NOTE: This class can only have one test because our test for caching is to nuke the SolrZkClient to
  // verify that a cached load is going to hit the cache, not try to talk to zk. Any other ZK related test
  // method in this class will fail if it runs after testReadWriteCached, so don't add one! :)
  //
  private String collectionName;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupClass() throws Exception {
    Boolean useLegacyCloud = rarely();
    log.info("Using legacyCloud?: {}", useLegacyCloud);

    configureCluster(4)
        .withProperty(ZkStateReader.LEGACY_CLOUD, String.valueOf(useLegacyCloud))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    collectionName = "CollectionPropsTest" + System.nanoTime();

    CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2);
    CollectionAdminResponse response = request.process(cluster.getSolrClient());
    assertTrue("Unable to create collection: " + response.toString(), response.isSuccess());
  }

  @Test
  public void testReadWriteCached() throws InterruptedException, IOException {
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();

    CollectionProperties collectionProps = new CollectionProperties(zkClient());

    collectionProps.setCollectionProperty(collectionName, "property1", "value1");
    checkValue("property1", "value1"); //Should be no cache, so the change should take effect immediately

    zkStateReader.getCollectionProperties(collectionName,9000);
    zkStateReader.getZkClient().close();
    assertFalse(zkStateReader.isClosed());
    checkValue("property1", "value1"); //Should be cached, so the change should not try to hit zk

    Thread.sleep(10000); // test the timeout feature
    try {
      checkValue("property1", "value1"); //Should not be cached anymore
      fail("cache should have expired, prev line should throw an exception trying to access zookeeper after closed");
    } catch (Exception e) {
      // expected, because we killed the client in zkStateReader.
    }
  }

  private void checkValue(String propertyName, String expectedValue) throws InterruptedException {
    final Object value = cluster.getSolrClient().getZkStateReader().getCollectionProperties(collectionName).get(propertyName);
    assertEquals("Unexpected value for collection property: " + propertyName, expectedValue, value);
  }



}
