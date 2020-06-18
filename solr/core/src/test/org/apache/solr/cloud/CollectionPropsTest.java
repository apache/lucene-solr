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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.CollectionPropsWatcher;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
@SolrTestCaseJ4.SuppressSSL
public class CollectionPropsTest extends SolrCloudTestCase {
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
  public void testReadWriteNoCache() throws InterruptedException, IOException {
    CollectionProperties collectionProps = new CollectionProperties(zkClient());

    collectionProps.setCollectionProperty(collectionName, "property1", "value1");
    collectionProps.setCollectionProperty(collectionName, "property2", "value2");
    checkValue("property1", "value1");
    checkValue("property2", "value2");
    
    collectionProps.setCollectionProperty(collectionName, "property1", "value1"); // no change
    checkValue("property1", "value1");

    collectionProps.setCollectionProperty(collectionName, "property1", null);
    collectionProps.setCollectionProperty(collectionName, "property2", "newValue");
    checkValue("property1", null);
    checkValue("property2", "newValue");
    
    collectionProps.setCollectionProperty(collectionName, "property2", null);
    checkValue("property2", null);
    
    collectionProps.setCollectionProperty(collectionName, "property2", null); // no change
    checkValue("property2", null);
  }
  
  @Test
  public void testReadWriteCached() throws InterruptedException, IOException {
    CollectionProperties collectionProps = new CollectionProperties(zkClient());

    // NOTE: Using a semaphore to ensure we wait for Watcher to fire before proceeding with
    // test logic, to prevent triggering SOLR-13678
    final Semaphore sawExpectedProps = new Semaphore(0);
    final AtomicReference<Map<String,String>> expectedProps
      = new AtomicReference<Map<String,String>>(null);
    
    final CollectionPropsWatcher w = new CollectionPropsWatcher() {
      @Override
      public boolean onStateChanged(Map<String,String> collectionProperties) {
        log.info("collection properties changed. Now: {}",  collectionProperties);
        final Map<String,String> expected = expectedProps.get();
        if (expected != null && expected.equals(collectionProperties)) {
          log.info("...new props match expected");
          sawExpectedProps.release();
        }
        return false;
      }
    };
    
    cluster.getSolrClient().getZkStateReader().registerCollectionPropsWatcher(collectionName, w);
    
    collectionProps.setCollectionProperty(collectionName, "property1", "value1");
    collectionProps.setCollectionProperty(collectionName, "property2", "value2");
    waitForValue("property1", "value1", 5000);
    waitForValue("property2", "value2", 5000);

    // HACK: don't let our watcher be removed until we're sure it's "up to date"
    // with the final prop values expected below...
    expectedProps.set(new HashMap<>());

    collectionProps.setCollectionProperty(collectionName, "property1", "value1"); // no change
    checkValue("property1", "value1");

    collectionProps.setCollectionProperty(collectionName, "property1", null);
    collectionProps.setCollectionProperty(collectionName, "property2", "newValue");
    waitForValue("property1", null, 5000);
    waitForValue("property2", "newValue", 5000);
    
    collectionProps.setCollectionProperty(collectionName, "property2", null);
    waitForValue("property2", null, 5000);
    
    collectionProps.setCollectionProperty(collectionName, "property2", null); // no change
    checkValue("property2", null);

    assertTrue("Gave up waitng an excessive amount of time for watcher to see final expected props",
               sawExpectedProps.tryAcquire(1, 120, TimeUnit.SECONDS));
    cluster.getSolrClient().getZkStateReader().removeCollectionPropsWatcher(collectionName, w);
    
    collectionProps.setCollectionProperty(collectionName, "property1", "value1");
    checkValue("property1", "value1"); //Should be no cache, so the change should take effect immediately
    
  }
  
  private void checkValue(String propertyName, String expectedValue) throws InterruptedException {
    final Object value = cluster.getSolrClient().getZkStateReader().getCollectionProperties(collectionName).get(propertyName);
    assertEquals("Unexpected value for collection property: " + propertyName, expectedValue, value);
  }

  private void waitForValue(String propertyName, String expectedValue, int timeout) throws InterruptedException {
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();

    Object lastValueSeen = null;
    for (int i = 0; i < timeout; i += 10) {
      final Object value = zkStateReader.getCollectionProperties(collectionName).get(propertyName);
      if ((expectedValue == null && value == null) ||
          (expectedValue != null && expectedValue.equals(value))) {
        return;
      }
      lastValueSeen = value;
      Thread.sleep(10);
    }
    String collectionpropsInZk = null;
    try {
      collectionpropsInZk = new String(cluster.getZkClient().getData("/collections/" + collectionName + "/collectionprops.json", null, null, true), StandardCharsets.UTF_8);
    } catch (Exception e) {
      collectionpropsInZk = "Could not get file from ZooKeeper: " + e.getMessage();
      log.error("Could not get collectionprops from ZooKeeper for assertion mesage", e);
    }
    
    String propertiesInZkReader = cluster.getSolrClient().getZkStateReader().getCollectionProperties(collectionName).toString();

    fail(String.format(Locale.ROOT, "Could not see value change after setting collection property. Name: %s, current value: %s, expected value: %s. " +
                                    "\ncollectionprops.json file in ZooKeeper: %s" +
                                    "\nCollectionProperties in zkStateReader: %s",
            propertyName, lastValueSeen, expectedValue, collectionpropsInZk, propertiesInZkReader));
  }

  @Test
  public void testWatcher() throws KeeperException, InterruptedException, IOException {
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    CollectionProperties collectionProps = new CollectionProperties(zkClient());

    // Add a watcher to collection props
    final Watcher watcher = new Watcher("Watcher", random().nextBoolean());
    zkStateReader.registerCollectionPropsWatcher(collectionName, watcher);
    assertEquals(0, watcher.waitForTrigger(TEST_NIGHTLY?2000:200));

    // Trigger a new znode event
    log.info("setting value1");
    collectionProps.setCollectionProperty(collectionName, "property", "value1");
    assertEquals(1, watcher.waitForTrigger());
    assertEquals("value1", watcher.getProps().get("property"));

    // Trigger a value change event
    log.info("setting value2");
    collectionProps.setCollectionProperty(collectionName, "property", "value2");
    if (log.isInfoEnabled()) {
      log.info("(value2) waitForTrigger=={}", watcher.waitForTrigger());
    }
    assertEquals("value2", watcher.getProps().get("property"));

    // Delete the properties znode
    log.info("deleting props");
    zkStateReader.getZkClient().delete("/collections/" + collectionName + "/collectionprops.json", -1, true);
    assertEquals(1, watcher.waitForTrigger());
    final Map<String, String> props = watcher.getProps();
    assertTrue(props.toString(), props.isEmpty());

    // Remove watcher and make sure that the watcher is not triggered
    log.info("removing watcher");
    zkStateReader.removeCollectionPropsWatcher(collectionName, watcher);
    log.info("setting value1 (again)");
    collectionProps.setCollectionProperty(collectionName, "property", "value1");
    assertEquals("ZK watcher was triggered after it was removed for collection " + collectionName, 0, watcher.waitForTrigger());
  }

  @Test
  public void testMultipleWatchers() throws InterruptedException, IOException {
    final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    CollectionProperties collectionProps = new CollectionProperties(zkClient());

    // Register the core with ZkStateReader
    zkStateReader.registerCore(collectionName);

    // Subsequent watchers won't be triggered when adding
    final Watcher watcher1 = new Watcher("Watcher1", random().nextBoolean());
    zkStateReader.registerCollectionPropsWatcher(collectionName, watcher1);
    watcher1.waitForTrigger(); // this might still get triggered because of registerCore
    final Watcher watcher2 = new Watcher("Watcher2", random().nextBoolean());
    zkStateReader.registerCollectionPropsWatcher(collectionName, watcher2);
    assertEquals(0, watcher2.waitForTrigger(TEST_NIGHTLY?2000:200));

    // Make sure a value change triggers both watchers
    log.info("setting value1");
    collectionProps.setCollectionProperty(collectionName, "property", "value1");
    assertEquals(1, watcher1.waitForTrigger());
    assertEquals(1, watcher2.waitForTrigger());
    assertEquals("value1", watcher1.getProps().get("property"));
    assertEquals("value1", watcher2.getProps().get("property"));

    // The watchers should be triggered when after the core is unregistered
    zkStateReader.unregisterCore(collectionName);
    log.info("setting value2");
    collectionProps.setCollectionProperty(collectionName, "property", "value2");
    assertEquals(1, watcher1.waitForTrigger());
    assertEquals(1, watcher2.waitForTrigger());
    assertEquals("value2", watcher1.getProps().get("property"));
    assertEquals("value2", watcher2.getProps().get("property"));

    // The watcher should be triggered after another watcher is removed
    log.info("removing watcher2");
    zkStateReader.removeCollectionPropsWatcher(collectionName, watcher2);
    log.info("setting value3");
    collectionProps.setCollectionProperty(collectionName, "property", "value3");
    assertEquals(1, watcher1.waitForTrigger());
    assertEquals("value3", watcher1.getProps().get("property"));

    // The last watcher shouldn't be triggered after removed, even if the core is registered
    zkStateReader.registerCore(collectionName);
    log.info("removing watcher1");
    zkStateReader.removeCollectionPropsWatcher(collectionName, watcher1);
    log.info("setting value4");
    collectionProps.setCollectionProperty(collectionName, "property", "value4");
    assertEquals(0, watcher1.waitForTrigger(TEST_NIGHTLY?2000:200));
  }

  private class Watcher implements CollectionPropsWatcher {
    private final String name;
    private final boolean forceReadPropsFromZk;
    private volatile Map<String, String> props = Collections.emptyMap();
    private final AtomicInteger triggered = new AtomicInteger();

    public Watcher(final String name, final boolean forceReadPropsFromZk) {
      this.name = name;
      this.forceReadPropsFromZk = forceReadPropsFromZk;
      log.info("Watcher '{}' initialized with forceReadPropsFromZk={}", name, forceReadPropsFromZk);
    }
    
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean onStateChanged(Map<String, String> collectionProperties) {
      log.info("{}: state changed...", name);
      if (forceReadPropsFromZk) {
        final ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
        props = Collections.unmodifiableMap(new HashMap(zkStateReader.getCollectionProperties(collectionName)));
        log.info("{}: Setting props from zk={}", name, props);
      } else {
        props = Collections.unmodifiableMap(new HashMap(collectionProperties));
        log.info("{}: Setting props from caller={}", name, props);
      }
      
      synchronized (this) {
        triggered.incrementAndGet();
        log.info("{}: notifying", name);
        notifyAll();
      }

      log.info("{}: done", name);
      return false;
    }

    private Map<String, String> getProps() {
      return props;
    }
    
    private int waitForTrigger() throws InterruptedException {
      return waitForTrigger(1000);
    }

    private int waitForTrigger(int waitTime) throws InterruptedException {
      synchronized (this) {
        if (triggered.get() > 0) {
          return triggered.getAndSet(0);
        }

        wait(waitTime);
        return triggered.getAndSet(0);
      }
    }
  }
}
