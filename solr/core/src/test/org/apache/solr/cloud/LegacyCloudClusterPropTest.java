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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CorePropertiesLocator;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;


public class LegacyCloudClusterPropTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {

    // currently this test is fine with a single shard with a single replica and it's simpler. Could easily be
    // extended to multiple shards/replicas, but there's no particular need.
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @After
  public void afterTest() throws Exception {
    cluster.deleteAllCollections();
  }


  // Are all these required?
  private static String[] requiredProps = {
      "numShards",
      "collection.configName",
      "name",
      "replicaType",
      "shard",
      "collection",
      "coreNodeName"
  };

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  //Commented 14-Oct-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 17-Aug-2018
  // commented out on: 01-Apr-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void testCreateCollectionSwitchLegacyCloud() throws Exception {
    createAndTest("legacyTrue", true);
    createAndTest("legacyFalse", false);
  }

  private void createAndTest(final String coll, final boolean legacy) throws Exception {

    // First, just insure that core.properties file gets created with coreNodeName and all other mandatory parameters.
    final String legacyString = Boolean.toString(legacy);
    final String legacyAnti = Boolean.toString(!legacy);
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, legacyString).process(cluster.getSolrClient());
    ClusterProperties props = new ClusterProperties(zkClient());

    assertEquals("Value of legacyCloud cluster prop unexpected", legacyString,
        props.getClusterProperty(ZkStateReader.LEGACY_CLOUD, legacyAnti));

    CollectionAdminRequest.createCollection(coll, "conf", 1, 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(coll, 1, 1);
    
    assertTrue(ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 120000));
    
    // Insure all mandatory properties are there.
    checkMandatoryProps(coll);

    checkCollectionActive(coll);
    // The fixes for SOLR-11503 insure that creating a collection has coreNodeName whether legacyCloud is true or false,
    // we still need to test repairing a properties file that does _not_ have coreNodeName set, the second part of
    // the fix.

    // First, remove the coreNodeName from cluster.properties and write it out it.
    removePropertyFromAllReplicas(coll, "coreNodeName");

    // Now restart Solr, this should repair the removal on core load no matter the value of legacyCloud
    JettySolrRunner jetty = cluster.getJettySolrRunner(0);
    jetty.stop();
    
    cluster.waitForJettyToStop(jetty);
    
    jetty.start();
    
    cluster.waitForAllNodes(30);
    
    checkMandatoryProps(coll);
    checkCollectionActive(coll);
  }

  private void checkCollectionActive(String coll) {
    assertTrue(ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 120000));
    DocCollection docColl = getCollectionState(coll);
    for (Replica rep : docColl.getReplicas()) {
      if (rep.getState() == Replica.State.ACTIVE) return;
    }
    fail("Replica was not active for collection " + coll);
  }
  private void removePropertyFromAllReplicas(String coll, String propDel) throws IOException {
    DocCollection docColl = getCollectionState(coll);

    // First remove the property from all core.properties files
    for (Replica rep : docColl.getReplicas()) {
      final String coreName = rep.getCoreName();
      Properties prop = loadPropFileForReplica(coreName);
      prop.remove(propDel);
      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve(coreName);
      Path corePropFile = Paths.get(expected.toString(), CorePropertiesLocator.PROPERTIES_FILENAME);

      try (Writer os = new OutputStreamWriter(Files.newOutputStream(corePropFile), StandardCharsets.UTF_8)) {
        prop.store(os, "");
      }
    }

    // Now insure it's really gone
    for (Replica rep : docColl.getReplicas()) {
      Properties prop = loadPropFileForReplica(rep.getCoreName());
      assertEquals("Property " + propDel + " should have been deleted",
          "bogus", prop.getProperty(propDel, "bogus"));
    }
  }

  private Properties loadPropFileForReplica(String coreName) throws IOException {
    JettySolrRunner jetty = cluster.getJettySolrRunner(0);
    Path expected = Paths.get(jetty.getSolrHome()).toAbsolutePath().resolve(coreName);
    Path corePropFile = Paths.get(expected.toString(), CorePropertiesLocator.PROPERTIES_FILENAME);
    Properties props = new Properties();
    try (InputStream fis = Files.newInputStream(corePropFile)) {
      props.load(new InputStreamReader(fis, StandardCharsets.UTF_8));
    }
    return props;
  }

  private void checkMandatoryProps(String coll) throws IOException {
    DocCollection docColl = getCollectionState(coll);
    for (Replica rep : docColl.getReplicas()) {
      Properties prop = loadPropFileForReplica(rep.getCoreName());      for (String testProp : requiredProps) {
        String propVal = prop.getProperty(testProp, "bogus");
        if ("bogus".equals(propVal)) {
          fail("Should have found property " + testProp + " in properties file");
        }
      }
    }
  }
}
