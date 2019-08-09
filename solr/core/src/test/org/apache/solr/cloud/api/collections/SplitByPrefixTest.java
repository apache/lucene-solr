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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 *  This class tests higher level SPLITSHARD functionality when splitByPrefix is specified.
 *  See SplitHandlerTest for random tests of lower-level split selection logic.
 */
public class SplitByPrefixTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String COLLECTION_NAME = "c1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");  // needed by cloud-managed config set

    // clould-managed has the copyField from ID to id_prefix
    // cloud-minimal does not and thus histogram should be driven from the "id" field directly
    String configSetName = random().nextBoolean() ? "cloud-minimal" : "cloud-managed";

    configureCluster(1)
        .addConfig("conf", configset(configSetName))  // cloud-managed has the id copyfield to id_prefix
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
  }

  public static class Prefix implements Comparable<Prefix> {
    public String key;
    public DocRouter.Range range;

    @Override
    public int compareTo(Prefix o) {
      return range.compareTo(o.range);
    }

    @Override
    public String toString() {
      return "prefix=" + key + ",range="+range;
    }
  }

  /**
   * find prefixes (shard keys) matching certain criteria
   */
  public static List<Prefix> findPrefixes(int numToFind, int lowerBound, int upperBound) {
    CompositeIdRouter router = new CompositeIdRouter();

    ArrayList<Prefix> prefixes = new ArrayList<>();
    int maxTries = 1000000;
    int numFound = 0;
    for (int i=0; i<maxTries; i++) {
      String shardKey = Integer.toHexString(i)+"!";
      DocRouter.Range range = router.getSearchRangeSingle(shardKey, null, null);
      int lower = range.min;
      if (lower >= lowerBound && lower <= upperBound) {
        Prefix prefix = new Prefix();
        prefix.key = shardKey;
        prefix.range = range;
        prefixes.add(prefix);
        if (++numFound >= numToFind) break;
      }
    }

    Collections.sort(prefixes);

    return prefixes;
  }

  /**
   * remove duplicate prefixes from the SORTED prefix list
   */
  public static List<Prefix> removeDups(List<Prefix> prefixes) {
    ArrayList<Prefix> result = new ArrayList<>();
    Prefix last = null;
    for (Prefix prefix : prefixes) {
      if (last!=null && prefix.range.equals(last.range)) {
        continue;
      }
      last = prefix;
      result.add(prefix);
    }
    return result;
  }

  // Randomly add a second level prefix to test that
  // they are all collapsed to a single bucket.  This behavior should change if/when counting support
  // for more levels of compositeId values
  SolrInputDocument getDoc(String prefix, String unique) {
    String secondLevel = "";
    if (random().nextBoolean()) {
      prefix = prefix.substring(0, prefix.length()-1) + "/16!";  // change "foo!" into "foo/16!" to match 2 level compositeId
      secondLevel="" + random().nextInt(2) + "!";
    }
    return sdoc("id", prefix + secondLevel + unique);
  }


  @Test
  public void doTest() throws IOException, SolrServerException {
    CollectionAdminRequest
        .createCollection(COLLECTION_NAME, "conf", 1, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION_NAME, 1, 1);


    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(COLLECTION_NAME);

    // splitting an empty collection by prefix should still work (i.e. fall back to old method of just dividing the hash range

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(2)
        .setSplitByPrefix(true)
        .setShardName("shard1");
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        COLLECTION_NAME, activeClusterShape(2, 3));  // expectedReplicas==3 because original replica still exists (just inactive)


    List<Prefix> prefixes = findPrefixes(20, 0, 0x00ffffff);
    List<Prefix> uniquePrefixes = removeDups(prefixes);
    if (uniquePrefixes.size() % 2 == 1) {  // make it an even sized list so we can split it exactly in two
      uniquePrefixes.remove(uniquePrefixes.size()-1);
    }
    log.info("Unique prefixes: " + uniquePrefixes);

    for (Prefix prefix : uniquePrefixes) {
      client.add( getDoc(prefix.key, "doc1") );
      client.add( getDoc(prefix.key, "doc2") );
    }
    client.commit();


    splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setSplitByPrefix(true)
        .setShardName("shard1_1");  // should start out with the range of 0-7fffffff
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        COLLECTION_NAME, activeClusterShape(3, 5));

    // OK, now let's check that the correct split point was chosen
    // We can use the router to find the shards for the middle prefixes and they should be different.

    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    Collection<Slice> slices1 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(uniquePrefixes.size()/2 - 1).key, null, collection);
    Collection<Slice> slices2 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(uniquePrefixes.size()/2    ).key, null, collection);

    Slice slice1 = slices1.iterator().next();
    Slice slice2 = slices2.iterator().next();

    assertTrue(slices1.size() == 1 && slices2.size() == 1);
    assertTrue(slice1 != slice2);


    //
    // now lets add enough documents to the first prefix to get it split out on its own
    //
    for (int i=0; i<uniquePrefixes.size(); i++) {
      client.add(  getDoc(uniquePrefixes.get(0).key, "doc"+(i+100)));
    }
    client.commit();

    splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setSplitByPrefix(true)
        .setShardName(slice1.getName());
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        COLLECTION_NAME, activeClusterShape(4, 7));

    collection = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    slices1 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(0).key, null, collection);
    slices2 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(1).key, null, collection);

    slice1 = slices1.iterator().next();
    slice2 = slices2.iterator().next();

    assertTrue(slices1.size() == 1 && slices2.size() == 1);
    assertTrue(slice1 != slice2);


    // Now if we call split (with splitByPrefix) on a shard that has a single prefix, it should split it in half

    splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setSplitByPrefix(true)
        .setShardName(slice1.getName());
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        COLLECTION_NAME, activeClusterShape(5, 9));

    collection = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    slices1 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(0).key, null, collection);
    slice1 = slices1.iterator().next();

    assertTrue(slices1.size() == 2);

    //
    // split one more time, this time on a partial prefix/bucket
    //
    splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setSplitByPrefix(true)
        .setShardName(slice1.getName());
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        COLLECTION_NAME, activeClusterShape(6, 11));

    collection = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    slices1 = collection.getRouter().getSearchSlicesSingle(uniquePrefixes.get(0).key, null, collection);

    assertTrue(slices1.size() == 3);

    // System.err.println("### STATE=" + cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME));
    // System.err.println("### getActiveSlices()=" + cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getActiveSlices());
  }

}
