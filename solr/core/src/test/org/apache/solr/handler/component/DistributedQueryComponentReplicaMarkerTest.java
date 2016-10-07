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
package org.apache.solr.handler.component;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for QueryComponent to set a "replica marker" on each search results.
 * When a query is executed, you could pass this "replica marker" to ensure your query
 * gets executed on the same replica.
 * This will ensure you don't face the "bouncing result" problem, when you don't have the same
 * number of deleted documents.
 *
 * @see QueryComponent
 */
public class DistributedQueryComponentReplicaMarkerTest extends SolrCloudTestCase {

  private static final String COLLECTION = "optimize";
  private static final String id = "id";

  private static final int numShards = 3;
  private static final int numReplicas = 2;
  private static final int maxShardsPerNode = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(numShards*numReplicas/maxShardsPerNode)
        .withSolrXml(TEST_PATH().resolve("solr-trackingshardhandler.xml"))
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, numShards, numReplicas));

    new UpdateRequest()
        .add(sdoc(id, "1", "text", "a", "test_sS", "21", "payload", ByteBuffer.wrap(new byte[]{0x12, 0x62, 0x15})))
        .add(sdoc(id, "2", "text", "b", "test_sS", "22", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x16})))                  //  5
        .add(sdoc(id, "3", "text", "a", "test_sS", "23", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x32, 0x58})))                  //  8
        .add(sdoc(id, "4", "text", "b", "test_sS", "24", "payload", ByteBuffer.wrap(new byte[]{0x25, 0x21, 0x15})))                    //  4
        .add(sdoc(id, "5", "text", "a", "test_sS", "25", "payload", ByteBuffer.wrap(new byte[]{0x35, 0x35, 0x10, 0x00})))              //  9
        .add(sdoc(id, "6", "text", "c", "test_sS", "26", "payload", ByteBuffer.wrap(new byte[]{0x1a, 0x2b, 0x3c, 0x00, 0x00, 0x03})))  //  3
        .add(sdoc(id, "7", "text", "c", "test_sS", "27", "payload", ByteBuffer.wrap(new byte[]{0x00, 0x3c, 0x73})))                    //  1
        .add(sdoc(id, "8", "text", "c", "test_sS", "28", "payload", ByteBuffer.wrap(new byte[]{0x59, 0x2d, 0x4d})))                    // 11
        .add(sdoc(id, "9", "text", "a", "test_sS", "29", "payload", ByteBuffer.wrap(new byte[]{0x39, 0x79, 0x7a})))                    // 10
        .add(sdoc(id, "10", "text", "b", "test_sS", "30", "payload", ByteBuffer.wrap(new byte[]{0x31, 0x39, 0x7c})))                   //  6
        .add(sdoc(id, "11", "text", "d", "test_sS", "31", "payload", ByteBuffer.wrap(new byte[]{(byte) 0xff, (byte) 0xaf, (byte) 0x9c}))) // 13
        .add(sdoc(id, "12", "text", "d", "test_sS", "32", "payload", ByteBuffer.wrap(new byte[]{0x34, (byte) 0xdd, 0x4d})))             //  7
        .add(sdoc(id, "13", "text", "d", "test_sS", "33", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x33})))             // 12
        // SOLR-6545, wild card field list
        .add(sdoc(id, "19", "text", "d", "cat_a_sS", "1", "dynamic_s", "2", "payload", ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x11, 0x34})))
        .commit(cluster.getSolrClient(), COLLECTION);

  }

  @Test
  public void testBasics() throws Exception {

    QueryResponse rsp;
    rsp = cluster.getSolrClient().query(COLLECTION,
        new SolrQuery("q", "*:*", "fl", "id,test_sS,score", "sort", "payload asc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "7", "1", "6", "4", "2", "10", "12", "3", "5", "9", "8", "13", "19", "11");
    assertFieldValues(rsp.getResults(), "test_sS", "27", "21", "26", "24", "22", "30", "32", "23", "25", "29", "28", "33", null, "31");
    rsp = cluster.getSolrClient().query(COLLECTION, new SolrQuery("q", "*:*", "fl", "id,score", "sort", "payload desc", "rows", "20"));
    assertFieldValues(rsp.getResults(), id, "11", "19", "13", "8", "9", "5", "3", "12", "10", "2", "4", "6", "1", "7");

  }

  @Test
  public void testReplicaChoiceMark() throws Exception {
    fail("todo");
  }
}
