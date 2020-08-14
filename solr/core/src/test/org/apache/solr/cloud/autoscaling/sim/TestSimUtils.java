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

package org.apache.solr.cloud.autoscaling.sim;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

/**
 *
 */
public class TestSimUtils extends SolrTestCaseJ4 {

  @Test
  public void testV2toV1() throws Exception {
    // valid patterns

    V2Request req = new V2Request.Builder("/c/myCollection")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'add-replica':{'shard':'shard2','node':'node1:1234','type':'TLOG'}}")
        .withParams(params("foo", "bar"))
        .build();
    SolrParams params = SimUtils.v2AdminRequestToV1Params(req);
    assertEquals("/admin/collections", params.get("path"));
    assertEquals("myCollection", params.get("collection"));
    assertEquals("bar", params.get("foo"));
    assertEquals("addreplica", params.get("action"));
    assertEquals("shard2", params.get("shard"));
    assertEquals("node1:1234", params.get("node"));
    assertEquals("TLOG", params.get("type"));

    req = new V2Request.Builder("/c/myCollection/shards/shard1")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'add-replica':{'shard':'shard2','node':'node1:1234','type':'TLOG'}}")
        .build();
    params = SimUtils.v2AdminRequestToV1Params(req);
    assertEquals("/admin/collections", params.get("path"));
    assertEquals("myCollection", params.get("collection"));
    // XXX should path parameters override the payload, or the other way around?
    assertEquals("shard1", params.get("shard"));

    req = new V2Request.Builder("/c/myCollection/shards/shard1/core_node5")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'deletereplica':{}}")
        .build();
    params = SimUtils.v2AdminRequestToV1Params(req);
    assertEquals("/admin/collections", params.get("path"));
    assertEquals("myCollection", params.get("collection"));
    // XXX should path parameters override the payload, or the other way around?
    assertEquals("shard1", params.get("shard"));
    assertEquals("core_node5", params.get("replica"));

    // invalid patterns
    req = new V2Request.Builder("/invalid/myCollection")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'add-replica':{'shard':'shard2','node':'node1:1234','type':'TLOG'}}")
        .withParams(params("foo", "bar"))
        .build();
    try {
      params = SimUtils.v2AdminRequestToV1Params(req);
    } catch (UnsupportedOperationException e) {
      // expected
      assertTrue(e.toString(), e.toString().contains("request path"));
    }

    req = new V2Request.Builder("/collections/myCollection/foobar/xyz")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'add-replica':{'shard':'shard2','node':'node1:1234','type':'TLOG'}}")
        .withParams(params("foo", "bar"))
        .build();
    try {
      params = SimUtils.v2AdminRequestToV1Params(req);
    } catch (UnsupportedOperationException e) {
      // expected
      assertTrue(e.toString(), e.toString().contains("expected 'shards'"));
    }
  }
}
