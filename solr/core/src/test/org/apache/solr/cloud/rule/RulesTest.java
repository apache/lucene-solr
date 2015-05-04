package org.apache.solr.cloud.rule;

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

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesTest extends AbstractFullDistribZkTestBase {
  static final Logger log = LoggerFactory.getLogger(RulesTest.class);

  @Test
  public void doIntegrationTest() throws Exception {
    String rulesColl = "rulesColl";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setNumShards(1);
      create.setReplicationFactor(2);
      create.setRule("cores:<4", "node:*,replica:1", "disk:>1");
      create.setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

    }

    DocCollection rulesCollection = cloudClient.getZkStateReader().getClusterState().getCollection(rulesColl);
    List list = (List) rulesCollection.get("rule");
    assertEquals(3, list.size());
    assertEquals ( "<4", ((Map)list.get(0)).get("cores"));
    assertEquals("1", ((Map) list.get(1)).get("replica"));
    assertEquals(">1", ((Map) list.get(2)).get("disk"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));

  }



}
