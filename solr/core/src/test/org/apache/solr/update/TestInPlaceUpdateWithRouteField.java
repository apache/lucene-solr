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

package org.apache.solr.update;

import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class TestInPlaceUpdateWithRouteField extends SolrCloudTestCase {

  private static final int NUMBER_OF_DOCS = 100;

  private static final String COLLECTION = "collection1";
  private static final String[] shards = new String[]{"shard1","shard2","shard3"};

  @BeforeClass
  public static void setupCluster() throws Exception {
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    String configName = "solrCloudCollectionConfig";
    int nodeCount = TestUtil.nextInt(random(), 1, 3);
    configureCluster(nodeCount)

        .addConfig(configName, configDir)
        .configure();

    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml" );
    collectionProperties.put("schema", "schema-inplace-updates.xml");

    int replicas = 2;
    // router field can be defined either  for ImplicitDocRouter or CompositeIdRouter
    boolean implicit = random().nextBoolean();
    String routerName = implicit ? "implicit":"compositeId";
    Create createCmd = CollectionAdminRequest.createCollection(COLLECTION, configName, shards.length, replicas)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(shards.length * replicas)
        .setProperties(collectionProperties)
        .setRouterName(routerName)
        .setRouterField("shardName");
    if (implicit) {
      createCmd.setShards(Arrays.stream(shards).collect(Collectors.joining(",")));
    }
    createCmd.process(cluster.getSolrClient());
  }

  @Test
  public void testUpdatingDocValuesWithRouteField() throws Exception {

     new UpdateRequest()
      .deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTION);
    
     new UpdateRequest().add(createDocs(NUMBER_OF_DOCS)).commit(cluster.getSolrClient(), COLLECTION);

    int id = TestUtil.nextInt(random(), 1, NUMBER_OF_DOCS - 1);
    SolrDocument solrDocument = queryDoc(id);
    Long initialVersion = (Long) solrDocument.get("_version_");
    Integer luceneDocId = (Integer) solrDocument.get("[docid]");
    String shardName = (String) solrDocument.get("shardName");
    Assert.assertThat(solrDocument.get("inplace_updatable_int"), is(id));

    int newDocValue = TestUtil.nextInt(random(), 1, 2 * NUMBER_OF_DOCS - 1);
    SolrInputDocument sdoc = sdoc("id", ""+id,
        // use route field in update command
        "shardName", shardName,
        "inplace_updatable_int", map("set", newDocValue));
    
    UpdateRequest updateRequest = new UpdateRequest()
        .add(sdoc);
    
    // since this atomic update will be done in place, it shouldn't matter if we specify this param, or what it's value is
    if (random().nextBoolean()) {
      updateRequest.setParam(UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE, Boolean.toString(random().nextBoolean()));
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTION);
    solrDocument = queryDoc(id);
    Long newVersion = (Long) solrDocument.get("_version_");
    Assert.assertTrue("Version of updated document must be greater than original one",
        newVersion > initialVersion);
    Assert.assertThat( "Doc value must be updated", solrDocument.get("inplace_updatable_int"), is(newDocValue));
    Assert.assertThat("Lucene doc id should not be changed for In-Place Updates.", solrDocument.get("[docid]"), is(luceneDocId));

    sdoc.remove("shardName");
    checkWrongCommandFailure(sdoc);

    sdoc.addField("shardName",  map("set", "newShardName"));
    checkWrongCommandFailure(sdoc);

    sdoc.setField("shardName", shardName);
    
    // if we now attempt an atomic update that we know can't be done in-place, this should fail...
    sdoc.addField("title_s", map("set", "this is a string that can't be updated in place"));
    final SolrException e = expectThrows(SolrException.class, () -> {
        final UpdateRequest r = new UpdateRequest();
        r.add(sdoc);
        r.setParam(UpdateParams.REQUIRE_PARTIAL_DOC_UPDATES_INPLACE, "true");
        r.process(cluster.getSolrClient(), COLLECTION);
      });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertThat(e.getMessage(), containsString("Unable to update doc in-place: " + id));
  }

  private void checkWrongCommandFailure(SolrInputDocument sdoc) throws SolrServerException, IOException {
    try {
      new UpdateRequest().add(sdoc).process(cluster.getSolrClient(), COLLECTION);
      fail("expect an exception for wrong update command");
    } catch (SolrException ex) {
      assertThat("expecting 400 in " + ex.getMessage(), ex.code(), is(400));
    }
  }

  private Collection<SolrInputDocument> createDocs(int number) {
    List<SolrInputDocument> result = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      String randomShard = shards[random().nextInt(shards.length)];
      result.add(sdoc("id", String.valueOf(i),
          "shardName", randomShard,
          "inplace_updatable_int", i));
    }
    return result;
  }

  private SolrDocument queryDoc(int id) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery(
        "q", "id:" + id,
        "fl", "_version_,inplace_updatable_int,[docid],shardName",
        "targetCollection", COLLECTION);
    QueryResponse response = cluster.getSolrClient().query(COLLECTION, query);
    SolrDocumentList result = (SolrDocumentList) response.getResponse().get("response");
    Assert.assertThat(result.getNumFound(), is(1L));
    return result.get(0);
  }
}
