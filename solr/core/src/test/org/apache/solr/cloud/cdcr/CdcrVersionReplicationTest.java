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
package org.apache.solr.cloud.cdcr;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CdcrVersionReplicationTest extends BaseCdcrDistributedZkTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String vfield = CommonParams.VERSION_FIELD;
  SolrClient solrServer;

  public CdcrVersionReplicationTest() {
    schemaString = "schema15.xml";      // we need a string id
    super.createTargetCollection = false;
  }

  SolrClient createClientRandomly() throws Exception {
    int r = random().nextInt(100);

    // testing the smart cloud client (requests to leaders) is more important than testing the forwarding logic
    if (r < 80) {
      return createCloudClient(SOURCE_COLLECTION);
    }

    if (r < 90) {
      return createNewSolrServer(shardToJetty.get(SOURCE_COLLECTION).get(SHARD1).get(random().nextInt(2)).url);
    }

    return createNewSolrServer(shardToJetty.get(SOURCE_COLLECTION).get(SHARD2).get(random().nextInt(2)).url);
  }

  @Test
  @ShardsFixed(num = 4)
  public void testCdcrDocVersions() throws Exception {
    SolrClient client = createClientRandomly();
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      doTestCdcrDocVersions(client);

      commit(SOURCE_COLLECTION); // work arround SOLR-5628
    } finally {
      client.close();
    }
  }

  private void doTestCdcrDocVersions(SolrClient solrClient) throws Exception {
    this.solrServer = solrClient;

    log.info("### STARTING doCdcrTestDocVersions - Add commands, client: {}", solrClient);

    vadd("doc1", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc2", 11, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "11");
    vadd("doc3", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc4", 11, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "11");
    commit(SOURCE_COLLECTION);

    // versions are preserved and verifiable both by query and by real-time get
    doQuery(solrClient, "doc1,10,doc2,11,doc3,10,doc4,11", "q", "*:*");
    doRealTimeGet("doc1,doc2,doc3,doc4", "10,11,10,11");

    vadd("doc1", 5, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "5");
    vadd("doc2", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc3", 9, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "9");
    vadd("doc4", 8, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "8");

    // lower versions are ignored
    doRealTimeGet("doc1,doc2,doc3,doc4", "10,11,10,11");

    vadd("doc1", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc2", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc3", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc4", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");

    // higher versions are accepted
    doRealTimeGet("doc1,doc2,doc3,doc4", "12,12,12,12");

    // non-cdcr update requests throw a version conflict exception for non-equal versions (optimistic locking feature)
    vaddFail("doc1", 13, 409);
    vaddFail("doc2", 13, 409);
    vaddFail("doc3", 13, 409);

    commit(SOURCE_COLLECTION);

    // versions are still as they were
    doQuery(solrClient, "doc1,12,doc2,12,doc3,12,doc4,12", "q", "*:*");

    // query all shard replicas individually
    doQueryShardReplica(SHARD1, "doc1,12,doc2,12,doc3,12,doc4,12", "q", "*:*");
    doQueryShardReplica(SHARD2, "doc1,12,doc2,12,doc3,12,doc4,12", "q", "*:*");

    // optimistic locking update
    vadd("doc4", 12);
    commit(SOURCE_COLLECTION);

    QueryResponse rsp = solrClient.query(params("qt", "/get", "ids", "doc4"));
    long version = (long) rsp.getResults().get(0).get(vfield);

    // update accepted and a new version number was generated
    assertTrue(version > 1_000_000_000_000l);

    log.info("### STARTING doCdcrTestDocVersions - Delete commands");

    // send a delete update with an older version number
    vdelete("doc1", 5, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "5");
    // must ignore the delete
    doRealTimeGet("doc1", "12");

    // send a delete update with a higher version number
    vdelete("doc1", 13, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "13");
    // must be deleted
    doRealTimeGet("doc1", "");

    // send a delete update with a higher version number
    vdelete("doc4", version + 1, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "" + (version + 1));
    // must be deleted
    doRealTimeGet("doc4", "");

    commit(SOURCE_COLLECTION);

    // query each shard replica individually
    doQueryShardReplica(SHARD1, "doc2,12,doc3,12", "q", "*:*");
    doQueryShardReplica(SHARD2, "doc2,12,doc3,12", "q", "*:*");

    // version conflict thanks to optimistic locking
    if (solrClient instanceof CloudSolrClient) // TODO: it seems that optimistic locking doesn't work with forwarding, test with shard2 client
      vdeleteFail("doc2", 50, 409);

    // cleanup after ourselves for the next run
    // deleteByQuery should work as usual with the CDCR_UPDATE param
    doDeleteByQuery("id:doc*", CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, Long.toString(1));
    commit(SOURCE_COLLECTION);

    // deleteByQuery with a version lower than anything else should have no effect
    doQuery(solrClient, "doc2,12,doc3,12", "q", "*:*");

    doDeleteByQuery("id:doc*", CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, Long.toString(51));
    commit(SOURCE_COLLECTION);

    // deleteByQuery with a version higher than everything else should delete all remaining docs
    doQuery(solrClient, "", "q", "*:*");

    // check that replicas are as expected too
    doQueryShardReplica(SHARD1, "", "q", "*:*");
    doQueryShardReplica(SHARD2, "", "q", "*:*");
  }


  // ------------------ auxiliary methods ------------------


  void doQueryShardReplica(String shard, String expectedDocs, String... queryParams) throws Exception {
    for (CloudJettyRunner jetty : shardToJetty.get(SOURCE_COLLECTION).get(shard)) {
      doQuery(jetty.client, expectedDocs, queryParams);
    }
  }

  void vdelete(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setParam(vfield, Long.toString(version));

    for (int i = 0; i < params.length; i += 2) {
      req.setParam(params[i], params[i + 1]);
    }
    solrServer.request(req);
  }

  void vdeleteFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vdelete(id, version, params);
    } catch (SolrException e) {
      failed = true;
      if (e.getCause() instanceof SolrException && e.getCause() != e) {
        e = (SolrException) e.getCause();
      }
      assertEquals(errCode, e.code());
    } catch (SolrServerException ex) {
      Throwable t = ex.getCause();
      if (t instanceof SolrException) {
        failed = true;
        SolrException exception = (SolrException) t;
        assertEquals(errCode, exception.code());
      }
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void vadd(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.add(sdoc("id", id, vfield, version));
    for (int i = 0; i < params.length; i += 2) {
      req.setParam(params[i], params[i + 1]);
    }
    solrServer.request(req);
  }

  void vaddFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vadd(id, version, params);
    } catch (SolrException e) {
      failed = true;
      if (e.getCause() instanceof SolrException && e.getCause() != e) {
        e = (SolrException) e.getCause();
      }
      assertEquals(errCode, e.code());
    } catch (SolrServerException ex) {
      Throwable t = ex.getCause();
      if (t instanceof SolrException) {
        failed = true;
        SolrException exception = (SolrException) t;
        assertEquals(errCode, exception.code());
      }
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void doQuery(SolrClient ss, String expectedDocs, String... queryParams) throws Exception {

    List<String> strs = StrUtils.splitSmart(expectedDocs, ",", true);
    Map<String, Object> expectedIds = new HashMap<>();
    for (int i = 0; i < strs.size(); i += 2) {
      String id = strs.get(i);
      String vS = strs.get(i + 1);
      Long v = Long.valueOf(vS);
      expectedIds.put(id, v);
    }

    QueryResponse rsp = ss.query(params(queryParams));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  void doRealTimeGet(String ids, String versions) throws Exception {
    Map<String, Object> expectedIds = new HashMap<>();
    List<String> strs = StrUtils.splitSmart(ids, ",", true);
    List<String> verS = StrUtils.splitSmart(versions, ",", true);
    for (int i = 0; i < strs.size(); i++) {
      if (!verS.isEmpty()) {
        expectedIds.put(strs.get(i), Long.valueOf(verS.get(i)));
      }
    }

    QueryResponse rsp = solrServer.query(params("qt", "/get", "ids", ids));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  void doDeleteByQuery(String q, String... reqParams) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(q);
    req.setParams(params(reqParams));
    req.process(solrServer);
  }

}

