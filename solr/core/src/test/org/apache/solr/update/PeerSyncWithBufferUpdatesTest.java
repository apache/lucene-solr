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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

@SolrTestCaseJ4.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class PeerSyncWithBufferUpdatesTest  extends BaseDistributedSearchTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static int numVersions = 100;  // number of versions to use when syncing
  private final String FROM_LEADER = DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString();

  private ModifiableSolrParams seenLeader =
      params(DISTRIB_UPDATE_PARAM, FROM_LEADER);

  public PeerSyncWithBufferUpdatesTest() {
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    Set<Integer> docsAdded = new LinkedHashSet<>();
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrClient client0 = clients.get(0);
    SolrClient client1 = clients.get(1);
    SolrClient client2 = clients.get(2);

    long v = 0;
    // add some context
    for (int i = 1; i <= 10; i++) {
      add(client0, seenLeader, sdoc("id",String.valueOf(i),"_version_",++v));
      add(client1, seenLeader, sdoc("id",String.valueOf(i),"_version_",v));
    }

    // jetty1 was down
    for (int i = 11; i <= 15; i++) {
      add(client0, seenLeader, sdoc("id",String.valueOf(i),"_version_",++v));
    }

    // it restarted and must do PeerSync
    SolrCore jetty1Core = jettys.get(1).getCoreContainer().getCores().iterator().next();
    jetty1Core.getUpdateHandler().getUpdateLog().bufferUpdates();
    for (int i = 16; i <= 20; i++) {
      add(client0, seenLeader, sdoc("id",String.valueOf(i),"_version_",++v));
      add(client1, seenLeader, sdoc("id",String.valueOf(i),"_version_",v));
    }

    // some updates are on-wire
    add(client0, seenLeader, sdoc("id","21","_version_",++v));
    add(client0, seenLeader, sdoc("id","22","_version_",++v));

    // this will make a gap in buffer tlog
    add(client0, seenLeader, sdoc("id","23","_version_",++v));
    add(client1, seenLeader, sdoc("id","23","_version_",v));

    // client1 should be able to sync
    assertSync(client1, numVersions, true, shardsArr[0]);

    // on-wire updates arrived on jetty1
    add(client1, seenLeader, sdoc("id","21","_version_",v-2));
    add(client1, seenLeader, sdoc("id","22","_version_",v-1));

    log.info("Apply buffered updates");
    jetty1Core.getUpdateHandler().getUpdateLog().applyBufferedUpdates().get();

    for (int i = 1; i <= 23; i++) docsAdded.add(i);

    validateDocs(docsAdded, client0, client1);

    // random test
    v = 2000;
    if (random().nextBoolean()) {
      for (int i = 24; i <= 30; i++) {
        add(client0, seenLeader, sdoc("id",String.valueOf(i),"_version_",++v));
        add(client1, seenLeader, sdoc("id",String.valueOf(i),"_version_",v));
      }
    }

    log.info("After buffer updates");
    jetty1Core.getUpdateHandler().getUpdateLog().bufferUpdates();
    List<Object> onWireUpdates = new ArrayList<>();
    Set<Integer> docIds = new HashSet<>();

    for (int i = 0; i <= 50; i++) {
      int kindOfUpdate = random().nextInt(100);
      if (docIds.size() < 10) kindOfUpdate = 0;
      //TODO test atomic update
      if (kindOfUpdate <= 50) {
        // add a new document update, may by duplicate with the current one
        int val = random().nextInt(1000);
        int docId = random().nextInt(10000);
        docIds.add(docId);

        SolrInputDocument doc = sdoc("id", docId, "val_i_dvo", val, "_version_",++v);
        add(client0, seenLeader, doc);
        if (random().nextBoolean()) add(client1, seenLeader, doc);
        else onWireUpdates.add(doc);

      } else if (kindOfUpdate <= 65) {
        // delete by query
        ArrayList<Integer> ids = new ArrayList<>(docIds);
        int docId1 = ids.get(random().nextInt(ids.size()));
        int docId2 = ids.get(random().nextInt(ids.size()));

        String query = "id:" +docId1+" OR id:"+docId2;
        String version = Long.toString(-++v);
        delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",version), query);
        if (random().nextBoolean()) {
          delQ(client1, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",version), query);
        } else {
          onWireUpdates.add(new DeleteByQuery(query, version));
        }

      } else {
        // delete by id
        ArrayList<Integer> ids = new ArrayList<>(docIds);
        String docId = ids.get(random().nextInt(ids.size())) + "";
        String version = Long.toString(-++v);

        del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",version), docId);
        if (random().nextBoolean()) {
          del(client1, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",version), docId);
        } else {
          onWireUpdates.add(new DeleteById(docId, version));
        }
      }
    }
    // with many gaps, client1 should be able to sync
    assertSync(client1, numVersions, true, shardsArr[0]);
  }

  private static class DeleteByQuery {
    String query;
    String version;

    public DeleteByQuery(String query, String version) {
      this.query = query;
      this.version = version;
    }
  }

  private static class DeleteById {
    String id;
    String version;

    public DeleteById(String id, String version) {
      this.id = id;
      this.version = version;
    }
  }

  private void validateDocs(Set<Integer> docsAdded, SolrClient client0, SolrClient client1) throws SolrServerException, IOException {
    client0.commit();
    client1.commit();
    QueryResponse qacResponse;
    qacResponse = queryAndCompare(params("q", "*:*", "rows", "10000", "sort","_version_ desc"), client0, client1);
    validateQACResponse(docsAdded, qacResponse);
  }

  void validateQACResponse(Set<Integer> docsAdded, QueryResponse qacResponse) {
    Set<Integer> qacDocs = new LinkedHashSet<>();
    for (int i=0; i<qacResponse.getResults().size(); i++) {
      qacDocs.add(Integer.parseInt(qacResponse.getResults().get(i).getFieldValue("id").toString()));
    }
    assertEquals(docsAdded, qacDocs);
    assertEquals(docsAdded.size(), qacResponse.getResults().getNumFound());
  }

  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "syncWithLeader", syncWith));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("syncWithLeader"));
  }

}
