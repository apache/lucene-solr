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

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.junit.Test;
import static org.hamcrest.core.StringContains.containsString;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class PeerSyncTest extends BaseDistributedSearchTestCase {
  protected static int numVersions = 100;  // number of versions to use when syncing
  protected static final String FROM_LEADER = DistribPhase.FROMLEADER.toString();
  protected static final ModifiableSolrParams seenLeader =
    params(DISTRIB_UPDATE_PARAM, FROM_LEADER);
  
  public PeerSyncTest() {
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";

    // validate that the schema was not changed to an unexpected state
    try {
      initCore(configString, schemaString);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    IndexSchema schema = h.getCore().getLatestSchema();
    assertTrue(schema.getFieldOrNull("_version_").hasDocValues() && !schema.getFieldOrNull("_version_").indexed()
        && !schema.getFieldOrNull("_version_").stored());
    assertTrue(!schema.getFieldOrNull("val_i_dvo").indexed() && !schema.getFieldOrNull("val_i_dvo").stored() &&
        schema.getFieldOrNull("val_i_dvo").hasDocValues());
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
    add(client0, seenLeader, sdoc("id","1","_version_",++v));

    // this fails because client0 has no context (i.e. no updates of its own to judge if applying the updates
    // from client1 will bring it into sync with client1)
    assertSync(client1, numVersions, false, shardsArr[0]);

    // bring client1 back into sync with client0 by adding the doc
    add(client1, seenLeader, sdoc("id","1","_version_",v));

    // both have the same version list, so sync should now return true
    assertSync(client1, numVersions, true, shardsArr[0]);
    // TODO: test that updates weren't necessary

    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*"), client0, client1);

    add(client0, seenLeader, addRandFields(sdoc("id","2","_version_",++v)));

    // now client1 has the context to sync
    assertSync(client1, numVersions, true, shardsArr[0]);

    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*"), client0, client1);

    add(client0, seenLeader, addRandFields(sdoc("id","3","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","4","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","5","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","6","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","7","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","8","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","9","_version_",++v)));
    add(client0, seenLeader, addRandFields(sdoc("id","10","_version_",++v)));
    for (int i=0; i<10; i++) docsAdded.add(i+1);
    assertSync(client1, numVersions, true, shardsArr[0]);

    validateDocs(docsAdded, client0, client1);

    testOverlap(docsAdded, client0, client1, v);
    // test delete and deleteByQuery
    v=1000;
    SolrInputDocument doc = sdoc("id","1000","_version_",++v);
    add(client0, seenLeader, doc);
    add(client0, seenLeader, sdoc("id","1001","_version_",++v));
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "id:1001 OR id:1002");
    add(client0, seenLeader, sdoc("id","1002","_version_",++v));
    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "1000");
    docsAdded.add(1002); // 1002 added

    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);

    // test that delete by query is returned even if not requested, and that it doesn't delete newer stuff than it should
    v=2000;
    SolrClient client = client0;
    add(client, seenLeader, sdoc("id","2000","_version_",++v));
    add(client, seenLeader, sdoc("id","2001","_version_",++v));
    delQ(client, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "id:2001 OR id:2002");
    add(client, seenLeader, sdoc("id","2002","_version_",++v));
    del(client, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "2000");
    docsAdded.add(2002); // 2002 added

    v=2000;
    client = client1;
    add(client, seenLeader, sdoc("id","2000","_version_",++v));
    ++v;  // pretend we missed the add of 2001.  peersync should retrieve it, but should also retrieve any deleteByQuery objects after it
    // add(client, seenLeader, sdoc("id","2001","_version_",++v));
    delQ(client, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "id:2001 OR id:2002");
    add(client, seenLeader, sdoc("id","2002","_version_",++v));
    del(client, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_",Long.toString(-++v)), "2000");

    assertSync(client1, numVersions, true, shardsArr[0]);

    validateDocs(docsAdded, client0, client1);

    //
    // Test that handling reorders work when applying docs retrieved from peer
    //

    // this should cause us to retrieve the delete (but not the following add)
    // the reorder in application shouldn't affect anything
    add(client0, seenLeader, sdoc("id","3000","_version_",3001));
    add(client1, seenLeader, sdoc("id","3000","_version_",3001));
    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","3000"),  "3000");
    docsAdded.add(3000);

    // this should cause us to retrieve an add tha was previously deleted
    add(client0, seenLeader, sdoc("id","3001","_version_",3003));
    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","3001"),  "3004");
    del(client1, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","3001"),  "3004");

    // this should cause us to retrieve an older add that was overwritten
    add(client0, seenLeader, sdoc("id","3002","_version_",3004));
    add(client0, seenLeader, sdoc("id","3002","_version_",3005));
    add(client1, seenLeader, sdoc("id","3002","_version_",3005));
    docsAdded.add(3001); // 3001 added
    docsAdded.add(3002); // 3002 added
    
    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);

    // now lets check fingerprinting causes appropriate fails
    v = 4000;
    add(client0, seenLeader, sdoc("id",Integer.toString((int)v),"_version_",v));
    docsAdded.add(4000);
    int toAdd = numVersions+10;
    for (int i=0; i<toAdd; i++) {
      add(client0, seenLeader, sdoc("id",Integer.toString((int)v+i+1),"_version_",v+i+1));
      add(client1, seenLeader, sdoc("id",Integer.toString((int)v+i+1),"_version_",v+i+1));
      docsAdded.add((int)v+i+1);
    }

    // client0 now has an additional add beyond our window and the fingerprint should cause this to fail
    assertSync(client1, numVersions, false, shardsArr[0]);

    // if we turn of fingerprinting, it should succeed
    System.setProperty("solr.disableFingerprint", "true");
    try {
      assertSync(client1, numVersions, true, shardsArr[0]);
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }

    // lets add the missing document and verify that order doesn't matter
    add(client1, seenLeader, sdoc("id",Integer.toString((int)v),"_version_",v));
    assertSync(client1, numVersions, true, shardsArr[0]);

    // lets do some overwrites to ensure that repeated updates and maxDoc don't matter
    for (int i=0; i<10; i++) {
      add(client0, seenLeader, sdoc("id", Integer.toString((int) v + i + 1), "_version_", v + i + 1));
    }
    assertSync(client1, numVersions, true, shardsArr[0]);
    
    validateDocs(docsAdded, client0, client1);

    // lets add some in-place updates
    add(client0, seenLeader, sdoc("id", "5000", "val_i_dvo", 0, "title", "mytitle", "_version_", 5000)); // full update
    docsAdded.add(5000);
    assertSync(client1, numVersions, true, shardsArr[0]);
    // verify the in-place updated document (id=5000) has correct fields
    assertEquals(0, client1.getById("5000").get("val_i_dvo"));
    assertEquals(client0.getById("5000")+" and "+client1.getById("5000"), 
        "mytitle", client1.getById("5000").getFirstValue("title"));

    ModifiableSolrParams inPlaceParams = new ModifiableSolrParams(seenLeader);
    inPlaceParams.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, "5000");
    add(client0, inPlaceParams, sdoc("id", "5000", "val_i_dvo", 1, "_version_", 5001)); // in-place update
    assertSync(client1, numVersions, true, shardsArr[0]);
    // verify the in-place updated document (id=5000) has correct fields
    assertEquals(1, client1.getById("5000").get("val_i_dvo"));
    assertEquals(client0.getById("5000")+" and "+client1.getById("5000"), 
        "mytitle", client1.getById("5000").getFirstValue("title"));

    // interleave the in-place updates with a few deletes to other documents
    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","5002"),  4001);
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","5003"),  "id:4002");
    docsAdded.remove(4001);
    docsAdded.remove(4002);
    
    inPlaceParams.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, "5001");
    add(client0, inPlaceParams, sdoc("id", 5000, "val_i_dvo", 2, "_version_", 5004)); // in-place update
    assertSync(client1, numVersions, true, shardsArr[0]);
    // verify the in-place updated document (id=5000) has correct fields
    assertEquals(2, client1.getById("5000").get("val_i_dvo"));
    assertEquals(client0.getById("5000")+" and "+client1.getById("5000"), 
        "mytitle", client1.getById("5000").getFirstValue("title"));

    // a DBQ with value
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","5005"),  "val_i_dvo:1"); // current val is 2, so this should not delete anything
    assertSync(client1, numVersions, true, shardsArr[0]);



    add(client0, seenLeader, sdoc("id", "5000", "val_i_dvo", 0, "title", "mytitle", "_version_", 5000)); // full update
    docsAdded.add(5000);
    assertSync(client1, numVersions, true, shardsArr[0]);
    inPlaceParams.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, "5004");
    add(client0, inPlaceParams, sdoc("id", 5000, "val_i_dvo", 3, "_version_", 5006));
    assertSync(client1, numVersions, true, shardsArr[0]);

    // verify the in-place updated document (id=5000) has correct fields
    assertEquals(3, client1.getById("5000").get("val_i_dvo"));
    assertEquals(client0.getById("5000")+" and "+client1.getById("5000"),
        "mytitle", client1.getById("5000").getFirstValue("title"));

    validateDocs(docsAdded, client0, client1);

    del(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","5007"),  5000);
    docsAdded.remove(5000);
    assertSync(client1, numVersions, true, shardsArr[0]);

    validateDocs(docsAdded, client0, client1);


    // if doc with id=6000 is deleted, further in-place-updates should fail
    add(client0, seenLeader, sdoc("id", "6000", "val_i_dvo", 6, "title", "mytitle", "_version_", 6000)); // full update
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","6004"),  "val_i_dvo:6"); // current val is 6000, this will delete id=6000
    assertSync(client1, numVersions, true, shardsArr[0]);
    SolrException ex = expectThrows(SolrException.class, () -> {
      inPlaceParams.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, "6000");
      add(client0, inPlaceParams, sdoc("id", 6000, "val_i_dvo", 6003, "_version_", 5007));
    });
    assertEquals(ex.toString(), SolrException.ErrorCode.SERVER_ERROR.code, ex.code());
    assertThat(ex.getMessage(), containsString("Can't find document with id=6000"));


    // Reordered DBQ with Child-nodes (SOLR-10114)
    docsAdded.clear();

    // Reordered full delete should not delete child-docs
    add(client0, seenLeader, sdocWithChildren(7001, "7001", 2)); // add with later version
    docsAdded.add(7001);
    docsAdded.add(7001001);
    docsAdded.add(7001002);
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","7000"),  "id:*"); // reordered delete
    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);

    // Reordered DBQ should not affect update
    add(client0, seenLeader, sdocWithChildren(8000, "8000", 5)); // add with later version
    delQ(client0, params(DISTRIB_UPDATE_PARAM,FROM_LEADER,"_version_","8002"),  "id:8500"); // not found, arrives earlier
    add(client0, seenLeader, sdocWithChildren(8000, "8001", 2)); // update with two childs
    docsAdded.add(8000);
    docsAdded.add(8000001);
    docsAdded.add(8000002);
    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);

  }

  protected void testOverlap(Set<Integer> docsAdded, SolrClient client0, SolrClient client1, long v) throws IOException, SolrServerException {
    int toAdd = (int)(numVersions *.95);
    for (int i=0; i<toAdd; i++) {
      add(client0, seenLeader, sdoc("id",Integer.toString(i+11),"_version_",v+i+1));
      docsAdded.add(i+11);
    }

    // sync should fail since there's not enough overlap to give us confidence
    assertSync(client1, numVersions, false, shardsArr[0]);

    // add some of the docs that were missing... just enough to give enough overlap
    int toAdd2 = (int)(numVersions * .25);
    for (int i=0; i<toAdd2; i++) {
      add(client1, seenLeader, sdoc("id",Integer.toString(i+11),"_version_",v+i+1));
    }

    assertSync(client1, numVersions, true, shardsArr[0]);
    validateDocs(docsAdded, client0, client1);
  }

  protected void validateDocs(Set<Integer> docsAdded, SolrClient client0, SolrClient client1) throws SolrServerException, IOException {
    client0.commit();
    client1.commit();
    QueryResponse qacResponse;
    qacResponse = queryAndCompare(params("q", "*:*", "rows", "10000", "sort","_version_ desc"), client0, client1);
    validateQACResponse(docsAdded, qacResponse);
  }

  void assertSync(SolrClient client, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "sync", StrUtils.join(Arrays.asList(syncWith), ',')));
    @SuppressWarnings({"rawtypes"})
    NamedList rsp = client.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("sync"));
  }
  
  void validateQACResponse(Set<Integer> docsAdded, QueryResponse qacResponse) {
    Set<Integer> qacDocs = new LinkedHashSet<>();
    for (int i=0; i<qacResponse.getResults().size(); i++) {
      qacDocs.add(Integer.parseInt(qacResponse.getResults().get(i).getFieldValue("id").toString()));
    }
    assertEquals(docsAdded, qacDocs);
    assertEquals(docsAdded.size(), qacResponse.getResults().getNumFound());
  }

}
