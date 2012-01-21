package org.apache.solr.update;

/**
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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PeerSyncTest extends BaseDistributedSearchTestCase {
  private static int numVersions = 100;  // number of versions to use when syncing
  private ModifiableSolrParams seenLeader = params("leader","true");
  
  public PeerSyncTest() {
    fixShardCount = true;
    shardCount = 3;
    stress = 0;

    // TODO: a better way to do this?
    configString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";
  }
  
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("maxScore", SKIPVAL);

    SolrServer client0 = clients.get(0);
    SolrServer client1 = clients.get(1);
    SolrServer client2 = clients.get(2);

    long v = 0;
    add(client0, seenLeader, sdoc("id","1","_version_",++v));

    // this fails because client0 has no context (i.e. no updates of it's own to judge if applying the updates
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

    assertSync(client1, numVersions, true, shardsArr[0]);

    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*"), client0, client1);

    int toAdd = (int)(numVersions *.95);
    for (int i=0; i<toAdd; i++) {
      add(client0, seenLeader, sdoc("id",Integer.toString(i+11),"_version_",v+i+1));
    }

    // sync should fail since there's not enough overlap to give us confidence
    assertSync(client1, numVersions, false, shardsArr[0]);

    // add some of the docs that were missing... just enough to give enough overlap
    int toAdd2 = (int)(numVersions * .25);
    for (int i=0; i<toAdd2; i++) {
      add(client1, seenLeader, sdoc("id",Integer.toString(i+11),"_version_",v+i+1));
    }

    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort","_version_ desc"), client0, client1);



    // test delete and deleteByQuery
    v=1000;
    add(client0, seenLeader, sdoc("id","1000","_version_",++v));
    add(client0, seenLeader, sdoc("id","1001","_version_",++v));
    delQ(client0, params("leader","true","_version_",Long.toString(-++v)), "id:1001 OR id:1002");
    add(client0, seenLeader, sdoc("id","1002","_version_",++v));
    del(client0, params("leader","true","_version_",Long.toString(-++v)), "1000");

    assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort","_version_ desc"), client0, client1);

    // test that delete by query is returned even if not requested, and that it doesn't delete newer stuff than it should
    v=2000;
    SolrServer client = client0;
    add(client, seenLeader, sdoc("id","2000","_version_",++v));
    add(client, seenLeader, sdoc("id","2001","_version_",++v));
    delQ(client, params("leader","true","_version_",Long.toString(-++v)), "id:2001 OR id:2002");
    add(client, seenLeader, sdoc("id","2002","_version_",++v));
    del(client, params("leader","true","_version_",Long.toString(-++v)), "2000");

    v=2000;
    client = client1;
    add(client, seenLeader, sdoc("id","2000","_version_",++v));
    ++v;  // pretend we missed the add of 2001.  peersync should retrieve it, but should also retrieve any deleteByQuery objects after it
    // add(client, seenLeader, sdoc("id","2001","_version_",++v));
    delQ(client, params("leader","true","_version_",Long.toString(-++v)), "id:2001 OR id:2002");
    add(client, seenLeader, sdoc("id","2002","_version_",++v));
    del(client, params("leader","true","_version_",Long.toString(-++v)), "2000");

    // assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit(); client1.commit(); queryAndCompare(params("q", "*:*", "sort","_version_ desc"), client0, client1);
  }


  void assertSync(SolrServer server, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "sync", StrUtils.join(Arrays.asList(syncWith), ',')));
    NamedList rsp = server.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("sync"));
  }

}
