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

/**
 assertSync(client1, numVersions, true, shardsArr[0]);
    client0.commit();
    client1.commit();
    queryAndCompare(params("q", "*:*"), client0, client1);

    for (int i=0; i<numVersions; i++) {

    }


 *  client0.add(addRandFields(sdoc("id",1)));

 client0.add(addRandFields(sdoc("id",1)));
 client0.add(addRandFields(sdoc("id", 1)));
 client0.add(addRandFields(sdoc("id", 2)));
 **/


  }


  void assertSync(SolrServer server, int numVersions, boolean expectedResult, String... syncWith) throws IOException, SolrServerException {
    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions",Integer.toString(numVersions), "sync", StrUtils.join(Arrays.asList(syncWith), ',')));
    NamedList rsp = server.request(qr);
    assertEquals(expectedResult, (Boolean) rsp.get("sync"));
  }

}
