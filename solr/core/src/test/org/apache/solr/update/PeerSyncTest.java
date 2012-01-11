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
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PeerSyncTest extends BaseDistributedSearchTestCase {


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
    SolrServer clients0 = clients.get(0);
    SolrServer clients1 = clients.get(1);
    SolrServer clients2 = clients.get(2);

    clients0.add(sdoc("id",1));

    QueryRequest qr = new QueryRequest(params("qt","/get", "getVersions","100", "sync",shardsArr[0]));
    NamedList rsp = clients1.request(qr);
    // System.out.println("RESPONSE="+rsp);
    assertTrue( (Boolean)rsp.get("sync") );
    clients1.commit();
    assertEquals(1, clients1.query(params("q","id:1")).getResults().getNumFound());

  }


}
