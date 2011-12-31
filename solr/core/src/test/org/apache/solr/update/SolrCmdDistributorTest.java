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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.Url;
import org.apache.solr.update.SolrCmdDistributor.StdUrl;

public class SolrCmdDistributorTest extends BaseDistributedSearchTestCase {
  
  
  public SolrCmdDistributorTest() {
    fixShardCount = true;
    shardCount = 1;
    stress = 0;
  }
  

  public static String getSchemaFile() {
    return "schema.xml";
  }
  
  public static  String getSolrConfigFile() {
    // use this because it has /update and is minimal
    return "solrconfig-tlog.xml";
  }
  
  // TODO: for now we redefine this method so that it pulls from the above
  // we don't get helpful override behavior due to the method being static
  protected void createServers(int numShards) throws Exception {
    controlJetty = createJetty(testDir, testDir + "/control/data", null, getSolrConfigFile(), getSchemaFile());

    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(testDir,
          testDir + "/shard" + i + "/data", null, getSolrConfigFile(),
          getSchemaFile());
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      String shardStr = "localhost:" + j.getLocalPort() + context;
      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }
  
  @Override
  public void doTest() throws Exception {
    //del("*:*");
    
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor();
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    List<Url> urls = new ArrayList<Url>();

    urls.add(new StdUrl(((CommonsHttpSolrServer) controlClient).getBaseURL()));
    
    // add one doc to controlClient
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = getSolrDoc("id", 1);
    cmdDistrib.distribAdd(cmd, urls, params);
    
    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    cmdDistrib.distribCommit(ccmd, urls, params);
    cmdDistrib.finish(urls);
    Response response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    long numFound = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(1, numFound);
    
    CommonsHttpSolrServer client2 = (CommonsHttpSolrServer) clients.get(0);
    urls.add(new StdUrl(client2.getBaseURL()));
    
    // add another 3 docs to both control and client1
    
    cmd.solrDoc = getSolrDoc("id", 2);
    cmdDistrib.distribAdd(cmd, urls, params);
    
    AddUpdateCommand cmd2 = new AddUpdateCommand(null);
    cmd2.solrDoc = getSolrDoc("id", 3);

    cmdDistrib.distribAdd(cmd2, urls, params);
    
    AddUpdateCommand cmd3 = new AddUpdateCommand(null);
    cmd3.solrDoc = getSolrDoc("id", 4);
    
    cmdDistrib.distribAdd(cmd3, Collections.singletonList(urls.get(0)), params);
    
    cmdDistrib.distribCommit(ccmd, urls, params);
    cmdDistrib.finish(urls);
    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    SolrDocumentList results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 4, numFound);
    
    numFound = client2.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(3, numFound);
    
    // now delete doc 2 which is on both control and client1
    
    DeleteUpdateCommand dcmd = new DeleteUpdateCommand(null);
    dcmd.id = "2";
    
    cmdDistrib.distribDelete(dcmd, urls, params);
    
    cmdDistrib.distribCommit(ccmd, urls, params);
    cmdDistrib.finish(urls);
    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 3, numFound);
    
    numFound = client2.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(results.toString(), 2, numFound);
  }
  
  protected void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }
  
  private SolrInputDocument getSolrDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
}
