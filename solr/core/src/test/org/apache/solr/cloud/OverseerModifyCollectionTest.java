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

package org.apache.solr.cloud;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverseerModifyCollectionTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Test
  public void testModifyColl() throws Exception {
    String collName = "modifyColl";
    String newConfName = "conf" + random().nextInt();
    String oldConfName = "conf1";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collName, oldConfName, 1, 2);
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());
      
      ConfigSetAdminRequest.Create createConfig = new ConfigSetAdminRequest.Create()
        .setBaseConfigSetName(oldConfName)
        .setConfigSetName(newConfName);
      
      ConfigSetAdminResponse configRsp = createConfig.process(client);
      
      assertEquals(0, configRsp.getStatus());
      
      ModifiableSolrParams p = new ModifiableSolrParams();
      p.add("collection", collName);
      p.add("action", "MODIFYCOLLECTION");
      p.add("collection.configName", newConfName);
      client.request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p));
    }
    
    assertEquals(newConfName, getConfigNameFromZk(collName));    
    
    //Try an invalid config name
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      ModifiableSolrParams p = new ModifiableSolrParams();
      p.add("collection", collName);
      p.add("action", "MODIFYCOLLECTION");
      p.add("collection.configName", "notARealConfigName");
      try{
        client.request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p));
        fail("Exception should be thrown");
      } catch(RemoteSolrException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("Can not find the specified config set"));
      }
    }

  }
  
  private String getConfigNameFromZk(String collName) throws KeeperException, InterruptedException {
    byte[] b = cloudClient.getZkStateReader().getZkClient().getData(ZkStateReader.getCollectionPathRoot(collName), null, null, false);
    Map confData = (Map) Utils.fromJSON(b);
    return (String) confData.get(ZkController.CONFIGNAME_PROP); 
  }

}
