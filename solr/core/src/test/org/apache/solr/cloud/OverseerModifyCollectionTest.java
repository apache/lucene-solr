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

import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;

public class OverseerModifyCollectionTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf1", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testModifyColl() throws Exception {

    final String collName = "modifyColl";

    CollectionAdminRequest.createCollection(collName, "conf1", 1, 2)
        .process(cluster.getSolrClient());

    // TODO create a modifyCollection() method on CollectionAdminRequest
    ModifiableSolrParams p1 = new ModifiableSolrParams();
    p1.add("collection", collName);
    p1.add("action", "MODIFYCOLLECTION");
    p1.add("collection.configName", "conf2");
    cluster.getSolrClient().request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p1));

    assertEquals("conf2", getConfigNameFromZk(collName));
    
    //Try an invalid config name
    ModifiableSolrParams p2 = new ModifiableSolrParams();
    p2.add("collection", collName);
    p2.add("action", "MODIFYCOLLECTION");
    p2.add("collection.configName", "notARealConfigName");
    Exception e = expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p2));
    });

    assertTrue(e.getMessage(), e.getMessage().contains("Can not find the specified config set"));

  }
  
  private String getConfigNameFromZk(String collName) throws KeeperException, InterruptedException {
    byte[] b = zkClient().getData(ZkStateReader.getCollectionPathRoot(collName), null, null, false);
    Map confData = (Map) Utils.fromJSON(b);
    return (String) confData.get(ZkController.CONFIGNAME_PROP); 
  }

}
