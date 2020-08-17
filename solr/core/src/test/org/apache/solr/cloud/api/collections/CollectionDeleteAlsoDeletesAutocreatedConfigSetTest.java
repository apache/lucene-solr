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
package org.apache.solr.cloud.api.collections;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.OverseerCollectionConfigSetProcessor;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class CollectionDeleteAlsoDeletesAutocreatedConfigSetTest extends AbstractFullDistribZkTestBase {

  public CollectionDeleteAlsoDeletesAutocreatedConfigSetTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void test() throws Exception {
    String overseerNode = OverseerCollectionConfigSetProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());

    String collectionName = "CollectionDeleteAlsoDeletesAutocreatedConfigSetTest";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,1,1)
            .setCreateNodeSet(overseerNode);

    NamedList<Object> request = create.process(cloudClient).getResponse();

    if (request.get("success") != null) {
      // collection exists now
      assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

      String configName = cloudClient.getZkStateReader().readConfigName(collectionName);

      // config for this collection is '.AUTOCREATED', and exists globally
      assertTrue(configName.endsWith(".AUTOCREATED"));
      assertTrue(cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));

      @SuppressWarnings({"rawtypes"})
      CollectionAdminRequest delete = CollectionAdminRequest.deleteCollection(collectionName);
      cloudClient.request(delete);

      // collection has been deleted
      assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
      // ... and so has its autocreated config set
      assertFalse("The auto-created config set should have been deleted with its collection", cloudClient.getZkStateReader().getConfigManager().listConfigs().contains(configName));
    }
  }
}
