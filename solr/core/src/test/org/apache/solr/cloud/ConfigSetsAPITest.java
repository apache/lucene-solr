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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.SolrException;
import org.junit.Test;

@LuceneTestCase.Slow
public class ConfigSetsAPITest extends AbstractFullDistribZkTestBase {
  public ConfigSetsAPITest() {
    super();
    sliceCount = 1;
  }

  @Test
  public void testConfigSetDeleteWhenInUse() throws Exception {
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setConfigName("conf1");
    create.setCollectionName("test_configset_delete");
    create.setNumShards(1);
    create.process(cloudClient);
    waitForCollection(cloudClient.getZkStateReader(), "test_configset_delete", 1);

    ConfigSetAdminRequest.Delete deleteConfigRequest = new ConfigSetAdminRequest.Delete();
    deleteConfigRequest.setConfigSetName("conf1");
    try {
      deleteConfigRequest.process(cloudClient);
      fail("The config deletion should cause an exception as it's currently being used by a collection.");
    } catch (SolrException e) {
      // Do nothing
    }

    // Clean up the collection
    CollectionAdminRequest.Delete deleteCollectionRequest = new CollectionAdminRequest.Delete();
    deleteCollectionRequest.setCollectionName("test_configset_delete");
    deleteCollectionRequest.process(cloudClient);
  }

}
