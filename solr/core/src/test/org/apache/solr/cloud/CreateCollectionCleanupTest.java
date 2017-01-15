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

import java.util.Properties;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateCollectionCleanupTest extends SolrCloudTestCase {

  protected static final String CLOUD_SOLR_XML_WITH_10S_CREATE_COLL_WAIT = "<solr>\n" +
      "\n" +
      "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
      "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
      "\n" +
      "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
      "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
      "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
      "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
      "  </shardHandlerFactory>\n" +
      "\n" +
      "  <solrcloud>\n" +
      "    <str name=\"host\">127.0.0.1</str>\n" +
      "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
      "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">10000</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "    <int name=\"createCollectionWaitTimeTillActive\">${createCollectionWaitTimeTillActive:10}</int>\n" +
      "  </solrcloud>\n" +
      "  \n" +
      "</solr>\n";


  @BeforeClass
  public static void createCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(CLOUD_SOLR_XML_WITH_10S_CREATE_COLL_WAIT)
        .configure();
  }

  @Test
  public void testCreateCollectionCleanup() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    // Create a collection that would fail
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("foo","conf1",1,1);

    Properties properties = new Properties();
    properties.put(CoreAdminParams.DATA_DIR, "/some_invalid_dir/foo");
    create.setProperties(properties);
    CollectionAdminResponse rsp = create.process(cloudClient);
    assertFalse(rsp.isSuccess());

    // Confirm using LIST that the collection does not exist
    assertFalse(CollectionAdminRequest.listCollections(cloudClient).contains("foo"));

  }
}
