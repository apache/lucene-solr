package org.apache.solr.cloud;

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

import java.util.ArrayList;
import java.util.Properties;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.Test;

public class CreateCollectionCleanupTest extends AbstractFullDistribZkTestBase {

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


  @Test
  public void testCreateCollectionCleanup() throws Exception {
    // Create a collection that would fail
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();

    Properties properties = new Properties();
    properties.put(CoreAdminParams.DATA_DIR, "/some_invalid_dir/foo");

    CollectionAdminResponse rsp = create.setCollectionName("foo")
        .setConfigName("conf1")
        .setNumShards(1)
        .setReplicationFactor(1)
        .setProperties(properties)
        .process(cloudClient);
    assertFalse(rsp.isSuccess());

    // Confirm using LIST that the collection does not exist
    CollectionAdminRequest.List list = new CollectionAdminRequest.List();
    rsp = list.process(cloudClient);
    assertFalse(((ArrayList) rsp.getResponse().get("collections")).contains("foo"));
  }
}
