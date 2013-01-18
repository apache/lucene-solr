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

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ShardRoutingCustomTest extends AbstractFullDistribZkTestBase {

  String collection = DEFAULT_COLLECTION;  // enable this to be configurable (more work needs to be done)

  @BeforeClass
  public static void beforeShardHashingTest() throws Exception {
    useFactory(null);
  }

  public ShardRoutingCustomTest() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 0;
  }

  @Override
  public void doTest() throws Exception {
    boolean testFinished = false;
    try {
      doCustomSharding();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayout();
      }
    }
  }

  private void doCustomSharding() throws Exception {
    printLayout();

    startCloudJetty(collection, "shardA");

    printLayout();
  }


}
