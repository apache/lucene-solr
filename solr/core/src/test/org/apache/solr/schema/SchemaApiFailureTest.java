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

package org.apache.solr.schema;

import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class SchemaApiFailureTest extends SolrCloudTestCase {

  private static final String COLLECTION = "schema-api-failure";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).configure();
    CollectionAdminRequest.createCollection(COLLECTION, 2, 1) // _default configset
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));
  }

  @Test
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
  public void testAddTheSameFieldTwice() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    SchemaRequest.Update fieldAddition = new SchemaRequest.AddField
        (Utils.makeMap("name","myfield", "type","string"));
    SchemaResponse.UpdateResponse updateResponse = fieldAddition.process(client, COLLECTION);

    HttpSolrClient.RemoteExecutionException ex = expectThrows(HttpSolrClient.RemoteExecutionException.class,
        () -> fieldAddition.process(client, COLLECTION));

    assertTrue("expected error message 'Field 'myfield' already exists'.",Utils.getObjectByPath(ex.getMetaData(), false, "error/details[0]/errorMessages[0]").toString().contains("Field 'myfield' already exists.") );

  }


}
