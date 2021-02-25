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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestCloudManagedSchema extends AbstractFullDistribZkTestBase {

  public TestCloudManagedSchema() {
    super();
  }

  @BeforeClass
  public static void initSysProperties() {
    System.setProperty("managed.schema.mutable", "false");
    System.setProperty("enable.update.log", "true");
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-managed-schema.xml";
  }

  @Test
  public void test() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.STATUS.toString());
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/cores");
    int which = r.nextInt(clients.size());
    HttpSolrClient client = (HttpSolrClient)clients.get(which);
    String previousBaseURL = client.getBaseURL();
    // Strip /collection1 step from baseURL - requests fail otherwise
    client.setBaseURL(previousBaseURL.substring(0, previousBaseURL.lastIndexOf("/")));
    @SuppressWarnings({"rawtypes"})
    NamedList namedListResponse = client.request(request);
    client.setBaseURL(previousBaseURL); // Restore baseURL 
    @SuppressWarnings({"rawtypes"})
    NamedList status = (NamedList)namedListResponse.get("status");
    @SuppressWarnings({"rawtypes"})
    NamedList collectionStatus = (NamedList)status.getVal(0);
    String collectionSchema = (String)collectionStatus.get(CoreAdminParams.SCHEMA);
    // Make sure the upgrade to managed schema happened
    assertEquals("Schema resource name differs from expected name", "managed-schema", collectionSchema);

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 30000);
    try {
      // Make sure "DO NOT EDIT" is in the content of the managed schema
      String fileContent = getFileContentFromZooKeeper(zkClient, "/solr/configs/conf1/managed-schema");
      assertTrue("Managed schema is missing", fileContent.contains("DO NOT EDIT"));

      // Make sure the original non-managed schema is no longer in ZooKeeper
      assertFileNotInZooKeeper(zkClient, "/solr/configs/conf1", "schema.xml");

      // Make sure the renamed non-managed schema is present in ZooKeeper
      fileContent = getFileContentFromZooKeeper(zkClient, "/solr/configs/conf1/schema.xml.bak");
      assertTrue("schema file doesn't contain '<schema'", fileContent.contains("<schema"));
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }
  
  private String getFileContentFromZooKeeper(SolrZkClient zkClient, String fileName)
      throws IOException, SolrServerException, KeeperException, InterruptedException {

    return (new String(zkClient.getData(fileName, null, null, true), StandardCharsets.UTF_8));

  }
  protected final void assertFileNotInZooKeeper(SolrZkClient zkClient, String parent, String fileName) throws Exception {
    List<String> kids = zkClient.getChildren(parent, null, true);
    for (String kid : kids) {
      if (kid.equalsIgnoreCase(fileName)) {
        String rawContent = new String(zkClient.getData(fileName, null, null, true), StandardCharsets.UTF_8);
        fail("File '" + fileName + "' was unexpectedly found in ZooKeeper.  Content starts with '"
            + rawContent.substring(0, 100) + " [...]'");
      }
    }
  }
}
