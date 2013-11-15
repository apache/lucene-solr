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

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.ModifiableSolrParams;

public class TestModifyConfFiles extends AbstractFullDistribZkTestBase {

  public TestModifyConfFiles() {
    super();
  }

  @Override
  public void doTest() throws Exception {
    int which = r.nextInt(clients.size());
    HttpSolrServer client = (HttpSolrServer) clients.get(which);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("op", "write");
    params.set("file", "schema.xml");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/file");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Input stream list was null for admin file write operation.");
    }

    params.remove("file");
    params.set("stream.body", "Testing rewrite of schema.xml file.");
    request = new QueryRequest(params);
    request.setPath("/admin/file");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "No file name specified for write operation.");
    }

    params.set("file", "bogus.txt");
    request = new QueryRequest(params);
    request.setPath("/admin/file");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Can not access: bogus.txt");
    }

    params.set("file", "schema.xml");
    request = new QueryRequest(params);
    request.setPath("/admin/file");

    client.request(request);

    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    String contents = new String(zkClient.getData("/configs/conf1/schema.xml", null, null, true), "UTF-8");

    //String schema = getFileContentFromZooKeeper("schema.xml");

    assertTrue("Schema contents should have changed!", "Testing rewrite of schema.xml file.".equals(contents));

    // Create a velocity/whatever node. Put a bit of data in it. See if you can change it.
    zkClient.makePath("/configs/conf1/velocity/test.vm", false, true);

    params.set("stream.body", "Some bogus stuff for a test.");
    params.set("file", "velocity/test.vm");
    request = new QueryRequest(params);
    request.setPath("/admin/file");

    client.request(request);

    contents = new String(zkClient.getData("/configs/conf1/velocity/test.vm", null, null, true), "UTF-8");
    assertTrue("Should have found new content in a velocity/test.vm.",
        contents.indexOf("Some bogus stuff for a test.") != -1);
  }

}
