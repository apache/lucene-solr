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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

import java.io.File;

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
    request.setPath("/admin/fileedit");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Input stream list was null for admin file write operation.");
    }

    params.remove("file");
    params.set("stream.body", "Testing rewrite of schema.xml file.");
    params.set("op", "test");
    request = new QueryRequest(params);
    request.setPath("/admin/fileedit");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "No file name specified for write operation.");
    }

    params.set("op", "write");
    params.set("file", "bogus.txt");
    request = new QueryRequest(params);
    request.setPath("/admin/fileedit");
    try {
      client.request(request);
      fail("Should have caught exception");
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Can not access: bogus.txt");
    }

    try {
      params.set("file", "schema.xml");
      request = new QueryRequest(params);
      request.setPath("/admin/fileedit");
      client.request(request);
      fail("Should have caught exception since it's mal-formed XML");
    } catch (Exception e) {
      assertTrue("Should have a sax parser exception here!",
          e.getMessage().contains("Invalid XML file: org.xml.sax.SAXParseException"));
    }

    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    params.set("stream.body", FileUtils.readFileToString(new File(top, "schema-tiny.xml"), "UTF-8"));
    params.set("file", "schema.xml");
    request = new QueryRequest(params);
    request.setPath("/admin/fileedit");

    client.request(request);

    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    String contents = new String(zkClient.getData("/configs/conf1/schema.xml", null, null, true), "UTF-8");

    assertTrue("Schema contents should have changed!", contents.contains("<schema name=\"tiny\" version=\"1.1\">"));

    // Create a velocity/whatever node. Put a bit of data in it. See if you can change it.
    zkClient.makePath("/configs/conf1/velocity/test.vm", false, true);

    params.set("stream.body", "Some bogus stuff for a test.");
    params.set("file", "velocity/test.vm");
    request = new QueryRequest(params);
    request.setPath("/admin/fileedit");

    client.request(request);

    contents = new String(zkClient.getData("/configs/conf1/velocity/test.vm", null, null, true), "UTF-8");
    assertTrue("Should have found new content in a velocity/test.vm.",
        contents.indexOf("Some bogus stuff for a test.") != -1);

    params = new ModifiableSolrParams();
    request = new QueryRequest(params);
    request.setPath("/admin/file");
    NamedList<Object> res = client.request(request);

    NamedList files = (NamedList)res.get("files");
    assertNotNull("Should have gotten files back", files);
    SimpleOrderedMap schema = (SimpleOrderedMap)files.get("schema.xml");
    assertNotNull("Should have a schema returned", schema);
    assertNull("Schema.xml should not be a directory", schema.get("directory"));

    SimpleOrderedMap velocity = (SimpleOrderedMap)files.get("velocity");
    assertNotNull("Should have velocity dir returned", velocity);

    assertTrue("Velocity should be a directory", Boolean.parseBoolean(velocity.get("directory").toString()));
  }

}
