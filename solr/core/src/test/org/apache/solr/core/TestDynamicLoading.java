package org.apache.solr.core;

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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SimplePostTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDynamicLoading extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestDynamicLoading.class);
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrServer client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrServer)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }



  @Override
  public void doTest() throws Exception {

   setupHarnesses();
   dynamicLoading();



  }

  private void dynamicLoading() throws Exception {
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/test1', 'class': 'org.apache.solr.core.BlobStoreTestRequestHandler' , 'lib':'test','version':'1'}\n" +
        "}";
    RestTestHarness client = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    TestSolrConfigHandler.runConfigCommand(client,"/config?wt=json",payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay?wt=json",
        null,
        Arrays.asList("overlay", "requestHandler", "/test1", "lib"),
        "test",10);

    Map map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);

    assertNotNull(map = (Map) map.get("error"));
    assertEquals(".system collection not available", map.get("msg"));

    HttpSolrServer server = (HttpSolrServer) clients.get(random().nextInt(clients.size()));
    String baseURL = server.getBaseURL();
    baseURL = baseURL.substring(0, baseURL.lastIndexOf('/'));
    TestBlobHandler.createSysColl(new HttpSolrServer(baseURL,server.getHttpClient()));
    map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);

    assertNotNull(map = (Map) map.get("error"));
    assertEquals("no such blob or version available: test/1", map.get("msg"));
    ByteBuffer jar = generateZip( TestDynamicLoading.class,BlobStoreTestRequestHandler.class);
    TestBlobHandler.postAndCheck(cloudClient, baseURL, jar,1);

    boolean success= false;
    for(int i=0;i<50;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);
      if(BlobStoreTestRequestHandler.class.getName().equals(map.get("class"))){
        success = true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue(new String( ZkStateReader.toJSON(map) , StandardCharsets.UTF_8), success );

  }


  public static ByteBuffer generateZip(Class... classes) throws IOException {
    ZipOutputStream zipOut = null;
    SimplePostTool.BAOS bos = new SimplePostTool.BAOS();
    zipOut = new ZipOutputStream(bos);
    zipOut.setLevel(ZipOutputStream.DEFLATED);
    for (Class c : classes) {
      String path = c.getName().replace('.', '/').concat(".class");
      ZipEntry entry = new ZipEntry(path);
      ByteBuffer b = SimplePostTool.inputStreamToByteArray(c.getClassLoader().getResourceAsStream(path));
      zipOut.putNextEntry(entry);
      zipOut.write(b.array(), 0, b.limit());
      zipOut.closeEntry();
    }
    zipOut.close();
    return bos.getByteBuffer();
  }



}
