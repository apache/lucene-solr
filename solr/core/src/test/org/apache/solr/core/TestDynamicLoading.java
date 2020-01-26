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
package org.apache.solr.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SimplePostTool;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.solr.handler.TestSolrConfigHandlerCloud.compareValues;

public class TestDynamicLoading extends AbstractFullDistribZkTestBase {

  @BeforeClass
  public static void enableRuntimeLib() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  //17-Aug-2018 commented @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
  public void testDynamicLoading() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    setupRestTestHarnesses();

    String blobName = "colltest";
    boolean success = false;


    HttpSolrClient randomClient = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    String baseURL = randomClient.getBaseURL();
    baseURL = baseURL.substring(0, baseURL.lastIndexOf('/'));
    String payload = "{\n" +
        "'add-runtimelib' : { 'name' : 'colltest' ,'version':1}\n" +
        "}";
    RestTestHarness client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "version"),
        1l, 10);


    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/test1', 'class': 'org.apache.solr.core.BlobStoreTestRequestHandler' ,registerPath: '/solr,/v2',  'runtimeLib' : true }\n" +
        "}";

    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client,"/config",payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "requestHandler", "/test1", "class"),
        "org.apache.solr.core.BlobStoreTestRequestHandler",10);

    Map map = TestSolrConfigHandler.getRespMap("/test1", client);

    assertNotNull(map.toString(), map = (Map) map.get("error"));
    assertTrue(map.toString(), map.get("msg").toString().contains(".system collection not available"));


    TestBlobHandler.createSystemCollection(getHttpSolrClient(baseURL, randomClient.getHttpClient()));
    waitForRecoveriesToFinish(".system", true);

    map = TestSolrConfigHandler.getRespMap("/test1", client);


    assertNotNull(map = (Map) map.get("error"));
    assertTrue("full output " + map, map.get("msg").toString().contains("no such blob or version available: colltest/1" ));
    payload = " {\n" +
        "  'set' : {'watched': {" +
        "                    'x':'X val',\n" +
        "                    'y': 'Y val'}\n" +
        "             }\n" +
        "  }";

    TestSolrConfigHandler.runConfigCommand(client,"/config/params",payload);
    TestSolrConfigHandler.testForResponseElement(
        client,
        null,
        "/config/params",
        cloudClient,
        Arrays.asList("response", "params", "watched", "x"),
        "X val",
        10);




    for(int i=0;i<100;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1", client);
      if("X val".equals(map.get("x"))){
         success = true;
         break;
      }
      Thread.sleep(100);
    }
    ByteBuffer jar = null;

//     jar = persistZip("/tmp/runtimelibs.jar.bin", TestDynamicLoading.class, RuntimeLibReqHandler.class, RuntimeLibResponseWriter.class, RuntimeLibSearchComponent.class);
//    if(true) return;

    jar = getFileContent("runtimecode/runtimelibs.jar.bin");
    TestBlobHandler.postAndCheck(cloudClient, baseURL, blobName, jar, 1);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/runtime', 'class': 'org.apache.solr.core.RuntimeLibReqHandler' , 'runtimeLib':true }," +
        "'create-searchcomponent' : { 'name' : 'get', 'class': 'org.apache.solr.core.RuntimeLibSearchComponent' , 'runtimeLib':true }," +
        "'create-queryResponseWriter' : { 'name' : 'json1', 'class': 'org.apache.solr.core.RuntimeLibResponseWriter' , 'runtimeLib':true }" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);

    Map result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "requestHandler", "/runtime", "class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);
    compareValues(result, "org.apache.solr.core.RuntimeLibResponseWriter", asList("overlay", "queryResponseWriter", "json1", "class"));
    compareValues(result, "org.apache.solr.core.RuntimeLibSearchComponent", asList("overlay", "searchComponent", "get", "class"));

    result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/runtime",
        null,
        Arrays.asList("class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);
    compareValues(result, MemClassLoader.class.getName(), asList( "loader"));

    result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/runtime?wt=json1",
        null,
        Arrays.asList("wt"),
        "org.apache.solr.core.RuntimeLibResponseWriter", 10);
    compareValues(result, MemClassLoader.class.getName(), asList( "loader"));

    result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/get?abc=xyz",
        null,
        Arrays.asList("get"),
        "org.apache.solr.core.RuntimeLibSearchComponent", 10);
    compareValues(result, MemClassLoader.class.getName(), asList( "loader"));

    jar = getFileContent("runtimecode/runtimelibs_v2.jar.bin");
    TestBlobHandler.postAndCheck(cloudClient, baseURL, blobName, jar, 2);
    payload = "{\n" +
        "'update-runtimelib' : { 'name' : 'colltest' ,'version':2}\n" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "version"),
        2l, 10);

    result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/get?abc=xyz",
        null,
        Arrays.asList("Version"),
        "2", 10);


    payload = " {\n" +
        "  'set' : {'watched': {" +
        "                    'x':'X val',\n" +
        "                    'y': 'Y val'}\n" +
        "             }\n" +
        "  }";

    TestSolrConfigHandler.runConfigCommand(client,"/config/params",payload);
    TestSolrConfigHandler.testForResponseElement(
        client,
        null,
        "/config/params",
        cloudClient,
        Arrays.asList("response", "params", "watched", "x"),
        "X val",
        10);
   result = TestSolrConfigHandler.testForResponseElement(
        client,
        null,
        "/test1",
        cloudClient,
        Arrays.asList("x"),
        "X val",
        10);

    payload = " {\n" +
        "  'set' : {'watched': {" +
        "                    'x':'X val changed',\n" +
        "                    'y': 'Y val'}\n" +
        "             }\n" +
        "  }";

    TestSolrConfigHandler.runConfigCommand(client,"/config/params",payload);
    result = TestSolrConfigHandler.testForResponseElement(
        client,
        null,
        "/test1",
        cloudClient,
        Arrays.asList("x"),
        "X val changed",
        10);
  }

  public static ByteBuffer getFileContent(String f) throws IOException {
    return getFileContent(f, true);
  }
  /**
   * @param loadFromClassPath if true, it will look in the classpath to find the file,
   *        otherwise load from absolute filesystem path.
   */
  public static ByteBuffer getFileContent(String f, boolean loadFromClassPath) throws IOException {
    ByteBuffer jar;
    File file = loadFromClassPath ? getFile(f): new File(f);
    try (FileInputStream fis = new FileInputStream(file)) {
      byte[] buf = new byte[fis.available()];
      fis.read(buf);
      jar = ByteBuffer.wrap(buf);
    }
    return jar;
  }

  public static  ByteBuffer persistZip(String loc, Class... classes) throws IOException {
    ByteBuffer jar = generateZip(classes);
    try (FileOutputStream fos =  new FileOutputStream(loc)){
      fos.write(jar.array(), 0, jar.limit());
      fos.flush();
    }
    return jar;
  }


  public static ByteBuffer generateZip(Class... classes) throws IOException {
    SimplePostTool.BAOS bos = new SimplePostTool.BAOS();
    try (ZipOutputStream zipOut = new ZipOutputStream(bos)) {
      zipOut.setLevel(ZipOutputStream.DEFLATED);
      for (Class c : classes) {
        String path = c.getName().replace('.', '/').concat(".class");
        ZipEntry entry = new ZipEntry(path);
        ByteBuffer b = SimplePostTool.inputStreamToByteArray(c.getClassLoader().getResourceAsStream(path));
        zipOut.putNextEntry(entry);
        zipOut.write(b.array(), 0, b.limit());
        zipOut.closeEntry();
      }
    }
    return bos.getByteBuffer();
  }

}
