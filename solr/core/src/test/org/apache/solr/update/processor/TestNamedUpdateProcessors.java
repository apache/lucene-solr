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
package org.apache.solr.update.processor;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.UnaryOperator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.TestDynamicLoading;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SimplePostTool;
import org.junit.Test;

public class TestNamedUpdateProcessors extends AbstractFullDistribZkTestBase {

  @Test
  //17-Aug-2018 commented @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
  public void test() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    setupRestTestHarnesses();

    String blobName = "colltest";

    HttpSolrClient randomClient = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    String baseURL = randomClient.getBaseURL();

    final String solrClientUrl = baseURL.substring(0, baseURL.lastIndexOf('/'));
    TestBlobHandler.createSystemCollection(getHttpSolrClient(solrClientUrl, randomClient.getHttpClient()));
    waitForRecoveriesToFinish(".system", true);

    TestBlobHandler.postAndCheck(cloudClient, baseURL.substring(0, baseURL.lastIndexOf('/')), blobName, TestDynamicLoading.generateZip(RuntimeUrp.class), 1);

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
        "'create-updateprocessor' : { 'name' : 'firstFld', 'class': 'solr.FirstFieldValueUpdateProcessorFactory', 'fieldName':'test_s'}, \n" +
        "'create-updateprocessor' : { 'name' : 'test', 'class': 'org.apache.solr.update.processor.RuntimeUrp', 'runtimeLib':true }, \n" +
        "'create-updateprocessor' : { 'name' : 'maxFld', 'class': 'solr.MaxFieldValueUpdateProcessorFactory', 'fieldName':'mul_s'} \n" +
        "}";

    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    forAllRestTestHarnesses( new UnaryOperator<RestTestHarness>() {
      @Override
      public RestTestHarness apply(RestTestHarness restTestHarness) {
        try {
          TestSolrConfigHandler.testForResponseElement(restTestHarness,
              null,
              "/config/overlay",
              null,
              Arrays.asList("overlay", "updateProcessor", "firstFld", "fieldName"),
              "test_s", 10);
        } catch (Exception ex) {
          fail("Caught exception: "+ex);
        }
        return restTestHarness;
      }
    });

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "123");
    doc.addField("test_s", Arrays.asList("one", "two"));
    doc.addField("mul_s", Arrays.asList("aaa", "bbb"));
    randomClient.add(doc);
    randomClient.commit(true, true);
    QueryResponse result = randomClient.query(new SolrQuery("id:123"));
    assertEquals(2, ((Collection) result.getResults().get(0).getFieldValues("test_s")).size());
    assertEquals(2, ((Collection) result.getResults().get(0).getFieldValues("mul_s")).size());
    doc = new SolrInputDocument();
    doc.addField("id", "456");
    doc.addField("test_s", Arrays.asList("three", "four"));
    doc.addField("mul_s", Arrays.asList("aaa", "bbb"));
    UpdateRequest ur = new UpdateRequest();
    ur.add(doc).setParam("processor", "firstFld,maxFld,test");
    randomClient.request(ur);
    randomClient.commit(true, true);
    result = randomClient.query(new SolrQuery("id:456"));
    SolrDocument d = result.getResults().get(0);
    assertEquals(1, d.getFieldValues("test_s").size());
    assertEquals(1, d.getFieldValues("mul_s").size());
    assertEquals("three", d.getFieldValues("test_s").iterator().next());
    assertEquals("bbb", d.getFieldValues("mul_s").iterator().next());
    String processors = (String) d.getFirstValue("processors_s");
    assertNotNull(processors);
    assertEquals(StrUtils.splitSmart(processors, '>'),
        Arrays.asList("FirstFieldValueUpdateProcessorFactory", "MaxFieldValueUpdateProcessorFactory", "RuntimeUrp", "LogUpdateProcessorFactory", "DistributedUpdateProcessorFactory", "RunUpdateProcessorFactory"));


  }

  public static ByteBuffer getFileContent(String f) throws IOException {
    ByteBuffer jar;
    try (FileInputStream fis = new FileInputStream(getFile(f))) {
      byte[] buf = new byte[fis.available()];
      fis.read(buf);
      jar = ByteBuffer.wrap(buf);
    }
    return jar;
  }

  public static ByteBuffer persistZip(String loc, Class... classes) throws IOException {
    ByteBuffer jar = generateZip(classes);
    try (FileOutputStream fos = new FileOutputStream(loc)) {
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
