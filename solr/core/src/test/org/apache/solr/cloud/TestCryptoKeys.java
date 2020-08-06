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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.LinkedHashMapWriter;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.MemClassLoader;
import org.apache.solr.core.TestDynamicLoading;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.CryptoKeys;
import org.apache.solr.util.RestTestHarness;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.solr.handler.TestSolrConfigHandlerCloud.compareValues;

public class TestCryptoKeys extends AbstractFullDistribZkTestBase {

  public TestCryptoKeys() {
    super();
    sliceCount = 1;
  }

  @Test
  public void test() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    setupRestTestHarnesses();
    String pk1sig = "G8LEW7uJ1is81Aqqfl3Sld3qDtOxPuVFeTLJHFJWecgDvUkmJNFXmf7nkHOVlXnDWahp1vqZf0W02VHXg37lBw==";
    String pk2sig = "pCyBQycB/0YvLVZfKLDIIqG1tFwM/awqzkp2QNpO7R3ThTqmmrj11wEJFDRLkY79efuFuQPHt40EE7jrOKoj9jLNELsfEqvU3jw9sZKiDONY+rV9Bj9QPeW8Pgt+F9Y1";
    String wrongKeySig = "xTk2hTipfpb+J5s4x3YZGOXkmHWtnJz05Vvd8RTm/Q1fbQVszR7vMk6dQ1URxX08fcg4HvxOo8g9bG2TSMOGjg==";
    String result = null;
    CryptoKeys cryptoKeys = null;
    SolrZkClient zk = getCommonCloudSolrClient().getZkStateReader().getZkClient();
    cryptoKeys = new CryptoKeys(CloudUtil.getTrustedKeys(zk, "exe"));
    ByteBuffer samplefile = ByteBuffer.wrap(readFile("cryptokeys/samplefile.bin"));
    //there are no keys yet created in ZK

    result = cryptoKeys.verify( pk1sig,samplefile);
    assertNull(result);

    zk.makePath("/keys/exe", true);
    zk.create("/keys/exe/pubk1.der", readFile("cryptokeys/pubk1.der"), CreateMode.PERSISTENT, true);
    zk.create("/keys/exe/pubk2.der", readFile("cryptokeys/pubk2.der"), CreateMode.PERSISTENT, true);
    Map<String, byte[]> trustedKeys = CloudUtil.getTrustedKeys(zk, "exe");

    cryptoKeys = new CryptoKeys(trustedKeys);
    result = cryptoKeys.verify(pk2sig, samplefile);
    assertEquals("pubk2.der", result);


    result = cryptoKeys.verify(pk1sig, samplefile);
    assertEquals("pubk1.der", result);

    try {
      result = cryptoKeys.verify(wrongKeySig,samplefile);
      assertNull(result);
    } catch (Exception e) {
      //pass
    }
    try {
      result = cryptoKeys.verify( "SGVsbG8gV29ybGQhCg==", samplefile);
      assertNull(result);
    } catch (Exception e) {
      //pass
    }


    HttpSolrClient randomClient = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    String baseURL = randomClient.getBaseURL();
    baseURL = baseURL.substring(0, baseURL.lastIndexOf('/'));

    TestBlobHandler.createSystemCollection(getHttpSolrClient(baseURL, randomClient.getHttpClient()));
    waitForRecoveriesToFinish(".system", true);

    ByteBuffer jar = TestDynamicLoading.getFileContent("runtimecode/runtimelibs.jar.bin");
    String blobName = "signedjar";
    TestBlobHandler.postAndCheck(cloudClient, baseURL, blobName, jar, 1);

    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/runtime', 'class': 'org.apache.solr.core.RuntimeLibReqHandler' , 'runtimeLib':true }" +
        "}";
    RestTestHarness client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);

    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "requestHandler", "/runtime", "class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);


    payload = "{\n" +
        "'add-runtimelib' : { 'name' : 'signedjar' ,'version':1}\n" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "version"),
        1l, 10);

    @SuppressWarnings({"rawtypes"})
    LinkedHashMapWriter map = TestSolrConfigHandler.getRespMap("/runtime", client);
    String s = map._getStr( "error/msg",null);
    assertNotNull(map.toString(), s);
    assertTrue(map.toString(), s.contains("should be signed with one of the keys in ZK /keys/exe"));

    String wrongSig = "QKqHtd37QN02iMW9UEgvAO9g9qOOuG5vEBNkbUsN7noc2hhXKic/ABFIOYJA9PKw61mNX2EmNFXOcO3WClYdSw==";

    payload = "{\n" +
        "'update-runtimelib' : { 'name' : 'signedjar' ,'version':1, 'sig': 'QKqHtd37QN02iMW9UEgvAO9g9qOOuG5vEBNkbUsN7noc2hhXKic/ABFIOYJA9PKw61mNX2EmNFXOcO3WClYdSw=='}\n" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "sig"),
        wrongSig, 10);

    map = TestSolrConfigHandler.getRespMap("/runtime", client);
    s = (String) Utils.getObjectByPath(map, false, Arrays.asList("error", "msg"));
    assertNotNull(map.toString(), s);//No key matched signature for jar
    assertTrue(map.toString(), s.contains("No key matched signature for jar"));

    String rightSig = "YkTQgOtvcM/H/5EQdABGl3wjjrPhonAGlouIx59vppBy2cZEofX3qX1yZu5sPNRmJisNXEuhHN2149dxeUmk2Q==";

    payload = "{\n" +
        "'update-runtimelib' : { 'name' : 'signedjar' ,'version':1, 'sig': 'YkTQgOtvcM/H/5EQdABGl3wjjrPhonAGlouIx59vppBy2cZEofX3qX1yZu5sPNRmJisNXEuhHN2149dxeUmk2Q=='}\n" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "sig"),
        rightSig, 10);

    map = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/runtime",
        null,
        Arrays.asList("class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);
    compareValues(map, MemClassLoader.class.getName(), asList("loader"));

    rightSig = "VJPMTxDf8Km3IBj2B5HWkIOqeM/o+HHNobOYCNA3WjrEVfOMZbMMqS1Lo7uLUUp//RZwOGkOhrUhuPNY1z2CGEIKX2/m8VGH64L14d52oSvFiwhoTDDuuyjW1TFGu35D";
    payload = "{\n" +
        "'update-runtimelib' : { 'name' : 'signedjar' ,'version':1, 'sig': 'VJPMTxDf8Km3IBj2B5HWkIOqeM/o+HHNobOYCNA3WjrEVfOMZbMMqS1Lo7uLUUp//RZwOGkOhrUhuPNY1z2CGEIKX2/m8VGH64L14d52oSvFiwhoTDDuuyjW1TFGu35D'}\n" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", blobName, "sig"),
        rightSig, 10);

    map = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/runtime",
        null,
        Arrays.asList("class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);
    compareValues(map, MemClassLoader.class.getName(), asList("loader"));
  }


  private byte[] readFile(String fname) throws IOException {
    byte[] buf = null;
    try (FileInputStream fis = new FileInputStream(getFile(fname))) {
      buf = new byte[fis.available()];
      fis.read(buf);
    }
    return buf;
  }


}
