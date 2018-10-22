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

import java.util.Arrays;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.RestTestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by caomanhdat on 6/3/16.
 */
public class TestCustomStream extends AbstractFullDistribZkTestBase {

  @BeforeClass
  public static void enableRuntimeLib() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
  }

  @Test
  public void testDynamicLoadingCustomStream() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    setupRestTestHarnesses();

    String blobName = "colltest";

    HttpSolrClient randomClient = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    String baseURL = randomClient.getBaseURL();
    baseURL = baseURL.substring(0, baseURL.lastIndexOf('/'));

    TestBlobHandler.createSystemCollection(getHttpSolrClient(baseURL, randomClient.getHttpClient()));
    waitForRecoveriesToFinish(".system", true);

    String payload = "{\n" +
        "'create-expressible' : { 'name' : 'hello', 'class': 'org.apache.solr.core.HelloStream' }\n" +
        "}";

    RestTestHarness client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client,"/config",payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "expressible", "hello", "class"),
        "org.apache.solr.core.HelloStream",10);

    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/stream?expr=hello()",
        null,
        Arrays.asList("result-set", "docs[0]", "msg"),
        "Hello World!",10);
  }


}
