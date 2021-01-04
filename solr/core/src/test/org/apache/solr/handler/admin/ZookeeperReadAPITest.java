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

package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Map;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.StrUtils.split;
import static org.apache.solr.common.util.Utils.getObjectByPath;

public class ZookeeperReadAPITest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testZkread() throws Exception {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    String basezk = baseUrl.toString().replace("/solr", "/api") + "/cluster/zk/data";
    String basezkls = baseUrl.toString().replace("/solr", "/api") + "/cluster/zk/ls";

    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      Object o = Utils.executeGET(client.getHttpClient(),
          basezk + "/security.json",
          Utils.JSONCONSUMER);
      assertNotNull(o);
      o = Utils.executeGET(client.getHttpClient(),
          basezkls + "/configs",
          Utils.JSONCONSUMER);
      assertEquals("0", String.valueOf(getObjectByPath(o, true, split(":/configs:_default:dataLength", ':'))));
      assertEquals("0", String.valueOf(getObjectByPath(o, true, split(":/configs:conf:dataLength", ':'))));
      assertEquals("0", String.valueOf(getObjectByPath(o, true, split("/stat/version", '/'))));

      o = Utils.executeGET(client.getHttpClient(),
          basezk + "/configs",
          Utils.JSONCONSUMER);
      assertTrue(((Map)o).containsKey("/configs"));
      assertNull(((Map)o).get("/configs"));

      byte[] bytes = new byte[1024 * 5];
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) random().nextInt(128);
      }
      cluster.getZkClient().create("/configs/_default/testdata", bytes, CreateMode.PERSISTENT, true);
      Utils.executeGET(client.getHttpClient(),
          basezk + "/configs/_default/testdata",
          is -> {
            byte[] newBytes = new byte[bytes.length];
            is.read(newBytes);
            for (int i = 0; i < newBytes.length; i++) {
              assertEquals(bytes[i], newBytes[i]);
            }
            return null;
          });
    }
  }

}