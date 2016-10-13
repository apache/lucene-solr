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
package org.apache.solr.security;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicHeader;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTest;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.security.BasicAuthIntegrationTest.NOT_NULL_PREDICATE;
import static org.apache.solr.security.BasicAuthIntegrationTest.STD_CONF;
import static org.apache.solr.security.BasicAuthIntegrationTest.verifySecurityStatus;

public class BasicAuthStandaloneTest extends AbstractSolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String CONF_DIR = "solr/configsets/configset-2/conf/";
  private static final String ROOT_DIR = "solr/";

  SecurityConfHandlerLocalForTest securityConfHandler;
  SolrInstance instance = null;
  JettySolrRunner jetty;
      
  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    instance = new SolrInstance("inst", null);
    instance.setUp();
    System.setProperty("solr.solr.home", instance.getHomeDir());    
    jetty = createJetty(instance);
    initCore("solrconfig.xml", "schema.xml", instance.getHomeDir());
    securityConfHandler = new SecurityConfHandlerLocalForTest(jetty.getCoreContainer());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    jetty.stop();
    super.tearDown();
  }

  @Test
  public void testBasicAuth() throws Exception {

    String authcPrefix = "/admin/authentication";

    HttpClient cl = null;
    HttpSolrClient httpSolrClient = null;
    try {
      cl = HttpClientUtil.createClient(null);
      String baseUrl = buildUrl(jetty.getLocalPort(), "/solr"); 
      httpSolrClient = getHttpSolrClient(baseUrl);
      
      verifySecurityStatus(cl, baseUrl + authcPrefix, "/errorMessages", null, 20);

      // Write security.json locally. Should cause security to be initialized
      securityConfHandler.persistConf(new SecurityConfHandler.SecurityConfig()
          .setData(Utils.fromJSONString(STD_CONF.replaceAll("'", "\""))));
      securityConfHandler.securityConfEdited();
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);

      String command = "{\n" +
          "'set-user': {'harry':'HarryIsCool'}\n" +
          "}";

      GenericSolrRequest genericReq = new GenericSolrRequest(SolrRequest.METHOD.POST, authcPrefix, new ModifiableSolrParams());
      genericReq.setContentStreams(Collections.singletonList(new ContentStreamBase.ByteArrayStream(command.getBytes(UTF_8), "")));

      HttpSolrClient finalHttpSolrClient = httpSolrClient;
      HttpSolrClient.RemoteSolrException exp = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
        finalHttpSolrClient.request(genericReq);
      });
      assertEquals(401, exp.code());

      command = "{\n" +
          "'set-user': {'harry':'HarryIsUberCool'}\n" +
          "}";

      HttpPost httpPost = new HttpPost(baseUrl + authcPrefix);
      setBasicAuthHeader(httpPost, "solr", "SolrRocks");
      httpPost.setEntity(new ByteArrayEntity(command.getBytes(UTF_8)));
      httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication.enabled", "true", 20);
      HttpResponse r = cl.execute(httpPost);
      int statusCode = r.getStatusLine().getStatusCode();
      Utils.consumeFully(r.getEntity());
      assertEquals("proper_cred sent, but access denied", 200, statusCode);

      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/credentials/harry", NOT_NULL_PREDICATE, 20);

      // Read file from SOLR_HOME and verify that it contains our new user
      assertTrue(new String(Utils.toJSON(securityConfHandler.getSecurityConfig(false).getData())).contains("harry"));
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
        httpSolrClient.close();
      }
    }
  }

  public static void setBasicAuthHeader(AbstractHttpMessage httpMsg, String user, String pwd) {
    String userPass = user + ":" + pwd;
    String encoded = Base64.byteArrayToBase64(userPass.getBytes(UTF_8));
    httpMsg.setHeader(new BasicHeader("Authorization", "Basic " + encoded));
    log.info("Added Basic Auth security Header {}",encoded );
  }

  public static Replica getRandomReplica(DocCollection coll, Random random) {
    ArrayList<Replica> l = new ArrayList<>();

    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        l.add(replica);
      }
    }
    Collections.shuffle(l, random);
    return l.isEmpty() ? null : l.get(0);
  }
  
  private JettySolrRunner createJetty(SolrInstance instance) throws Exception {
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, buildJettyConfig("/solr"));
    jetty.start();
    return jetty;
  }
  
  
  private class SolrInstance {
    String name;
    Integer port;
    File homeDir;
    File confDir;
    File dataDir;
    
    /**
     * if masterPort is null, this instance is a master -- otherwise this instance is a slave, and assumes the master is
     * on localhost at the specified port.
     */
    public SolrInstance(String name, Integer port) {
      this.name = name;
      this.port = port;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getSchemaFile() {
      return CONF_DIR + "schema.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return CONF_DIR + "solrconfig.xml";
    }

    public String getSolrXmlFile() {
      return ROOT_DIR + "solr.xml";
    }


    public void setUp() throws Exception {
      homeDir = createTempDir("inst").toFile();
      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      FileUtils.copyFile(getFile(getSolrXmlFile()), new File(homeDir, "solr.xml"));
      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(getFile(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");

      FileUtils.copyFile(getFile(getSchemaFile()), f);

      Files.createFile(homeDir.toPath().resolve("collection1/core.properties"));
    }

  }
}
