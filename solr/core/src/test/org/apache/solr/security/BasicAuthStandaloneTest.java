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

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.LogLevel;
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
  private Path ROOT_DIR = Paths.get(getSolrHome());
  private Path CONF_DIR = ROOT_DIR.resolve("configsets").resolve("configset-2").resolve("conf");

  SecurityConfHandlerLocalForTesting securityConfHandler;
  SolrInstance instance = null;
  JettySolrRunner jetty;
      
  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    instance = new SolrInstance("inst", null);
    instance.setUp();
    jetty = createJetty(instance);
    securityConfHandler = new SecurityConfHandlerLocalForTesting(jetty.getCoreContainer());
    HttpClientUtil.clearRequestInterceptors(); // Clear out any old Authorization headers
  }

  @Override
  @After
  public void tearDown() throws Exception {
    jetty.stop();
    super.tearDown();
  }

  @Test
  @LogLevel("org.apache.solr=DEBUG")
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
      assertTrue(new String(Utils.toJSON(securityConfHandler.getSecurityConfig(false).getData()), 
          Charset.forName("UTF-8")).contains("harry"));
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

  private JettySolrRunner createJetty(SolrInstance instance) throws Exception {
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir().toString());
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir().toString(), nodeProperties, buildJettyConfig("/solr"));
    jetty.start();
    return jetty;
  }
  
  
  private class SolrInstance {
    String name;
    Integer port;
    Path homeDir;
    Path confDir;
    Path dataDir;
    
    /**
     * if masterPort is null, this instance is a master -- otherwise this instance is a slave, and assumes the master is
     * on localhost at the specified port.
     */
    public SolrInstance(String name, Integer port) {
      this.name = name;
      this.port = port;
    }

    public Path getHomeDir() {
      return homeDir;
    }

    public Path getSchemaFile() {
      return CONF_DIR.resolve("schema.xml");
    }

    public Path getConfDir() {
      return confDir;
    }

    public Path getDataDir() {
      return dataDir;
    }

    public Path getSolrConfigFile() {
      return CONF_DIR.resolve("solrconfig.xml");
    }

    public Path getSolrXmlFile() {
      return ROOT_DIR.resolve("solr.xml");
    }


    public void setUp() throws Exception {
      homeDir = createTempDir(name).toAbsolutePath();
      dataDir = homeDir.resolve("collection1").resolve("data");
      confDir = homeDir.resolve("collection1").resolve("conf");

      Files.createDirectories(homeDir);
      Files.createDirectories(dataDir);
      Files.createDirectories(confDir);

      Files.copy(getSolrXmlFile(), homeDir.resolve("solr.xml"));
      Files.copy(getSolrConfigFile(), confDir.resolve("solrconfig.xml"));
      Files.copy(getSchemaFile(), confDir.resolve("schema.xml"));

      Files.createFile(homeDir.resolve("collection1").resolve("core.properties"));
    }

  }
}
