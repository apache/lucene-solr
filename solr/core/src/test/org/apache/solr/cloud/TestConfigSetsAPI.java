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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Delete;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.TestDynamicLoading;
import org.apache.solr.security.BasicAuthIntegrationTest;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.ConfigSetProperties.DEFAULT_FILENAME;
import static org.junit.matchers.JUnitMatchers.containsString;

/**
 * Simple ConfigSets API tests on user errors and simple success cases.
 */
public class TestConfigSetsAPI extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private MiniSolrCloudCluster solrCluster;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrCluster = new MiniSolrCloudCluster(1, createTempDir(), buildJettyConfig("/solr"));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (null != solrCluster) {
      solrCluster.shutdown();
      solrCluster = null;
    }
    super.tearDown();
  }

  @Test
  public void testCreateErrors() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    solrCluster.uploadConfigSet(configset("configset-2"), "configSet");

    // no action
    CreateNoErrorChecking createNoAction = new CreateNoErrorChecking();
    createNoAction.setAction(null);
    verifyException(solrClient, createNoAction, "action");

    // no ConfigSet name
    CreateNoErrorChecking create = new CreateNoErrorChecking();
    verifyException(solrClient, create, NAME);

    // set ConfigSet
    create.setConfigSetName("configSetName");

    // ConfigSet already exists
    Create alreadyExists = new Create();
    alreadyExists.setConfigSetName("configSet").setBaseConfigSetName("baseConfigSet");
    verifyException(solrClient, alreadyExists, "ConfigSet already exists");

    // Base ConfigSet does not exist
    Create baseConfigNoExists = new Create();
    baseConfigNoExists.setConfigSetName("newConfigSet").setBaseConfigSetName("baseConfigSet");
    verifyException(solrClient, baseConfigNoExists, "Base ConfigSet does not exist");

    solrClient.close();
  }

  @Test
  public void testCreate() throws Exception {
    // no old, no new
    verifyCreate(null, "configSet1", null, null);

    // no old, new
    verifyCreate("baseConfigSet2", "configSet2",
        null, ImmutableMap.<String, String>of("immutable", "true", "key1", "value1"));

    // old, no new
    verifyCreate("baseConfigSet3", "configSet3",
        ImmutableMap.<String, String>of("immutable", "false", "key2", "value2"), null);

    // old, new
    verifyCreate("baseConfigSet4", "configSet4",
        ImmutableMap.<String, String>of("immutable", "true", "onlyOld", "onlyOldValue"),
        ImmutableMap.<String, String>of("immutable", "false", "onlyNew", "onlyNewValue"));
  }

  private void setupBaseConfigSet(String baseConfigSetName, Map<String, String> oldProps) throws Exception {
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    if (oldProps != null) {
      FileUtils.write(new File(tmpConfigDir, ConfigSetProperties.DEFAULT_FILENAME),
          getConfigSetProps(oldProps), StandardCharsets.UTF_8);
    }
    solrCluster.uploadConfigSet(tmpConfigDir.toPath(), baseConfigSetName);
  }

  private void verifyCreate(String baseConfigSetName, String configSetName,
      Map<String, String> oldProps, Map<String, String> newProps) throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    setupBaseConfigSet(baseConfigSetName, oldProps);

    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
    try {
      ZkConfigManager configManager = new ZkConfigManager(zkClient);
      assertFalse(configManager.configExists(configSetName));

      Create create = new Create();
      create.setBaseConfigSetName(baseConfigSetName).setConfigSetName(configSetName);
      if (newProps != null) {
        Properties p = new Properties();
        p.putAll(newProps);
        create.setNewConfigSetProperties(p);
      }
      ConfigSetAdminResponse response = create.process(solrClient);
      assertNotNull(response.getResponse());
      assertTrue(configManager.configExists(configSetName));

      verifyProperties(configSetName, oldProps, newProps, zkClient);
    } finally {
      zkClient.close();
    }
    solrClient.close();
  }

  private NamedList getConfigSetPropertiesFromZk(
      SolrZkClient zkClient, String path) throws Exception {
    byte [] oldPropsData = null;
    try {
      oldPropsData = zkClient.getData(path, null, null, true);
    } catch (KeeperException.NoNodeException e) {
      // okay, properties just don't exist
    }

    if (oldPropsData != null) {
      InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(oldPropsData), StandardCharsets.UTF_8);
      try {
        return ConfigSetProperties.readFromInputStream(reader);
      } finally {
        reader.close();
      }
    }
    return null;
  }

  private void verifyProperties(String configSetName, Map<String, String> oldProps,
       Map<String, String> newProps, SolrZkClient zkClient) throws Exception {
    NamedList properties = getConfigSetPropertiesFromZk(zkClient,
        ZkConfigManager.CONFIGS_ZKNODE + "/" + configSetName + "/" + DEFAULT_FILENAME);
    // let's check without merging the maps, since that's what the MessageHandler does
    // (since we'd probably repeat any bug in the MessageHandler here)
    if (oldProps == null && newProps == null) {
      assertNull(properties);
      return;
    }
    assertNotNull(properties);

    // check all oldProps are in props
    if (oldProps != null) {
      for (Map.Entry<String, String> entry : oldProps.entrySet()) {
        assertNotNull(properties.get(entry.getKey()));
      }
    }
    // check all newProps are in props
    if (newProps != null) {
      for (Map.Entry<String, String> entry : newProps.entrySet()) {
        assertNotNull(properties.get(entry.getKey()));
      }
    }

    // check the value in properties are correct
    Iterator<Map.Entry<String, Object>> it = properties.iterator();
    while (it.hasNext()) {
      Map.Entry<String, Object> entry = it.next();
      String newValue = newProps != null ? newProps.get(entry.getKey()) : null;
      String oldValue = oldProps != null ? oldProps.get(entry.getKey()) : null;
      if (newValue != null) {
        assertTrue(newValue.equals(entry.getValue()));
      } else if (oldValue != null) {
        assertTrue(oldValue.equals(entry.getValue()));
      } else {
        // not in either
        assert(false);
      }
    }
  }

  @Test
  public void testUploadErrors() throws Exception {
    final SolrClient solrClient = getHttpSolrClient(solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString());

    ByteBuffer emptyData = ByteBuffer.allocate(0);

    // Checking error when no configuration name is specified in request
    Map map = postDataAndGetResponse(solrCluster.getSolrClient(),
        solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString()
        + "/admin/configs?action=UPLOAD", emptyData, null, null);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false,
        Arrays.asList("responseHeader", "status"));
    assertEquals(400l, statusCode);

    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null);

    // Create dummy config files in zookeeper
    zkClient.makePath("/configs/myconf", true);
    zkClient.create("/configs/myconf/firstDummyFile",
        "first dummy content".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    zkClient.create("/configs/myconf/anotherDummyFile",
        "second dummy content".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);

    // Checking error when configuration name specified already exists
    map = postDataAndGetResponse(solrCluster.getSolrClient(),
        solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString()
        + "/admin/configs?action=UPLOAD&name=myconf", emptyData, null, null);
    assertNotNull(map);
    statusCode = (long) getObjectByPath(map, false,
        Arrays.asList("responseHeader", "status"));
    assertEquals(400l, statusCode);
    assertTrue("Expected file doesnt exist in zk. It's possibly overwritten",
        zkClient.exists("/configs/myconf/firstDummyFile", true));
    assertTrue("Expected file doesnt exist in zk. It's possibly overwritten",
        zkClient.exists("/configs/myconf/anotherDummyFile", true));

    zkClient.close();
    solrClient.close();
  }

  @Test
  public void testUploadDisabled() throws Exception {
    try (SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {

      for (boolean enabled: new boolean[] {true, false}) {
        System.setProperty("configset.upload.enabled", String.valueOf(enabled));
        try {
          long statusCode = uploadConfigSet("regular", "test-enabled-is-" + enabled, null, null, zkClient);
          assertEquals("ConfigSet upload enabling/disabling not working as expected for enabled=" + enabled + ".",
              enabled? 0l: 400l, statusCode);
        } finally {
          System.clearProperty("configset.upload.enabled");
        }
      }
    }
  }

  @Test
  public void testUpload() throws Exception {
    String suffix = "-untrusted";
    uploadConfigSetWithAssertions("regular", suffix, null, null);
    // try to create a collection with the uploaded configset
    createCollection("newcollection", "regular" + suffix, 1, 1, solrCluster.getSolrClient());
  }
  
  @Test
  public void testUploadWithScriptUpdateProcessor() throws Exception {
      // Authorization off
      final String untrustedSuffix = "-untrusted";
      uploadConfigSetWithAssertions("with-script-processor", untrustedSuffix, null, null);
      // try to create a collection with the uploaded configset
      Throwable thrown = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
        createCollection("newcollection2", "with-script-processor" + untrustedSuffix,
      1, 1, solrCluster.getSolrClient());
      });

    assertThat(thrown.getMessage(), containsString("Underlying core creation failed"));

    // Authorization on
    final String trustedSuffix = "-trusted";
    protectConfigsHandler();
    uploadConfigSetWithAssertions("with-script-processor", trustedSuffix, "solr", "SolrRocks");
    // try to create a collection with the uploaded configset
    CollectionAdminResponse resp = createCollection("newcollection2", "with-script-processor" + trustedSuffix,
    1, 1, solrCluster.getSolrClient());
    scriptRequest("newcollection2");

  }

  protected SolrZkClient zkClient() {
    ZkStateReader reader = solrCluster.getSolrClient().getZkStateReader();
    if (reader == null)
      solrCluster.getSolrClient().connect();
    return solrCluster.getSolrClient().getZkStateReader().getZkClient();
  }

  private void protectConfigsHandler() throws Exception {
    String authcPrefix = "/admin/authentication";
    String authzPrefix = "/admin/authorization";

    String securityJson = "{\n" +
        "  'authentication':{\n" +
        "    'class':'solr.BasicAuthPlugin',\n" +
        "    'blockUnknown': false,\n" +
        "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
        "  'authorization':{\n" +
        "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
        "    'user-role':{'solr':'admin'},\n" +
        "    'permissions':[{'name':'security-edit','role':'admin'}, {'name':'config-edit','role':'admin'}]}}";

    HttpClient cl = null;
    try {
      cl = HttpClientUtil.createClient(null);
      JettySolrRunner randomJetty = solrCluster.getRandomJetty(random());
      String baseUrl = randomJetty.getBaseUrl().toString();

      zkClient().setData("/security.json", securityJson.replaceAll("'", "\"").getBytes(UTF_8), true);
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 50);
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/class", "solr.RuleBasedAuthorizationPlugin", 50);
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
    Thread.sleep(5000); // TODO: Without a delay, the test fails. Some problem with Authc/Authz framework?
  }

  private void uploadConfigSetWithAssertions(String configSetName, String suffix, String username, String password) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null);
    try {
      long statusCode = uploadConfigSet(configSetName, suffix, username, password, zkClient);
      assertEquals(0l, statusCode);

      assertTrue("managed-schema file should have been uploaded",
          zkClient.exists("/configs/"+configSetName+suffix+"/managed-schema", true));
      assertTrue("managed-schema file contents on zookeeper are not exactly same as that of the file uploaded in config",
          Arrays.equals(zkClient.getData("/configs/"+configSetName+suffix+"/managed-schema", null, null, true),
              readFile("solr/configsets/upload/"+configSetName+"/managed-schema")));

      assertTrue("solrconfig.xml file should have been uploaded",
          zkClient.exists("/configs/"+configSetName+suffix+"/solrconfig.xml", true));
      byte data[] = zkClient.getData("/configs/"+configSetName+suffix, null, null, true);
      //assertEquals("{\"trusted\": false}", new String(data, StandardCharsets.UTF_8));
      assertTrue("solrconfig.xml file contents on zookeeper are not exactly same as that of the file uploaded in config",
          Arrays.equals(zkClient.getData("/configs/"+configSetName+suffix+"/solrconfig.xml", null, null, true),
              readFile("solr/configsets/upload/"+configSetName+"/solrconfig.xml")));
    } finally {
      zkClient.close();
    }
  }

  private long uploadConfigSet(String configSetName, String suffix, String username, String password,
      SolrZkClient zkClient) throws IOException {
    // Read zipped sample config
    ByteBuffer sampleZippedConfig = TestDynamicLoading
        .getFileContent(
            createTempZipFile("solr/configsets/upload/"+configSetName), false);

    ZkConfigManager configManager = new ZkConfigManager(zkClient);
    assertFalse(configManager.configExists(configSetName+suffix));

    Map map = postDataAndGetResponse(solrCluster.getSolrClient(),
        solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD&name="+configSetName+suffix,
        sampleZippedConfig, username, password);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    return statusCode;
  }
  
  /**
   * Create a zip file (in the temp directory) containing all the files within the specified directory
   * and return the path for the zip file.
   */
  private String createTempZipFile(String directoryPath) {
    File zipFile = new File(solrCluster.getBaseDir().toFile().getAbsolutePath() +
        File.separator + TestUtil.randomSimpleString(random(), 6, 8) + ".zip");

    File directory = TestDynamicLoading.getFile(directoryPath);
    log.info("Directory: "+directory.getAbsolutePath());
    try {
      zip (directory, zipFile);
      log.info("Zipfile: "+zipFile.getAbsolutePath());
      return zipFile.getAbsolutePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void zip(File directory, File zipfile) throws IOException {
    URI base = directory.toURI();
    Deque<File> queue = new LinkedList<File>();
    queue.push(directory);
    OutputStream out = new FileOutputStream(zipfile);
    ZipOutputStream zout = new ZipOutputStream(out);
    try {
      while (!queue.isEmpty()) {
        directory = queue.pop();
        for (File kid : directory.listFiles()) {
          String name = base.relativize(kid.toURI()).getPath();
          if (kid.isDirectory()) {
            queue.push(kid);
            name = name.endsWith("/") ? name : name + "/";
            zout.putNextEntry(new ZipEntry(name));
          } else {
            zout.putNextEntry(new ZipEntry(name));

            InputStream in = new FileInputStream(kid);
            try {
              byte[] buffer = new byte[1024];
              while (true) {
                int readCount = in.read(buffer);
                if (readCount < 0) {
                  break;
                }
                zout.write(buffer, 0, readCount);
              }
            } finally {
              in.close();
            }

            zout.closeEntry();
          }
        }
      }
    } finally {
      zout.close();
    }
  }
  
  public void scriptRequest(String collection) throws SolrServerException, IOException {
    SolrClient client = solrCluster.getSolrClient();
    SolrInputDocument doc = sdoc("id", "4055", "subject", "Solr");
    client.add(collection, doc);
    client.commit(collection);

    assertEquals("42", client.query(collection, params("q", "*:*")).getResults().get(0).get("script_added_i"));
  }

  protected CollectionAdminResponse createCollection(String collectionName, String confSetName, int numShards,
      int replicationFactor, SolrClient client)  throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("collection.configName", confSetName);
    params.set("name", collectionName);
    params.set("numShards", numShards);
    params.set("replicationFactor", replicationFactor);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse(client.request(request));
    return res;
  }
  
  public static Map postDataAndGetResponse(CloudSolrClient cloudClient,
      String uri, ByteBuffer bytarr, String username, String password) throws IOException {
    HttpPost httpPost = null;
    HttpEntity entity;
    String response = null;
    Map m = null;
    
    try {
      httpPost = new HttpPost(uri);
      
      if (username != null) {
        String userPass = username + ":" + password;
        String encoded = Base64.byteArrayToBase64(userPass.getBytes(UTF_8));
        BasicHeader header = new BasicHeader("Authorization", "Basic " + encoded);
        httpPost.setHeader(header);
      }

      httpPost.setHeader("Content-Type", "application/octet-stream");
      httpPost.setEntity(new ByteArrayEntity(bytarr.array(), bytarr
          .arrayOffset(), bytarr.limit()));
      entity = cloudClient.getLbClient().getHttpClient().execute(httpPost)
          .getEntity();
      try {
        response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        m = (Map) Utils.fromJSONString(response);
      } catch (JSONParser.ParseException e) {
        System.err.println("err response: " + response);
        throw new AssertionError(e);
      }
    } finally {
      httpPost.releaseConnection();
    }
    return m;
  }

  private static Object getObjectByPath(Map root, boolean onlyPrimitive, java.util.List<String> hierarchy) {
    Map obj = root;
    for (int i = 0; i < hierarchy.size(); i++) {
      String s = hierarchy.get(i);
      if (i < hierarchy.size() - 1) {
        if (!(obj.get(s) instanceof Map)) return null;
        obj = (Map) obj.get(s);
        if (obj == null) return null;
      } else {
        Object val = obj.get(s);
        if (onlyPrimitive && val instanceof Map) {
          return null;
        }
        return val;
      }
    }

    return false;
  }

  private byte[] readFile(String fname) throws IOException {
    byte[] buf = null;
    try (FileInputStream fis = new FileInputStream(getFile(fname))) {
      buf = new byte[fis.available()];
      fis.read(buf);
    }
    return buf;
  }
  
  @Test
  public void testDeleteErrors() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    // Ensure ConfigSet is immutable
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    FileUtils.write(new File(tmpConfigDir, "configsetprops.json"),
        getConfigSetProps(ImmutableMap.<String, String>of("immutable", "true")), StandardCharsets.UTF_8);
    solrCluster.uploadConfigSet(tmpConfigDir.toPath(), "configSet");

    // no ConfigSet name
    DeleteNoErrorChecking delete = new DeleteNoErrorChecking();
    verifyException(solrClient, delete, NAME);

    // ConfigSet doesn't exist
    delete.setConfigSetName("configSetBogus");
    verifyException(solrClient, delete, "ConfigSet does not exist");

    // ConfigSet is immutable
    delete.setConfigSetName("configSet");
    verifyException(solrClient, delete, "Requested delete of immutable ConfigSet");

    solrClient.close();
  }

  private void verifyException(SolrClient solrClient, ConfigSetAdminRequest request,
      String errorContains) throws Exception {
    Exception e = expectThrows(Exception.class, () -> solrClient.request(request));
    assertTrue("Expected exception message to contain: " + errorContains
        + " got: " + e.getMessage(), e.getMessage().contains(errorContains));
  }

  @Test
  public void testDelete() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final String configSet = "configSet";
    solrCluster.uploadConfigSet(configset("configset-2"), configSet);

    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
    try {
      ZkConfigManager configManager = new ZkConfigManager(zkClient);
      assertTrue(configManager.configExists(configSet));

      Delete delete = new Delete();
      delete.setConfigSetName(configSet);
      ConfigSetAdminResponse response = delete.process(solrClient);
      assertNotNull(response.getResponse());
      assertFalse(configManager.configExists(configSet));
    } finally {
      zkClient.close();
    }

    solrClient.close();
  }

  @Test
  public void testList() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);

    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
    try {
      // test empty
      ConfigSetAdminRequest.List list = new ConfigSetAdminRequest.List();
      ConfigSetAdminResponse.List response = list.process(solrClient);
      Collection<String> actualConfigSets = response.getConfigSets();
      assertEquals(1, actualConfigSets.size()); // only the _default configset

      // test multiple
      Set<String> configSets = new HashSet<String>();
      for (int i = 0; i < 5; ++i) {
        String configSet = "configSet" + i;
        solrCluster.uploadConfigSet(configset("configset-2"), configSet);
        configSets.add(configSet);
      }
      response = list.process(solrClient);
      actualConfigSets = response.getConfigSets();
      assertEquals(configSets.size() + 1, actualConfigSets.size());
      assertTrue(actualConfigSets.containsAll(configSets));
    } finally {
      zkClient.close();
    }

    solrClient.close();
  }
  
  @Test
  public void testUserAndTestDefaultConfigsetsAreSame() throws IOException {
    File testDefaultConf = configset("_default").toFile();
    log.info("Test _default path: " + testDefaultConf);
    
    File userDefaultConf = new File(ExternalPaths.DEFAULT_CONFIGSET);
    log.info("User _default path: " + userDefaultConf);
    
    compareDirectories(userDefaultConf, testDefaultConf);
  }

  private static void compareDirectories(File userDefault, File testDefault) throws IOException {
    assertTrue("Test _default doesn't exist: " + testDefault.getAbsolutePath(), testDefault.exists());
    assertTrue("Test _default not a directory: " + testDefault.getAbsolutePath(),testDefault.isDirectory());
    assertTrue("User _default doesn't exist: " + userDefault.getAbsolutePath(), userDefault.exists());
    assertTrue("User _default not a directory: " + userDefault.getAbsolutePath(),userDefault.isDirectory());

    Files.walkFileTree(userDefault.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        FileVisitResult result = super.preVisitDirectory(dir, attrs);
        Path relativePath = userDefault.toPath().relativize(dir);
        File testDefaultFile = testDefault.toPath().resolve(relativePath).toFile();
        String[] listOne = dir.toFile().list();
        String[] listTwo = testDefaultFile.list();
        Arrays.sort(listOne);
        Arrays.sort(listTwo);
        assertEquals("Mismatch in files", Arrays.toString(listOne), Arrays.toString(listTwo));
        return result;
      }
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        FileVisitResult result = super.visitFile(file, attrs);
        Path relativePath = userDefault.toPath().relativize(file);
        File testDefaultFile = testDefault.toPath().resolve(relativePath).toFile();
        String userDefaultContents = FileUtils.readFileToString(file.toFile(), "UTF-8");
        String testDefaultContents = FileUtils.readFileToString(testDefaultFile, "UTF-8");
        assertEquals(testDefaultFile+" contents doesn't match expected ("+file+")", userDefaultContents, testDefaultContents);                    
        return result;
      }
    });
  }
  
  private StringBuilder getConfigSetProps(Map<String, String> map) {
    return new StringBuilder(new String(Utils.toJSON(map), StandardCharsets.UTF_8));
  }

  public static class CreateNoErrorChecking extends ConfigSetAdminRequest.Create {
    public ConfigSetAdminRequest setAction(ConfigSetAction action) {
       return super.setAction(action);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      if (action != null) params.set(ConfigSetParams.ACTION, action.toString());
      if (configSetName != null) params.set(NAME, configSetName);
      if (baseConfigSetName != null) params.set("baseConfigSet", baseConfigSetName);
      return params;
    }
  }

  public static class DeleteNoErrorChecking extends ConfigSetAdminRequest.Delete {
    public ConfigSetAdminRequest setAction(ConfigSetAction action) {
       return super.setAction(action);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      if (action != null) params.set(ConfigSetParams.ACTION, action.toString());
      if (configSetName != null) params.set(NAME, configSetName);
      return params;
    }
  }
}
