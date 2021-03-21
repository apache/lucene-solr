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

import javax.script.ScriptEngineManager;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Delete;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.ParWork;
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
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.security.BasicAuthIntegrationTest;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.*;
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

  @BeforeClass
  public static void beforTestConfigSetsAPI() throws Exception {
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
    System.setProperty("solr.enablePublicKeyHandler", "true");
    disableReuseOfCryptoKeys();
    useFactory(null);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    solrCluster = new MiniSolrCloudCluster(1, SolrTestUtil.createTempDir(), buildJettyConfig("/solr"));
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
    solrCluster.uploadConfigSet(SolrTestUtil.configset("configset-2"), "configSet");

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
    final File configDir = SolrTestUtil.getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = SolrTestUtil.createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    if (oldProps != null && oldProps.size() > 0) {
      FileUtils.write(new File(tmpConfigDir, ConfigSetProperties.DEFAULT_FILENAME),
          getConfigSetProps(oldProps), StandardCharsets.UTF_8);
    }
    ZkConfigManager configManager = new ZkConfigManager(zkClient());
    configManager.uploadConfigDir(tmpConfigDir.toPath(), baseConfigSetName);
  }

  private void verifyCreate(String baseConfigSetName, String configSetName,
      Map<String, String> oldProps, Map<String, String> newProps) throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    if (baseConfigSetName != null) {
      setupBaseConfigSet(baseConfigSetName, oldProps);
    }

    SolrZkClient zkClient = zkClient();

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

    solrClient.close();
  }

  private NamedList getConfigSetPropertiesFromZk(
      SolrZkClient zkClient, String path) throws Exception {
    byte [] oldPropsData = null;
    try {
      oldPropsData = zkClient.getData(path, null, null);
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
      assertTrue(properties == null || properties.size() == 0);
      return;
    }
    assertNotNull(properties);

    // check all oldProps are in props
    if (oldProps != null) {
      for (Map.Entry<String, String> entry : oldProps.entrySet()) {
        assertNotNull("Could not find " + entry.getKey() + " in " + properties, properties.get(entry.getKey()));
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
    ByteBuffer emptyData = ByteBuffer.allocate(0);

    // Checking error when no configuration name is specified in request
    Map map = postDataAndGetResponse(solrCluster.getSolrClient(), solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD", emptyData, null, null);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    assertEquals(400l, statusCode);

    SolrZkClient zkClient = zkClient();

    // Create dummy config files in zookeeper
    zkClient.mkdir("/configs/myconf");
    zkClient.create("/configs/myconf/firstDummyFile", "first dummy content".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    zkClient.create("/configs/myconf/anotherDummyFile", "second dummy content".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);

    // Checking error when configuration name specified already exists
    map = postDataAndGetResponse(solrCluster.getSolrClient(), solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD&name=myconf", emptyData, null, null);
    assertNotNull(map);
    statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    assertEquals(400l, statusCode);
    assertTrue("Expected file doesnt exist in zk. It's possibly overwritten", zkClient.exists("/configs/myconf/firstDummyFile"));
    assertTrue("Expected file doesnt exist in zk. It's possibly overwritten", zkClient.exists("/configs/myconf/anotherDummyFile"));
  }

  @Test
  public void testUploadDisabled() throws Exception {
    SolrZkClient zkClient = zkClient();
    for (boolean enabled : new boolean[]{true, false}) {
      System.setProperty("configset.upload.enabled", String.valueOf(enabled));
      try {
        long statusCode = uploadConfigSet("regular", "test-enabled-is-" + enabled, null, null, zkClient);
        assertEquals("ConfigSet upload enabling/disabling not working as expected for enabled=" + enabled + ".",
                enabled ? 0l : 400l, statusCode);
      } finally {
        System.clearProperty("configset.upload.enabled");
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
  @Ignore // MRM TODO: debug
  public void testUploadWithScriptUpdateProcessor() throws Exception {
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("JavaScript"));
    
      // Authorization off
      // unprotectConfigsHandler(); // TODO Enable this back when testUploadWithLibDirective() is re-enabled
      final String untrustedSuffix = "-untrusted";
      uploadConfigSetWithAssertions("with-script-processor", untrustedSuffix, null, null);
      // try to create a collection with the uploaded configset
      Throwable thrown = SolrTestCaseUtil.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
        createCollection("newcollection2", "with-script-processor" + untrustedSuffix, 1, 1, solrCluster.getSolrClient());
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

  @Test
  @Ignore // MRM-TEST TODO: flakey
  public void testUploadWithLibDirective() throws Exception {
    // Authorization off
    unprotectConfigsHandler();
    final String untrustedSuffix = "-untrusted";
    uploadConfigSetWithAssertions("with-lib-directive", untrustedSuffix, null, null);
    // try to create a collection with the uploaded configset
    Throwable thrown = SolrTestCaseUtil.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
      createCollection("newcollection3", "with-lib-directive" + untrustedSuffix, 1, 1, solrCluster.getSolrClient());
    });

    assertThat(thrown.getMessage(), containsString("Underlying core creation failed"));

    // MRM TODO: - work out using newcollection3 and dealing with it finding the collection after first create fails - instead of using newCollection4
    // Authorization on
    final String trustedSuffix = "-trusted";
    protectConfigsHandler();
    uploadConfigSetWithAssertions("with-lib-directive", trustedSuffix, "solr", "SolrRocks");
    // try to create a collection with the uploaded configset
    CollectionAdminResponse resp = createCollection("newcollection4", "with-lib-directive" + trustedSuffix, 1, 1, solrCluster.getSolrClient());

    SolrInputDocument doc = sdoc("id", "4055", "subject", "Solr");
    solrCluster.getSolrClient().add("newcollection4", doc);
    solrCluster.getSolrClient().commit("newcollection4");

    assertEquals("4055", solrCluster.getSolrClient().query("newcollection4", params("q", "*:*")).getResults().get(0).get("id"));
  }

  protected SolrZkClient zkClient() {
    ZkStateReader reader = solrCluster.getSolrClient().getZkStateReader();
    if (reader == null)
      solrCluster.getSolrClient().connect();
    return solrCluster.getSolrClient().getZkStateReader().getZkClient();
  }

  private void unprotectConfigsHandler() throws Exception {
    HttpClient cl = null;
    try {
      cl = HttpClientUtil.createClient(null);
      zkClient().setData("/security.json", "{}".getBytes(UTF_8), true);
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
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
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 100);
      BasicAuthIntegrationTest.verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/class", "solr.RuleBasedAuthorizationPlugin", 100);
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
  }

  private void uploadConfigSetWithAssertions(String configSetName, String suffix, String username, String password) throws Exception {
    SolrZkClient zkClient = zkClient();

    long statusCode = uploadConfigSet(configSetName, suffix, username, password, zkClient);
    assertEquals(0l, statusCode);

    assertTrue("managed-schema file should have been uploaded",
            zkClient.exists("/configs/" + configSetName + suffix + "/managed-schema"));
    assertTrue("managed-schema file contents on zookeeper are not exactly same as that of the file uploaded in config",
            Arrays.equals(zkClient.getData("/configs/" + configSetName + suffix + "/managed-schema", null, null),
                    readFile("solr/configsets/upload/" + configSetName + "/managed-schema")));

    assertTrue("solrconfig.xml file should have been uploaded",
            zkClient.exists("/configs/" + configSetName + suffix + "/solrconfig.xml"));
    byte data[] = zkClient.getData("/configs/" + configSetName + suffix, null, null);
    //assertEquals("{\"trusted\": false}", new String(data, StandardCharsets.UTF_8));
    assertTrue("solrconfig.xml file contents on zookeeper are not exactly same as that of the file uploaded in config",
            Arrays.equals(zkClient.getData("/configs/" + configSetName + suffix + "/solrconfig.xml", null, null),
                    readFile("solr/configsets/upload/" + configSetName + "/solrconfig.xml")));

  }

  private long uploadConfigSet(String configSetName, String suffix, String username, String password,
      SolrZkClient zkClient) throws IOException {
    // Read zipped sample config
    ByteBuffer sampleZippedConfig = TestSolrConfigHandler
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

    File directory = SolrTestUtil.getFile(directoryPath);
    if (log.isInfoEnabled()) {
      log.info("Directory: {}", directory.getAbsolutePath());
    }
    try {
      zip (directory, zipFile);
      if (log.isInfoEnabled()) {
        log.info("Zipfile: {}", zipFile.getAbsolutePath());
      }
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
  
  public static Map postDataAndGetResponse(CloudHttp2SolrClient cloudClient,
                                           String uri, ByteBuffer bytarr, String username, String password) throws IOException {

    log.info("postDataAndGetResponse {}", uri);
    Map m = null;
    try {

      Map<String, String> headers = new HashMap<>(1);
      if (username != null) {
        String userPass = username + ":" + password;
        String encoded = Base64.byteArrayToBase64(userPass.getBytes(UTF_8));

        headers.put("Authorization", "Basic " + encoded);
      }

      Http2SolrClient.SimpleResponse resp = Http2SolrClient.POST(uri, cloudClient.getHttpClient(), bytarr, "application/octet-stream", headers);
      try {

        m = (Map) Utils.fromJSONString(resp.asString);
      } catch (JSONParser.ParseException e) {
        System.err.println("err response: " + resp);
        throw new AssertionError(e);
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
    } catch (ExecutionException e) {
      log.error("", e);
    } catch (TimeoutException e) {
      log.error("", e);
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
    try (FileInputStream fis = new FileInputStream(SolrTestUtil.getFile(fname))) {
      buf = new byte[fis.available()];
      fis.read(buf);
    }
    return buf;
  }
  
  @Test
  public void testDeleteErrors() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final File configDir = SolrTestUtil.getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = SolrTestUtil.createTempDir().toFile();
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
    Exception e = SolrTestCaseUtil.expectThrows(Exception.class, () -> solrClient.request(request));
    assertTrue("Expected exception message to contain: " + errorContains
        + " got: " + e.getMessage(), e.getMessage().contains(errorContains));
  }

  @Test
  public void testDelete() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final String configSet = "configSet";
    solrCluster.uploadConfigSet(SolrTestUtil.configset("configset-2"), configSet);

    SolrZkClient zkClient = zkClient();
    ZkConfigManager configManager = new ZkConfigManager(zkClient);
    assertTrue(configManager.configExists(configSet));

    Delete delete = new Delete();
    delete.setConfigSetName(configSet);
    ConfigSetAdminResponse response = delete.process(solrClient);
    assertNotNull(response.getResponse());
    assertFalse(configManager.configExists(configSet));


    solrClient.close();
  }

  @Test
  public void testList() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);

    // test empty
    ConfigSetAdminRequest.List list = new ConfigSetAdminRequest.List();
    ConfigSetAdminResponse.List response = list.process(solrClient);
    Collection<String> actualConfigSets = response.getConfigSets();
    assertEquals(1, actualConfigSets.size()); // _default configset suppressed

    // test multiple
    Set<String> configSets = new HashSet<String>();
    for (int i = 0; i < 5; ++i) {
      String configSet = "configSet" + i;
      solrCluster.uploadConfigSet(SolrTestUtil.configset("configset-2"), configSet);
      configSets.add(configSet);
    }
    response = list.process(solrClient);
    actualConfigSets = response.getConfigSets();
    assertEquals(configSets.size() + 1, actualConfigSets.size());
    assertTrue(actualConfigSets.containsAll(configSets));

    solrClient.close();
  }

  /**
   * A simple sanity check that the test-framework hueristic logic for setting 
   * {@link ExternalPaths#DEFAULT_CONFIGSET} is working as it should 
   * in the current test env, and finding the real directory which matches what {@link ZkController}
   * finds and uses to bootstrap ZK in cloud based tests.
   *
   * <p>
   * This assumes the {@link SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE} system property 
   * has not been externally set in the environment where this test is being run -- which should 
   * <b>never</b> be the case, since it would prevent the test-framework from using 
   * {@link ExternalPaths#DEFAULT_CONFIGSET}
   *
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   * @see ZkController#getDefaultConfigDirPath
   */
  @Test
  public void testUserAndTestDefaultConfigsetsAreSame() throws IOException {
    final File extPath = new File(ExternalPaths.DEFAULT_CONFIGSET);
    assertTrue("_default dir doesn't exist: " + ExternalPaths.DEFAULT_CONFIGSET, extPath.exists());
    assertTrue("_default dir isn't a dir: " + ExternalPaths.DEFAULT_CONFIGSET, extPath.isDirectory());
    
    final String zkBootStrap = ZkController.getDefaultConfigDirPath();
    assertEquals("extPath _default configset dir vs zk bootstrap path",
                 ExternalPaths.DEFAULT_CONFIGSET, zkBootStrap);
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
