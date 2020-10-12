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
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
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
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Delete;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.apache.zookeeper.data.Stat;
import org.junit.Assume;
import org.junit.BeforeClass;
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
public class TestConfigSetsAPI extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static ZkConfigManager zkConfigManager;

  @BeforeClass
  public static void setUpClass() throws Exception {
    configureCluster(1)
            .withSecurityJson(getSecurityJson())
            .configure();
    zkConfigManager = new ZkConfigManager(cluster.getZkClient());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    zkConfigManager = null;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.deleteAllCollections();
    cluster.deleteAllConfigSets();
    super.tearDown();
  }

  @Test
  public void testCreateErrors() throws Exception {
    final String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    try (final SolrClient solrClient = getHttpSolrClient(baseUrl)) {
      zkConfigManager.uploadConfigDir(configset("configset-2"), "configSet");

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
    }
  }

  @Test
  public void testCreate() throws Exception {
    // no old, no new
    verifyCreate(null, "configSet1", null, null, "solr");

    // no old, new
    verifyCreate("baseConfigSet2", "configSet2",
        null, ImmutableMap.<String, String>of("immutable", "true", "key1", "value1"), "solr");

    // old, no new
    verifyCreate("baseConfigSet3", "configSet3",
        ImmutableMap.<String, String>of("immutable", "false", "key2", "value2"), null, "solr");

    // old, new
    verifyCreate("baseConfigSet4", "configSet4",
        ImmutableMap.<String, String>of("immutable", "true", "onlyOld", "onlyOldValue"),
        ImmutableMap.<String, String>of("immutable", "false", "onlyNew", "onlyNewValue"), "solr");
  }

  @Test
  public void testCreateWithTrust() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testCreateWithTrust";
    String configsetSuffix2 = "testCreateWithTrust2";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, "solr");
    uploadConfigSetWithAssertions(configsetName, configsetSuffix2, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix2));
      try {
        ignoreException("unauthenticated request");
        // trusted -> unstrusted
        createConfigSet(configsetName + configsetSuffix, "foo", Collections.emptyMap(), cluster.getSolrClient(), null);
        fail("Expecting exception");
      } catch (SolrException e) {
        assertEquals(SolrException.ErrorCode.UNAUTHORIZED.code, e.code());
        unIgnoreException("unauthenticated request");
      }
      // trusted -> trusted
      verifyCreate(configsetName + configsetSuffix, "foo2", Collections.emptyMap(), Collections.emptyMap(), "solr");
      assertTrue(isTrusted(zkClient, "foo2", ""));

      // unstrusted -> unstrusted
      verifyCreate(configsetName + configsetSuffix2, "bar", Collections.emptyMap(), Collections.emptyMap(), null);
      assertFalse(isTrusted(zkClient, "bar", ""));

      // unstrusted -> trusted
      verifyCreate(configsetName + configsetSuffix2, "bar2", Collections.emptyMap(), Collections.emptyMap(), "solr");
      assertFalse(isTrusted(zkClient, "bar2", ""));
    }
  }

  private void setupBaseConfigSet(String baseConfigSetName, Map<String, String> oldProps) throws Exception {
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    if (oldProps != null) {
      FileUtils.write(new File(tmpConfigDir, ConfigSetProperties.DEFAULT_FILENAME),
          getConfigSetProps(oldProps), UTF_8);
    }
    zkConfigManager.uploadConfigDir(tmpConfigDir.toPath(), baseConfigSetName);
  }

  private void verifyCreate(String baseConfigSetName, String configSetName,
      Map<String, String> oldProps, Map<String, String> newProps, String username) throws Exception {
    final String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    try (final SolrClient solrClient = getHttpSolrClient(baseUrl)) {
      setupBaseConfigSet(baseConfigSetName, oldProps);

      SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
              AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
      try {
        ZkConfigManager configManager = new ZkConfigManager(zkClient);
        assertFalse(configManager.configExists(configSetName));

        ConfigSetAdminResponse response = createConfigSet(baseConfigSetName, configSetName, newProps, solrClient, username);
        assertNotNull(response.getResponse());
        assertTrue(configManager.configExists(configSetName));

        verifyProperties(configSetName, oldProps, newProps, zkClient);
      } finally {
        zkClient.close();
      }
    }
  }

  private ConfigSetAdminResponse createConfigSet(String baseConfigSetName, String configSetName, Map<String, String> newProps, SolrClient solrClient, String username) throws SolrServerException, IOException {
    Create create = new Create();
    create.setBaseConfigSetName(baseConfigSetName).setConfigSetName(configSetName);
    if (newProps != null) {
      Properties p = new Properties();
      p.putAll(newProps);
      create.setNewConfigSetProperties(p);
    }
    if (username != null) {
      create.addHeader("user", username);
    }
    return create.process(solrClient);
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList getConfigSetPropertiesFromZk(
      SolrZkClient zkClient, String path) throws Exception {
    byte [] oldPropsData = null;
    try {
      oldPropsData = zkClient.getData(path, null, null, true);
    } catch (KeeperException.NoNodeException e) {
      // okay, properties just don't exist
    }

    if (oldPropsData != null) {
      InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(oldPropsData), UTF_8);
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
    @SuppressWarnings({"rawtypes"})
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
    @SuppressWarnings({"unchecked"})
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
    final SolrClient solrClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());

    ByteBuffer emptyData = ByteBuffer.allocate(0);

    ignoreException("The configuration name should be provided");
    // Checking error when no configuration name is specified in request
    @SuppressWarnings({"rawtypes"})
    Map map = postDataAndGetResponse(cluster.getSolrClient(),
        cluster.getJettySolrRunners().get(0).getBaseUrl().toString()
        + "/admin/configs?action=UPLOAD", emptyData, null);
    assertNotNull(map);
    unIgnoreException("The configuration name should be provided");
    long statusCode = (long) getObjectByPath(map, false,
        Arrays.asList("responseHeader", "status"));
    assertEquals(400l, statusCode);

    SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null);

    // Create dummy config files in zookeeper
    zkClient.makePath("/configs/myconf", true);
    zkClient.create("/configs/myconf/firstDummyFile",
        "first dummy content".getBytes(UTF_8), CreateMode.PERSISTENT, true);
    zkClient.create("/configs/myconf/anotherDummyFile",
        "second dummy content".getBytes(UTF_8), CreateMode.PERSISTENT, true);

    // Checking error when configuration name specified already exists
    ignoreException("already exists");
    map = postDataAndGetResponse(cluster.getSolrClient(),
        cluster.getJettySolrRunners().get(0).getBaseUrl().toString()
        + "/admin/configs?action=UPLOAD&name=myconf", emptyData, null);
    assertNotNull(map);
    unIgnoreException("already exists`");
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
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {

      ignoreException("Configset upload feature is disabled");
      for (boolean enabled: new boolean[] {true, false}) {
        System.setProperty("configset.upload.enabled", String.valueOf(enabled));
        try {
          long statusCode = uploadConfigSet("regular", "test-enabled-is-" + enabled, null, zkClient);
          assertEquals("ConfigSet upload enabling/disabling not working as expected for enabled=" + enabled + ".",
              enabled? 0l: 400l, statusCode);
        } finally {
          System.clearProperty("configset.upload.enabled");
        }
      }
      unIgnoreException("Configset upload feature is disabled");
    }
  }

  @Test
  public void testOverwrite() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testOverwrite-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      int solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");
      ignoreException("The configuration regulartestOverwrite-1 already exists in zookeeper");
      assertEquals("Can't overwrite an existing configset unless the overwrite parameter is set",
              400, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, false, false));
      unIgnoreException("The configuration regulartestOverwrite-1 already exists in zookeeper");
      assertEquals("Expecting version to remain equal",
              solrconfigZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, false));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
    }

  }

  @Test
  public void testOverwriteWithCleanup() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testOverwriteWithCleanup-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      List<String> extraFiles = Arrays.asList(
              "/configs/regulartestOverwriteWithCleanup-1/foo1",
              "/configs/regulartestOverwriteWithCleanup-1/foo2",
              "/configs/regulartestOverwriteWithCleanup-1/foo2/1",
              "/configs/regulartestOverwriteWithCleanup-1/foo2/2");
      for (String f : extraFiles) {
        zkClient.makePath(f, true);
      }
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, false));
      for (String f : extraFiles) {
        assertTrue("Expecting file " + f + " to exist in ConfigSet but it's gone", zkClient.exists(f, true));
      }
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, true));
      for (String f : extraFiles) {
        assertFalse("Expecting file " + f + " to be deleted from ConfigSet but it wasn't", zkClient.exists(f, true));
      }
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
    }
  }

  @Test
  public void testOverwriteWithTrust() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testOverwriteWithTrust-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      int solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");
      // Was untrusted, overwrite with untrusted
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, false));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");

      // Was untrusted, overwrite with trusted but no cleanup
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, "solr", zkClient, true, false));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");

      // Was untrusted, overwrite with trusted with cleanup but fail on unzipping.
      // Should not set trusted=true
      assertEquals(500, uploadBadConfigSet(configsetName, configsetSuffix, "solr", zkClient, true, true));
      assertEquals("Expecting version bump",
              solrconfigZkVersion,  getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");

      // Was untrusted, overwrite with trusted with cleanup
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, "solr", zkClient, true, true));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");

      // Was trusted, try to overwrite with untrusted with no cleanup
      ignoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");
      assertEquals("Can't upload a trusted configset with an untrusted request",
              400, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, false));
      assertEquals("Expecting version to remain equal",
              solrconfigZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));

      // Was trusted, try to overwrite with untrusted with cleanup
      ignoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");
      assertEquals("Can't upload a trusted configset with an untrusted request",
              400, uploadConfigSet(configsetName, configsetSuffix, null, zkClient, true, true));
      assertEquals("Expecting version to remain equal",
              solrconfigZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      unIgnoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");

      // Was trusted, overwrite with trusted no cleanup
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, "solr", zkClient, true, false));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");

      // Was trusted, overwrite with trusted with cleanup
      assertEquals(0, uploadConfigSet(configsetName, configsetSuffix, "solr", zkClient, true, true));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
    }

  }

  @Test
  public void testSingleFileOverwrite() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testSinglePathOverwrite-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      int solrconfigZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml");
      ignoreException("The configuration regulartestOverwrite-1 already exists in zookeeper");
      assertEquals("Can't overwrite an existing configset unless the overwrite parameter is set",
              400, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "solrconfig.xml", false, false));
      unIgnoreException("The configuration regulartestOverwrite-1 already exists in zookeeper");
      assertEquals("Expecting version to remain equal",
              solrconfigZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "solrconfig.xml", true, false));
      assertTrue("Expecting version bump",
              solrconfigZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "solrconfig.xml"));
    }
  }

  @Test
  public void testNewSingleFile() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testSinglePathNew-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", false, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml"));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
    }
  }

  @Test
  public void testSingleWithCleanup() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testSinglePathCleanup-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      ignoreException("ConfigSet uploads do not allow cleanup=true when filePath is used.");
      assertEquals(400, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", false, true));
      assertFalse("New file should not exist, since the trust check did not succeed.", zkClient.exists("/configs/"+configsetName+configsetSuffix+"/test/upload/path/solrconfig.xml", true));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
      unIgnoreException("ConfigSet uploads do not allow cleanup=true when filePath is used.");
    }
  }

  @Test
  public void testSingleFileTrusted() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testSinglePathTrusted-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, "solr");
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, "solr", zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", true, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);

      ignoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");
      assertEquals("Can't upload a trusted configset with an untrusted request",
              400, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/different/path/solrconfig.xml", true, false));
      assertFalse("New file should not exist, since the trust check did not succeed.", zkClient.exists("/configs/"+configsetName+configsetSuffix+"/test/different/path/solrconfig.xml", true));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
      unIgnoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");

      ignoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");
      int extraFileZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml");
      assertEquals("Can't upload a trusted configset with an untrusted request",
              400, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", true, false));
      assertEquals("Expecting version to remain equal",
              extraFileZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
      unIgnoreException("Trying to make an unstrusted ConfigSet update on a trusted configSet");
    }
  }

  @Test
  public void testSingleFileUntrusted() throws Exception {
    String configsetName = "regular";
    String configsetSuffix = "testSinglePathUntrusted-1";
    uploadConfigSetWithAssertions(configsetName, configsetSuffix, null);
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      // New file with trusted request
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, "solr", zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", false, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);

      // New file with untrusted request
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/different/path/solrconfig.xml", false, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/different/path/solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);

      // Overwrite with trusted request
      int extraFileZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/different/path/solrconfig.xml");
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, "solr", zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/different/path/solrconfig.xml", true, false));
      assertTrue("Expecting version bump",
              extraFileZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/different/path/solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);

      // Overwrite with untrusted request
      extraFileZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml");
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffix, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/upload/path/solrconfig.xml", true, false));
      assertTrue("Expecting version bump",
              extraFileZkVersion < getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/upload/path/solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);

      // Make sure that cleanup flag does not result in configSet being trusted.
      ignoreException("ConfigSet uploads do not allow cleanup=true when filePath is used.");
      extraFileZkVersion = getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/different/path/solrconfig.xml");
      assertEquals(400, uploadSingleConfigSetFile(configsetName, configsetSuffix, "solr", zkClient, "solr/configsets/upload/regular/solrconfig.xml", "/test/different/path/solrconfig.xml", true, true));
      assertEquals("Expecting version to stay the same",
              extraFileZkVersion, getConfigZNodeVersion(zkClient, configsetName, configsetSuffix, "test/different/path/solrconfig.xml"));
      assertFalse("The cleanup=true flag allowed for trust overwriting in a filePath upload.", isTrusted(zkClient, configsetName, configsetSuffix));
      assertConfigsetFiles(configsetName, configsetSuffix, zkClient);
      unIgnoreException("ConfigSet uploads do not allow cleanup=true when filePath is used.");
    }
  }

  @Test
  public void testSingleFileNewConfig() throws Exception {
    String configsetName = "regular";
    String configsetSuffixTrusted = "testSinglePathNewConfig-1";
    String configsetSuffixUntrusted = "testSinglePathNewConfig-2";
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
            AbstractZkTestCase.TIMEOUT, 45000, null)) {
      // New file with trusted request
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffixTrusted, "solr", zkClient, "solr/configsets/upload/regular/solrconfig.xml", "solrconfig.xml", false, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffixTrusted, "solrconfig.xml"));
      assertTrue(isTrusted(zkClient, configsetName, configsetSuffixTrusted));
      List<String> children = zkClient.getChildren(String.format(Locale.ROOT,"/configs/%s%s", configsetName, configsetSuffixTrusted), null, true);
      assertEquals("The configSet should only have one file uploaded.", 1, children.size());
      assertEquals("Incorrect file uploaded.", "solrconfig.xml", children.get(0));

      // New file with trusted request
      assertEquals(0, uploadSingleConfigSetFile(configsetName, configsetSuffixUntrusted, null, zkClient, "solr/configsets/upload/regular/solrconfig.xml", "solrconfig.xml", false, false));
      assertEquals("Expecting first version of new file", 0, getConfigZNodeVersion(zkClient, configsetName, configsetSuffixUntrusted, "solrconfig.xml"));
      assertFalse(isTrusted(zkClient, configsetName, configsetSuffixUntrusted));
      children = zkClient.getChildren(String.format(Locale.ROOT,"/configs/%s%s", configsetName, configsetSuffixUntrusted), null, true);
      assertEquals("The configSet should only have one file uploaded.", 1, children.size());
      assertEquals("Incorrect file uploaded.", "solrconfig.xml", children.get(0));
    }
  }

  private boolean isTrusted(SolrZkClient zkClient, String configsetName, String configsetSuffix) throws KeeperException, InterruptedException {
    String configSetZkPath = String.format(Locale.ROOT,"/configs/%s%s", configsetName, configsetSuffix);
    byte[] configSetNodeContent = zkClient.getData(configSetZkPath, null, null, true);;

    @SuppressWarnings("unchecked")
    Map<Object, Object> contentMap = (Map<Object, Object>) Utils.fromJSON(configSetNodeContent);
    return (boolean) contentMap.getOrDefault("trusted", true);
  }

  private int getConfigZNodeVersion(SolrZkClient zkClient, String configsetName, String configsetSuffix, String configFile) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zkClient.getData(String.format(Locale.ROOT, "/configs/%s%s/%s", configsetName, configsetSuffix, configFile), null, stat, true);
    return stat.getVersion();
  }

  @Test
  public void testUpload() throws Exception {
    String suffix = "-untrusted";
    uploadConfigSetWithAssertions("regular", suffix, null);
    // try to create a collection with the uploaded configset
    createCollection("newcollection", "regular" + suffix, 1, 1, cluster.getSolrClient());
  }

  @Test
  public void testUploadWithScriptUpdateProcessor() throws Exception {
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("JavaScript"));

    // Authorization off
    final String untrustedSuffix = "-untrusted";
    uploadConfigSetWithAssertions("with-script-processor", untrustedSuffix, null);
    // try to create a collection with the uploaded configset
    ignoreException("uploaded without any authentication in place");
    Throwable thrown = expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
      createCollection("newcollection2", "with-script-processor" + untrustedSuffix,
              1, 1, cluster.getSolrClient());
    });
    unIgnoreException("uploaded without any authentication in place");

    assertThat(thrown.getMessage(), containsString("Underlying core creation failed"));

    // Authorization on
    final String trustedSuffix = "-trusted";
    uploadConfigSetWithAssertions("with-script-processor", trustedSuffix, "solr");
    // try to create a collection with the uploaded configset
    CollectionAdminResponse resp = createCollection("newcollection2", "with-script-processor" + trustedSuffix,
            1, 1, cluster.getSolrClient());
    scriptRequest("newcollection2");

  }

  @Test
  public void testUploadWithLibDirective() throws Exception {
    final String untrustedSuffix = "-untrusted";
    uploadConfigSetWithAssertions("with-lib-directive", untrustedSuffix, null);
    // try to create a collection with the uploaded configset
    ignoreException("without any authentication in place");
    Throwable thrown = expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> {
      createCollection("newcollection3", "with-lib-directive" + untrustedSuffix,
          1, 1, cluster.getSolrClient());
    });
    unIgnoreException("without any authentication in place");

    assertThat(thrown.getMessage(), containsString("Underlying core creation failed"));

    // Authorization on
    final String trustedSuffix = "-trusted";
    uploadConfigSetWithAssertions("with-lib-directive", trustedSuffix, "solr");
    // try to create a collection with the uploaded configset
    CollectionAdminResponse resp = createCollection("newcollection3", "with-lib-directive" + trustedSuffix,
        1, 1, cluster.getSolrClient());
    
    SolrInputDocument doc = sdoc("id", "4055", "subject", "Solr");
    cluster.getSolrClient().add("newcollection3", doc);
    cluster.getSolrClient().commit("newcollection3");
    assertEquals("4055", cluster.getSolrClient().query("newcollection3",
        params("q", "*:*")).getResults().get(0).get("id"));
  }

  private static String getSecurityJson() throws KeeperException, InterruptedException {
    String securityJson = "{\n" +
            "  'authentication':{\n" +
            "    'class':'" + MockAuthenticationPlugin.class.getName() + "'},\n" +
            "  'authorization':{\n" +
            "    'class':'" + MockAuthorizationPlugin.class.getName() + "'}}";
    return securityJson;
  }

  private void uploadConfigSetWithAssertions(String configSetName, String suffix, String username) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null);
    try {
      long statusCode = uploadConfigSet(configSetName, suffix, username, zkClient);
      assertEquals(0l, statusCode);
      assertConfigsetFiles(configSetName, suffix, zkClient);
    } finally {
      zkClient.close();
    }
  }
  private void assertConfigsetFiles(String configSetName, String suffix, SolrZkClient zkClient) throws KeeperException, InterruptedException, IOException {
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
  }

  private long uploadConfigSet(String configSetName, String suffix, String username,
                               SolrZkClient zkClient) throws IOException {
    ZkConfigManager configManager = new ZkConfigManager(zkClient);
    assertFalse(configManager.configExists(configSetName + suffix));
    return uploadConfigSet(configSetName, suffix, username, zkClient, false, false);
  }

  private long uploadConfigSet(String configSetName, String suffix, String username,
      SolrZkClient zkClient, boolean overwrite, boolean cleanup) throws IOException {
    // Read zipped sample config
    ByteBuffer sampleZippedConfig = TestSolrConfigHandler
        .getFileContent(
            createTempZipFile("solr/configsets/upload/"+configSetName), false);

    @SuppressWarnings({"rawtypes"})
    Map map = postDataAndGetResponse(cluster.getSolrClient(),
        cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD&name="+configSetName+suffix + (overwrite? "&overwrite=true" : "") + (cleanup? "&cleanup=true" : ""),
        sampleZippedConfig, username);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    return statusCode;
  }

  private long uploadBadConfigSet(String configSetName, String suffix, String username,
                               SolrZkClient zkClient, boolean overwrite, boolean cleanup) throws IOException {
    // Read single file from sample configs. This should fail the unzipping
    ByteBuffer sampleBadZippedFile = TestSolrConfigHandler.getFileContent(SolrTestCaseJ4.getFile("solr/configsets/upload/regular/solrconfig.xml").getAbsolutePath(), false);

    @SuppressWarnings({"rawtypes"})
    Map map = postDataAndGetResponse(cluster.getSolrClient(),
            cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD&name="+configSetName+suffix + (overwrite? "&overwrite=true" : "") + (cleanup? "&cleanup=true" : ""),
            sampleBadZippedFile, username);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    return statusCode;
  }

  private long uploadSingleConfigSetFile(String configSetName, String suffix, String username,
                                         SolrZkClient zkClient, String filePath, String uploadPath, boolean overwrite, boolean cleanup) throws IOException {
    // Read single file from sample configs
    ByteBuffer sampleConfigFile = TestSolrConfigHandler.getFileContent(SolrTestCaseJ4.getFile(filePath).getAbsolutePath(), false);

    @SuppressWarnings({"rawtypes"})
    Map map = postDataAndGetResponse(cluster.getSolrClient(),
            cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/admin/configs?action=UPLOAD&name="+configSetName+suffix+"&filePath="+uploadPath + (overwrite? "&overwrite=true" : "") + (cleanup? "&cleanup=true" : ""),
            sampleConfigFile, username);
    assertNotNull(map);
    long statusCode = (long) getObjectByPath(map, false, Arrays.asList("responseHeader", "status"));
    return statusCode;
  }

  /**
   * Create a zip file (in the temp directory) containing all the files within the specified directory
   * and return the path for the zip file.
   */
  private String createTempZipFile(String directoryPath) {
    File zipFile = new File(cluster.getBaseDir().toFile().getAbsolutePath() +
        File.separator + TestUtil.randomSimpleString(random(), 6, 8) + ".zip");

    File directory = SolrTestCaseJ4.getFile(directoryPath);
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
    SolrClient client = cluster.getSolrClient();
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
    @SuppressWarnings({"rawtypes"})
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse(client.request(request));
    return res;
  }
  
  @SuppressWarnings({"rawtypes"})
  public static Map postDataAndGetResponse(CloudSolrClient cloudClient,
      String uri, ByteBuffer bytarr, String username) throws IOException {
    HttpPost httpPost = null;
    HttpEntity entity;
    String response = null;
    Map m = null;
    
    try {
      httpPost = new HttpPost(uri);
      
      if (username != null) {
        httpPost.addHeader(new BasicHeader("user", username));
      }

      httpPost.setHeader("Content-Type", "application/octet-stream");
      httpPost.setEntity(new ByteArrayEntity(bytarr.array(), bytarr
          .arrayOffset(), bytarr.limit()));
      log.info("Uploading configset with user {}", username);
      entity = cloudClient.getLbClient().getHttpClient().execute(httpPost)
          .getEntity();
      try {
        response = EntityUtils.toString(entity, UTF_8);
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

  private static Object getObjectByPath(@SuppressWarnings({"rawtypes"})Map root,
                                        boolean onlyPrimitive, java.util.List<String> hierarchy) {
    @SuppressWarnings({"rawtypes"})
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
    final String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    // Ensure ConfigSet is immutable
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    FileUtils.write(new File(tmpConfigDir, "configsetprops.json"),
        getConfigSetProps(ImmutableMap.<String, String>of("immutable", "true")), UTF_8);
    zkConfigManager.uploadConfigDir(tmpConfigDir.toPath(), "configSet");

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

  private void verifyException(SolrClient solrClient,
                               @SuppressWarnings({"rawtypes"})ConfigSetAdminRequest request,
      String errorContains) throws Exception {
    ignoreException(errorContains);
    Exception e = expectThrows(Exception.class, () -> solrClient.request(request));
    assertTrue("Expected exception message to contain: " + errorContains
        + " got: " + e.getMessage(), e.getMessage().contains(errorContains));
    unIgnoreException(errorContains);
  }

  @Test
  public void testDelete() throws Exception {
    final String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);
    final String configSet = "testDelete";
    zkConfigManager.uploadConfigDir(configset("configset-2"), configSet);

    SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
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
    final String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);

    SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(),
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
        zkConfigManager.uploadConfigDir(configset("configset-2"), configSet);
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
   * @see #setDefaultConfigDirSysPropIfNotSet
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
    return new StringBuilder(new String(Utils.toJSON(map), UTF_8));
  }

  public static class CreateNoErrorChecking extends ConfigSetAdminRequest.Create {
    @SuppressWarnings({"rawtypes"})
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
    @SuppressWarnings({"rawtypes"})
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

  public static class MockAuthenticationPlugin extends AuthenticationPlugin {

    @Override
    public void init(Map<String, Object> pluginConfig) {

    }

    @Override
    public boolean doAuthenticate(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws Exception {
      if (request.getHeader("user") != null) {
        final Principal p = new BasicUserPrincipal("solr");
        filterChain.doFilter(wrap(request, p, "solr"), response);
      } else {
        filterChain.doFilter(request, response);
      }
      return true;
    }

    HttpServletRequest wrap(HttpServletRequest request, Principal principal, String username) {
      return new HttpServletRequestWrapper(request) {
        @Override
        public Principal getUserPrincipal() {
          return principal;
        }

        @Override
        public String getRemoteUser() {
          return username;
        }
      };
    }
  }

  public static class MockAuthorizationPlugin implements AuthorizationPlugin {

    @Override
    public AuthorizationResponse authorize(AuthorizationContext context) {
      return AuthorizationResponse.OK;
    }

    @Override
    public void init(Map<String, Object> initInfo) {

    }

    @Override
    public void close() throws IOException {

    }
  }
}
