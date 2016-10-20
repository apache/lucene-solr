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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Delete;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.List;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.BASE_CONFIGSET;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.ConfigSetProperties.DEFAULT_FILENAME;

/**
 * Simple ConfigSets API tests on user errors and simple success cases.
 */
public class TestConfigSetsAPI extends SolrTestCaseJ4 {

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
    solrCluster.shutdown();
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

    // no base ConfigSet name
    create.setConfigSetName("configSetName");
    verifyException(solrClient, create, BASE_CONFIGSET);

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
    verifyCreate("baseConfigSet1", "configSet1", null, null);

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
    try {
      solrClient.request(request);
      Assert.fail("Expected exception");
    } catch (Exception e) {
      assertTrue("Expected exception message to contain: " + errorContains
          + " got: " + e.getMessage(), e.getMessage().contains(errorContains));
    }
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
      List list = new List();
      ConfigSetAdminResponse.List response = list.process(solrClient);
      Collection<String> actualConfigSets = response.getConfigSets();
      assertEquals(0, actualConfigSets.size());

      // test multiple
      Set<String> configSets = new HashSet<String>();
      for (int i = 0; i < 5; ++i) {
        String configSet = "configSet" + i;
        solrCluster.uploadConfigSet(configset("configset-2"), configSet);
        configSets.add(configSet);
      }
      response = list.process(solrClient);
      actualConfigSets = response.getConfigSets();
      assertEquals(configSets.size(), actualConfigSets.size());
      assertTrue(configSets.containsAll(actualConfigSets));
    } finally {
      zkClient.close();
    }

    solrClient.close();
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
