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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Delete;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the exclusivity of the ConfigSets API.
 * Submits a number of API requests concurrently and checks that
 * the responses indicate the requests are handled sequentially for
 * the same ConfigSet and base ConfigSet.
 */
public class TestConfigSetsAPIExclusivity extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private MiniSolrCloudCluster solrCluster;
  private static final String GRANDBASE_CONFIGSET_NAME = "grandBaseConfigSet1";
  private static final String BASE_CONFIGSET_NAME = "baseConfigSet1";
  private static final String CONFIGSET_NAME = "configSet1";

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
  public void testAPIExclusivity() throws Exception {
    int trials = 20;
    setupBaseConfigSet(GRANDBASE_CONFIGSET_NAME);
    CreateThread createBaseThread =
        new CreateThread(solrCluster, BASE_CONFIGSET_NAME, GRANDBASE_CONFIGSET_NAME, trials);
    CreateThread createThread =
        new CreateThread(solrCluster, CONFIGSET_NAME, BASE_CONFIGSET_NAME, trials);
    DeleteThread deleteBaseThread = new DeleteThread(solrCluster, BASE_CONFIGSET_NAME, trials);
    DeleteThread deleteThread = new DeleteThread(solrCluster, CONFIGSET_NAME, trials);
    List<ConfigSetsAPIThread> threads = Arrays.asList(
        createBaseThread, createThread, deleteBaseThread, deleteThread);

    for (ConfigSetsAPIThread thread : threads) {
      thread.start();
    }
    for (ConfigSetsAPIThread thread : threads) {
      thread.join();
    }
    List<Exception> exceptions = new LinkedList<Exception>();
    for (ConfigSetsAPIThread thread : threads) {
      exceptions.addAll(thread.getUnexpectedExceptions());
    }
    assertEquals("Unexpected exception: " + getFirstExceptionOrNull(exceptions),
        0, exceptions.size());
  }

  private void setupBaseConfigSet(String baseConfigSetName) throws Exception {
    solrCluster.uploadConfigSet(configset("configset-2"), baseConfigSetName);
    //Make configset untrusted
    solrCluster.getZkClient().setData("/configs/" + baseConfigSetName, "{\"trusted\": false}".getBytes(StandardCharsets.UTF_8), true);
  }

  private Exception getFirstExceptionOrNull(List<Exception> list) {
    return list.size() == 0 ? null : list.get(0);
  }

  private static abstract class ConfigSetsAPIThread extends Thread {
    private MiniSolrCloudCluster solrCluster;
    private int trials;
    private List<Exception> unexpectedExceptions = new LinkedList<Exception>();
    private List<String> allowedExceptions = Arrays.asList(new String[] {
        "ConfigSet already exists",
        "ConfigSet does not exist to delete",
        "Base ConfigSet does not exist"});

    public ConfigSetsAPIThread(MiniSolrCloudCluster solrCluster, int trials) {
      this.solrCluster = solrCluster;
      this.trials = trials;
    }

    @SuppressWarnings({"rawtypes"})
    public abstract ConfigSetAdminRequest createRequest();

    public void run() {
      final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
      final SolrClient solrClient = getHttpSolrClient(baseUrl);
      @SuppressWarnings({"rawtypes"})
      ConfigSetAdminRequest request = createRequest();

      for (int i = 0; i < trials; ++i) {
        try {
          request.process(solrClient);
        } catch (Exception e) {
          verifyException(e);
        }
      }
      try {
        solrClient.close();
      } catch (Exception e) {
        log.error("Error closing client", e);
      }
    }

    private void verifyException(Exception e) {
      for (String ex : allowedExceptions) {
        if (e.getMessage().contains(ex)) {
          return;
        }
      }
      unexpectedExceptions.add(e);
    }

    public List<Exception> getUnexpectedExceptions() {
      return unexpectedExceptions;
    }
  }

  private static class CreateThread extends ConfigSetsAPIThread {
    private String configSet;
    private String baseConfigSet;

    public CreateThread(MiniSolrCloudCluster solrCluster, String configSet,
        String baseConfigSet, int trials) {
      super(solrCluster, trials);
      this.configSet = configSet;
      this.baseConfigSet = baseConfigSet;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public ConfigSetAdminRequest createRequest() {
      Create create = new Create();
      create.setBaseConfigSetName(baseConfigSet).setConfigSetName(configSet);
      return create;
    }
  }

  private static class DeleteThread extends ConfigSetsAPIThread {
    private String configSet;

    public DeleteThread(MiniSolrCloudCluster solrCluster, String configSet, int trials) {
      super(solrCluster, trials);
      this.configSet = configSet;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public ConfigSetAdminRequest createRequest() {
      Delete delete = new Delete();
      delete.setConfigSetName(configSet);
      return delete;
    }
  }
}
