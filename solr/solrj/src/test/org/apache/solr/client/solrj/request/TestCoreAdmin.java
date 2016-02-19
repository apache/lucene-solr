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
package org.apache.solr.client.solrj.request;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;

import java.io.File;
import java.lang.invoke.MethodHandles;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.AbstractEmbeddedSolrServerTestCase;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

@ThreadLeakFilters(defaultFilters = true, filters = {SolrIgnoredThreadsFilter.class})
public class TestCoreAdmin extends AbstractEmbeddedSolrServerTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static String tempDirProp;

  @Rule
  public TestRule testRule = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  /*
  @Override
  protected File getSolrXml() throws Exception {
    // This test writes on the directory where the solr.xml is located. Better
    // to copy the solr.xml to
    // the temporary directory where we store the index
    File origSolrXml = new File(SOLR_HOME, SOLR_XML);
    File solrXml = new File(tempDir, SOLR_XML);
    FileUtils.copyFile(origSolrXml, solrXml);
    return solrXml;
  }
  */

  protected SolrClient getSolrAdmin() {
    return new EmbeddedSolrServer(cores, "core0");
  }

  @Test
  public void testConfigSet() throws Exception {

    SolrClient client = getSolrAdmin();
    File testDir = createTempDir(LuceneTestCase.getTestClass().getSimpleName()).toFile();

    File newCoreInstanceDir = new File(testDir, "newcore");

    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName("corewithconfigset");
    req.setInstanceDir(newCoreInstanceDir.getAbsolutePath());
    req.setConfigSet("configset-2");

    CoreAdminResponse response = req.process(client);
    assertThat((String) response.getResponse().get("core"), is("corewithconfigset"));

    try (SolrCore core = cores.getCore("corewithconfigset")) {
      assertThat(core, is(notNullValue()));
    }

  }

  @Test
  public void testCustomUlogDir() throws Exception {
    
    try (SolrClient client = getSolrAdmin()) {

      File dataDir = createTempDir("data").toFile();

      File newCoreInstanceDir = createTempDir("instance").toFile();

      File instanceDir = new File(cores.getSolrHome());
      FileUtils.copyDirectory(instanceDir, new File(newCoreInstanceDir,
          "newcore"));

      CoreAdminRequest.Create req = new CoreAdminRequest.Create();
      req.setCoreName("newcore");
      req.setInstanceDir(newCoreInstanceDir.getAbsolutePath() + File.separator + "newcore");
      req.setDataDir(dataDir.getAbsolutePath());
      req.setUlogDir(new File(dataDir, "ulog").getAbsolutePath());
      req.setConfigSet("shared");

      // These should be the inverse of defaults.
      req.setIsLoadOnStartup(false);
      req.setIsTransient(true);
      req.process(client);

      // Show that the newly-created core has values for load on startup and transient different than defaults due to the
      // above.
      File logDir;
      try (SolrCore coreProveIt = cores.getCore("collection1");
           SolrCore core = cores.getCore("newcore")) {

        assertTrue(core.getCoreDescriptor().isTransient());
        assertFalse(coreProveIt.getCoreDescriptor().isTransient());

        assertFalse(core.getCoreDescriptor().isLoadOnStartup());
        assertTrue(coreProveIt.getCoreDescriptor().isLoadOnStartup());

        logDir = new File(core.getUpdateHandler().getUpdateLog().getLogDir());
      }

      assertEquals(new File(dataDir, "ulog" + File.separator + "tlog").getAbsolutePath(), logDir.getAbsolutePath());

    }
    
  }
  
  @Test
  public void testErrorCases() throws Exception {
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/cores");
    boolean gotExp = false;
    NamedList<Object> resp = null;
    try {
      resp = getSolrAdmin().request(request);
    } catch (SolrException e) {
      gotExp = true;
    }
    
    assertTrue(gotExp);
  }
  
  @Test
  public void testInvalidCoreNamesAreRejectedWhenCreatingCore() {
    final Create createRequest = new Create();
    
    try {
      createRequest.setCoreName("invalid$core@name");
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(exceptionMessage.contains("Invalid core"));
      assertTrue(exceptionMessage.contains("invalid$core@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
  
  @Test
  public void testInvalidCoreNamesAreRejectedWhenRenamingExistingCore() throws Exception {
    try {
      CoreAdminRequest.renameCore("validExistingCoreName", "invalid$core@name", null);
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(e.getMessage(), exceptionMessage.contains("Invalid core"));
      assertTrue(exceptionMessage.contains("invalid$core@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
  
  @BeforeClass
  public static void before() {
    // wtf?
    if (System.getProperty("tempDir") != null)
      tempDirProp = System.getProperty("tempDir");
  }
  
  @After
  public void after() {
    // wtf?
    if (tempDirProp != null) {
      System.setProperty("tempDir", tempDirProp);
    } else {
      System.clearProperty("tempDir");
    }
    
    System.clearProperty("solr.solr.home");
  }
  
}
