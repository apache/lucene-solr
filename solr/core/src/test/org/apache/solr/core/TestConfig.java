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

import net.sf.saxon.om.NodeInfo;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.update.SolrIndexConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;

public class TestConfig extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTestConfig() throws Exception {
    System.setProperty("solr.tests.ramBufferSizeMB", "99");
    initCore("solrconfig-test-misc.xml","schema-reversed.xml");
  }

  @AfterClass
  public static void afterTestConfig() throws Exception {
    deleteCore();
  }

  @Test
  public void testLib() throws IOException {
    try (SolrCore core = h.getCore()) {
      SolrResourceLoader loader = core.getResourceLoader();
      InputStream data = null;
      String[] expectedFiles = new String[] {"empty-file-main-lib.txt", "empty-file-a1.txt", "empty-file-a2.txt", "empty-file-b1.txt", "empty-file-b2.txt", "empty-file-c1.txt"};
      for (String f : expectedFiles) {
        data = loader.openResource(f);
        assertNotNull("Should have found file " + f, data);
        data.close();
      }
      String[] unexpectedFiles = new String[] {"empty-file-c2.txt", "empty-file-d2.txt"};
      for (String f : unexpectedFiles) {
        data = null;
        try {
          data = loader.openResource(f);
        } catch (Exception e) { /* :NOOP: (un)expected */ }
        assertNull("should not have been able to find " + f, data);
      }
    }
  }
  @Test
  public void testDisableRequetsHandler() throws Exception {
    try (SolrCore core = h.getCore()) {
      assertNull(core.getRequestHandler("/disabled"));
      assertNotNull(core.getRequestHandler("/enabled"));
    }
  }

  @Test
  public void testJavaProperty() throws XPathExpressionException {
    // property values defined in build.xml

    String s = solrConfig.get("propTest");
    assertEquals("prefix-proptwo-suffix", s);

    s = solrConfig.get("propTest/@attr1", "default");
    assertEquals("propone-${literal}", s);

    s = solrConfig.get("propTest/@attr2", "default");
    assertEquals("default-from-config", s);

    s = solrConfig.get("propTest[@attr2='default-from-config']", "default");
    assertEquals("prefix-proptwo-suffix", s);

    ArrayList<NodeInfo> nl = (ArrayList) solrConfig.evaluate(solrConfig.getTree(), "propTest", XPathConstants.NODESET);
    assertEquals(1, nl.size());
    assertEquals("prefix-proptwo-suffix", nl.get(0).getStringValue());
    String path = IndexSchema.normalize("propTest", solrConfig.getPrefix());
    NodeInfo node = solrConfig.getNode(solrConfig.getResourceLoader().getXPath().compile(path), path, true);
    assertEquals("prefix-proptwo-suffix", node.getStringValue());
  }

  // sometime if the config referes to old things, it must be replaced with new stuff
  @Test
  public void testAutomaticDeprecationSupport() {
    // make sure the "admin/file" handler is registered
    try (SolrCore core = h.getCore()) {
      ShowFileRequestHandler handler = (ShowFileRequestHandler)core.getRequestHandler("/admin/file");
      assertTrue("file handler should have been automatically registered", handler != null);
    }
  }
  
 @Test
 public void testCacheEnablingDisabling() throws Exception {
   // ensure if cache is not defined in the config then cache is disabled 
   try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
     SolrConfig sc = new SolrConfig(loader, "solrconfig-defaults.xml");
     assertNull(sc.filterCacheConfig);
     assertNull(sc.queryResultCacheConfig);
     assertNull(sc.documentCacheConfig);
     //
     assertNotNull(sc.userCacheConfigs);
     assertEquals(Collections.<String,CacheConfig>emptyMap(), sc.userCacheConfigs);
   }
   
   // enable all the core caches (and one user cache) via system properties and verify 
   System.setProperty("filterCache.enabled", "true");
   System.setProperty("queryResultCache.enabled", "true");
   System.setProperty("documentCache.enabled", "true");
   System.setProperty("user_definied_cache_XXX.enabled","true");
   // user_definied_cache_ZZZ.enabled defaults to false in config
   SolrConfig sc;
   try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
     sc = new SolrConfig(loader, "solrconfig-cache-enable-disable.xml");
   }
   assertNotNull(sc.filterCacheConfig);
   assertNotNull(sc.queryResultCacheConfig);
   assertNotNull(sc.documentCacheConfig);
   //
   assertNotNull(sc.userCacheConfigs);
   assertEquals(1, sc.userCacheConfigs.size());
   assertNotNull(sc.userCacheConfigs.get("user_definied_cache_XXX"));
   
   // disable all the core caches (and enable both user caches) via system properties and verify
   System.setProperty("filterCache.enabled", "false");
   System.setProperty("queryResultCache.enabled", "false");
   System.setProperty("documentCache.enabled", "false");
   System.setProperty("user_definied_cache_XXX.enabled","true");
   System.setProperty("user_definied_cache_ZZZ.enabled","true");

   try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
     sc = new SolrConfig(loader, "solrconfig-cache-enable-disable.xml");
   }
   assertNull(sc.filterCacheConfig);
   assertNull(sc.queryResultCacheConfig);
   assertNull(sc.documentCacheConfig);
   //
   assertNotNull(sc.userCacheConfigs);
   assertEquals(2, sc.userCacheConfigs.size());
   assertNotNull(sc.userCacheConfigs.get("user_definied_cache_XXX"));
   assertNotNull(sc.userCacheConfigs.get("user_definied_cache_ZZZ"));
   
   System.clearProperty("user_definied_cache_XXX.enabled");
   System.clearProperty("user_definied_cache_ZZZ.enabled");
   System.clearProperty("filterCache.enabled");
   System.clearProperty("queryResultCache.enabled");
   System.clearProperty("documentCache.enabled");
 }
  

  // If defaults change, add test methods to cover each version
  @Test
  public void testDefaults() throws Exception {

    int numDefaultsTested = 0;
    int numNullDefaults = 0;
    SolrConfig sc;
    try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
       sc = new SolrConfig(loader, "solrconfig-defaults.xml");
    }
    SolrIndexConfig sic = sc.indexConfig;

    ++numDefaultsTested; assertEquals("default useCompoundFile", false, sic.useCompoundFile);

    ++numDefaultsTested; assertEquals("default maxBufferedDocs", -1, sic.maxBufferedDocs);

    ++numDefaultsTested; assertEquals("default ramBufferSizeMB", 100.0D, sic.ramBufferSizeMB, 0.0D);
    ++numDefaultsTested; assertEquals("default ramPerThreadHardLimitMB", -1, sic.ramPerThreadHardLimitMB);
    ++numDefaultsTested; assertEquals("default writeLockTimeout", -1, sic.writeLockTimeout);
    ++numDefaultsTested; assertEquals("default LockType", DirectoryFactory.LOCK_TYPE_NATIVE, sic.lockType);

    ++numDefaultsTested; assertEquals("default infoStream", InfoStream.NO_OUTPUT, sic.infoStream);

    ++numDefaultsTested; assertNotNull("default metrics", sic.metricsInfo);

    ++numDefaultsTested; ++numNullDefaults;
    assertNull("default mergePolicyFactoryInfo", sic.mergePolicyFactoryInfo);

    ++numDefaultsTested; ++numNullDefaults; assertNull("default mergeSchedulerInfo", sic.mergeSchedulerInfo);
    ++numDefaultsTested; ++numNullDefaults; assertNull("default mergedSegmentWarmerInfo", sic.mergedSegmentWarmerInfo);

    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", solrConfig);
    IndexWriterConfig iwc;
    try (SolrCore core = h.getCore()) {
      iwc = sic.toIndexWriterConfig(core);
    }

    assertNotNull("null mp", iwc.getMergePolicy());
    assertTrue("mp is not TieredMergePolicy", iwc.getMergePolicy() instanceof TieredMergePolicy);

    assertNotNull("null ms", iwc.getMergeScheduler());
    assertTrue("ms is not CMS", iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler);

    assertNull("non-null mergedSegmentWarmer", iwc.getMergedSegmentWarmer());

    final int numDefaultsMapped = sic.toMap(new LinkedHashMap<>()).size();
    assertEquals("numDefaultsTested vs. numDefaultsMapped+numNullDefaults ="+sic.toMap(new LinkedHashMap<>()).keySet(), numDefaultsTested, numDefaultsMapped+numNullDefaults);
  }

  @Test
  public void testConvertAutoCommitMaxSizeStringToBytes() {

    // Valid values
    Assert.assertEquals(300, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300"));
    Assert.assertEquals(307200, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300k"));
    Assert.assertEquals(307200, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300K"));
    Assert.assertEquals(314572800, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300m"));
    Assert.assertEquals(314572800, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300M"));
    Assert.assertEquals(322122547200L, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300g"));
    Assert.assertEquals(322122547200L, SolrConfig.convertHeapOptionStyleConfigStringToBytes("300G"));
    Assert.assertEquals(-1, SolrConfig.convertHeapOptionStyleConfigStringToBytes(""));

    // Invalid values
    RuntimeException thrown = SolrTestCaseUtil.expectThrows(RuntimeException.class, () -> {
      SolrConfig.convertHeapOptionStyleConfigStringToBytes("3jbk32k"); // valid suffix but non-numeric prefix
    });
    assertTrue(thrown.getMessage().contains("Invalid"));

    thrown = SolrTestCaseUtil.expectThrows(RuntimeException.class, () -> {
      SolrConfig.convertHeapOptionStyleConfigStringToBytes("300x"); // valid prefix but invalid suffix
    });
    assertTrue(thrown.getMessage().contains("Invalid"));
  }

  @Test
  public void testMaxSizeSettingWithoutAutoCommit() throws Exception {
    try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
      SolrConfig solrConfig = new SolrConfig(loader, "bad-solrconfig-no-autocommit-tag.xml");
      Assert.assertEquals(-1, solrConfig.getUpdateHandlerInfo().autoCommitMaxSizeBytes);
      Assert.assertEquals(-1, solrConfig.getUpdateHandlerInfo().autoCommmitMaxDocs);
      Assert.assertEquals(-1, solrConfig.getUpdateHandlerInfo().autoCommmitMaxTime);
    }
  }

  // sanity check that sys properties are working as expected
  public void testSanityCheckTestSysPropsAreUsed() throws Exception {
    System.setProperty("solr.tests.ramBufferSizeMB", "100");
    try (SolrResourceLoader loader = new SolrResourceLoader(SolrTestUtil.TEST_PATH().resolve("collection1"))) {
      SolrConfig sc = new SolrConfig(loader, "solrconfig-basic.xml");
      SolrIndexConfig sic = sc.indexConfig;

      assertEquals("ramBufferSizeMB sysprop", Double.parseDouble(System.getProperty("solr.tests.ramBufferSizeMB")), sic.ramBufferSizeMB, 0.0D);
      assertEquals("ramPerThreadHardLimitMB sysprop", Integer.parseInt(System.getProperty("solr.tests.ramPerThreadHardLimitMB")), sic.ramPerThreadHardLimitMB);
      assertEquals("useCompoundFile sysprop", Boolean.parseBoolean(System.getProperty("useCompoundFile")), sic.useCompoundFile);
    }
  }

}
