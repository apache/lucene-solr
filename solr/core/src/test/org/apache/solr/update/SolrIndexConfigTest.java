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
package org.apache.solr.update;

import java.nio.file.Path;
import java.util.Map;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.TestMergePolicyConfig;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testcase for {@link SolrIndexConfig}
 *
 * @see TestMergePolicyConfig
 */
public class SolrIndexConfigTest extends SolrTestCaseJ4 {

  private static final String solrConfigFileName = "solrconfig.xml";
  private static final String solrConfigFileNameWarmerRandomMergePolicy = "solrconfig-warmer.xml";
  private static final String solrConfigFileNameWarmerRandomMergePolicyFactory = "solrconfig-warmer-randommergepolicyfactory.xml";
  private static final String solrConfigFileNameTieredMergePolicy = "solrconfig-tieredmergepolicy.xml";
  private static final String solrConfigFileNameTieredMergePolicyFactory = "solrconfig-tieredmergepolicyfactory.xml";
  private static final String schemaFileName = "schema.xml";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(solrConfigFileName,schemaFileName);
  }
  
  private final Path instanceDir = TEST_PATH().resolve("collection1");

  @Test
  public void testFailingSolrIndexConfigCreation() {
    try {
      SolrConfig solrConfig = new SolrConfig(random().nextBoolean() ? "bad-mp-solrconfig.xml" : "bad-mpf-solrconfig.xml");
      SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null, null);
      IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);
      h.getCore().setLatestSchema(indexSchema);
      solrIndexConfig.toIndexWriterConfig(h.getCore());
      fail("a mergePolicy should have an empty constructor in order to be instantiated in Solr thus this should fail ");
    } catch (Exception e) {
      // it failed as expected
    }
  }

  @Test
  public void testTieredMPSolrIndexConfigCreation() throws Exception {
    String solrConfigFileName = random().nextBoolean() ? solrConfigFileNameTieredMergePolicy : solrConfigFileNameTieredMergePolicyFactory;
    SolrConfig solrConfig = new SolrConfig(instanceDir, solrConfigFileName, null);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null, null);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);
    
    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());

    assertNotNull("null mp", iwc.getMergePolicy());
    assertTrue("mp is not TieredMergePolicy", iwc.getMergePolicy() instanceof TieredMergePolicy);
    TieredMergePolicy mp = (TieredMergePolicy) iwc.getMergePolicy();
    assertEquals("mp.maxMergeAtOnceExplicit", 19, mp.getMaxMergeAtOnceExplicit());
    assertEquals("mp.segmentsPerTier",9,(int)mp.getSegmentsPerTier());

    assertNotNull("null ms", iwc.getMergeScheduler());
    assertTrue("ms is not CMS", iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler);
    ConcurrentMergeScheduler ms = (ConcurrentMergeScheduler)  iwc.getMergeScheduler();
    assertEquals("ms.maxMergeCount", 987, ms.getMaxMergeCount());
    assertEquals("ms.maxThreadCount", 42, ms.getMaxThreadCount());

  }

  public void testMergedSegmentWarmerIndexConfigCreation() throws Exception {
    SolrConfig solrConfig = new SolrConfig(instanceDir, random().nextBoolean() ? solrConfigFileNameWarmerRandomMergePolicy : solrConfigFileNameWarmerRandomMergePolicyFactory, null);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null, null);
    assertNotNull(solrIndexConfig);
    assertNotNull(solrIndexConfig.mergedSegmentWarmerInfo);
    assertEquals(SimpleMergedSegmentWarmer.class.getName(),
        solrIndexConfig.mergedSegmentWarmerInfo.className);
    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schemaFileName, solrConfig);
    h.getCore().setLatestSchema(indexSchema);
    IndexWriterConfig iwc = solrIndexConfig.toIndexWriterConfig(h.getCore());
    assertEquals(SimpleMergedSegmentWarmer.class, iwc.getMergedSegmentWarmer().getClass());
  }

  public void testToMap() throws Exception {
    final String solrConfigFileNameWarmer = random().nextBoolean() ? solrConfigFileNameWarmerRandomMergePolicy : solrConfigFileNameWarmerRandomMergePolicyFactory;
    final String solrConfigFileNameTMP = random().nextBoolean() ? solrConfigFileNameTieredMergePolicy : solrConfigFileNameTieredMergePolicyFactory;
    final String solrConfigFileName = (random().nextBoolean() ? solrConfigFileNameWarmer : solrConfigFileNameTMP);
    SolrConfig solrConfig = new SolrConfig(instanceDir, solrConfigFileName, null);
    SolrIndexConfig solrIndexConfig = new SolrIndexConfig(solrConfig, null, null);
    assertNotNull(solrIndexConfig);
    if (solrConfigFileName.equals(solrConfigFileNameTieredMergePolicyFactory) ||
        solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      assertNotNull(solrIndexConfig.mergePolicyFactoryInfo);
    } else {
      assertNotNull(solrIndexConfig.mergePolicyInfo);
    }
    if (solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicy) ||
        solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      assertNotNull(solrIndexConfig.mergedSegmentWarmerInfo);
    } else {
      assertNull(solrIndexConfig.mergedSegmentWarmerInfo);
    }
    assertNotNull(solrIndexConfig.mergeSchedulerInfo);

    Map<String, Object> m = solrIndexConfig.toMap();
    int mSizeExpected = 0;

    ++mSizeExpected; assertTrue(m.get("useCompoundFile") instanceof Boolean);

    ++mSizeExpected; assertTrue(m.get("maxBufferedDocs") instanceof Integer);
    ++mSizeExpected; assertTrue(m.get("maxMergeDocs") instanceof Integer);
    ++mSizeExpected; assertTrue(m.get("mergeFactor") instanceof Integer);

    ++mSizeExpected; assertTrue(m.get("ramBufferSizeMB") instanceof Double);

    ++mSizeExpected; assertTrue(m.get("writeLockTimeout") instanceof Integer);

    ++mSizeExpected; assertTrue(m.get("lockType") instanceof String);
    {
      final String lockType = (String)m.get("lockType");
      assertTrue(DirectoryFactory.LOCK_TYPE_SIMPLE.equals(lockType) ||
          DirectoryFactory.LOCK_TYPE_NATIVE.equals(lockType) ||
          DirectoryFactory.LOCK_TYPE_SINGLE.equals(lockType) ||
          DirectoryFactory.LOCK_TYPE_NONE.equals(lockType) ||
          DirectoryFactory.LOCK_TYPE_HDFS.equals(lockType));
    }

    ++mSizeExpected; assertTrue(m.get("infoStreamEnabled") instanceof Boolean);
    {
      assertFalse(Boolean.valueOf(m.get("infoStreamEnabled").toString()).booleanValue());
    }
    
    ++mSizeExpected; assertTrue(m.get("mergeScheduler") instanceof Map);
    if (solrConfigFileName.equals(solrConfigFileNameTieredMergePolicyFactory) ||
        solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      assertNull(m.get("mergePolicy"));
      ++mSizeExpected; assertTrue(m.get("mergePolicyFactory") instanceof Map);
    } else {
      ++mSizeExpected; assertTrue(m.get("mergePolicy") instanceof Map);
      assertNull(m.get("mergePolicyFactory"));
    }
    if (solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicy) ||
        solrConfigFileName.equals(solrConfigFileNameWarmerRandomMergePolicyFactory)) {
      ++mSizeExpected; assertTrue(m.get("mergedSegmentWarmer") instanceof Map);
    } else {
      assertNull(m.get("mergedSegmentWarmer"));
    }

    assertEquals(mSizeExpected, m.size());
  }
}
