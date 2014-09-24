package org.apache.solr.core;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.update.SolrIndexConfigTest;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.solr.util.RefCounted;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import java.util.concurrent.atomic.AtomicInteger;

/** @see SolrIndexConfigTest */
public class TestMergePolicyConfig extends SolrTestCaseJ4 {
  
  private static AtomicInteger docIdCounter = new AtomicInteger(42);

  @After
  public void after() throws Exception {
    deleteCore();
  }

  public void testDefaultMergePolicyConfig() throws Exception {
    initCore("solrconfig-mergepolicy-defaults.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());
    assertEquals(false, iwc.getUseCompoundFile());

    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());
    assertEquals(0.0D, tieredMP.getNoCFSRatio(), 0.0D);

    assertCommitSomeNewDocs();
    assertCompoundSegments(h.getCore(), false);
  }

  public void testLegacyMergePolicyConfig() throws Exception {
    final boolean expectCFS 
      = Boolean.parseBoolean(System.getProperty("useCompoundFile"));

    initCore("solrconfig-mergepolicy-legacy.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());
    assertEquals(expectCFS, iwc.getUseCompoundFile());


    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());

    assertEquals(7, tieredMP.getMaxMergeAtOnce());
    assertEquals(7.0D, tieredMP.getSegmentsPerTier(), 0.0D);
    assertEquals(expectCFS ? 1.0D : 0.0D, tieredMP.getNoCFSRatio(), 0.0D);

    assertCommitSomeNewDocs();
    assertCompoundSegments(h.getCore(), expectCFS);
  }
  
  public void testTieredMergePolicyConfig() throws Exception {
    final boolean expectCFS 
      = Boolean.parseBoolean(System.getProperty("useCompoundFile"));

    initCore("solrconfig-tieredmergepolicy.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());
    assertEquals(expectCFS, iwc.getUseCompoundFile());


    TieredMergePolicy tieredMP = assertAndCast(TieredMergePolicy.class,
                                               iwc.getMergePolicy());

    // set by legacy <mergeFactor> setting
    assertEquals(7, tieredMP.getMaxMergeAtOnce());
    
    // mp-specific setters
    assertEquals(19, tieredMP.getMaxMergeAtOnceExplicit());
    assertEquals(0.1D, tieredMP.getNoCFSRatio(), 0.0D);
    // make sure we overrode segmentsPerTier 
    // (split from maxMergeAtOnce out of mergeFactor)
    assertEquals(9D, tieredMP.getSegmentsPerTier(), 0.001);
    
    assertCommitSomeNewDocs();
    // even though we have a single segment (which is 100% of the size of 
    // the index which is higher then our 0.6D threashold) the
    // compound ratio doesn't matter because the segment was never merged
    assertCompoundSegments(h.getCore(), expectCFS);

    assertCommitSomeNewDocs();
    assertNumSegments(h.getCore(), 2);
    assertCompoundSegments(h.getCore(), expectCFS);

    assertU(optimize());
    assertNumSegments(h.getCore(), 1);
    // we've now forced a merge, and the MP ratio should be in play
    assertCompoundSegments(h.getCore(), false);
  }

  public void testLogMergePolicyConfig() throws Exception {
    
    final Class<? extends LogMergePolicy> mpClass = random().nextBoolean()
      ? LogByteSizeMergePolicy.class : LogDocMergePolicy.class;

    System.setProperty("solr.test.log.merge.policy", mpClass.getName());

    initCore("solrconfig-logmergepolicy.xml","schema-minimal.xml");
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore().getLatestSchema());

    // verify some props set to -1 get lucene internal defaults
    assertEquals(-1, solrConfig.indexConfig.maxBufferedDocs);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, 
                 iwc.getMaxBufferedDocs());
    assertEquals(-1, solrConfig.indexConfig.maxIndexingThreads);
    assertEquals(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, 
                 iwc.getMaxThreadStates());
    assertEquals(-1, solrConfig.indexConfig.ramBufferSizeMB, 0.0D);
    assertEquals(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, 
                 iwc.getRAMBufferSizeMB(), 0.0D);


    LogMergePolicy logMP = assertAndCast(mpClass, iwc.getMergePolicy());

    // set by legacy <mergeFactor> setting
    assertEquals(11, logMP.getMergeFactor());
    // set by legacy <maxMergeDocs> setting
    assertEquals(456, logMP.getMaxMergeDocs());

  }

  /**
   * Given a Type and an object asserts that the object is non-null and an 
   * instance of the specified Type.  The object is then cast to that type and 
   * returned.
   */
  public static <T> T assertAndCast(Class<? extends T> clazz, Object o) {
    assertNotNull(clazz);
    assertNotNull(o);
    assertTrue(clazz.isInstance(o));
    return clazz.cast(o);
  }

  public static void assertCommitSomeNewDocs() {
    for (int i = 0; i < 5; i++) {
      int val = docIdCounter.getAndIncrement();
      assertU(adoc("id", "" + val,
                   "a_s", val + "_" + val + "_" + val + "_" + val,
                   "b_s", val + "_" + val + "_" + val + "_" + val,
                   "c_s", val + "_" + val + "_" + val + "_" + val,
                   "d_s", val + "_" + val + "_" + val + "_" + val,
                   "e_s", val + "_" + val + "_" + val + "_" + val,
                   "f_s", val + "_" + val + "_" + val + "_" + val));
    }
    assertU(commit());
  }

  /**
   * Given an SolrCore, asserts that the number of leave segments in 
   * the index reader matches the expected value.
   */
  public static void assertNumSegments(SolrCore core, int expected) {
    RefCounted<SolrIndexSearcher> searcherRef = core.getRegisteredSearcher();
    try {
      assertEquals(expected, searcherRef.get().getIndexReader().leaves().size());
    } finally {
      searcherRef.decref();
    }
  }

  /**
   * Given an SolrCore, asserts that each segment in the (searchable) index 
   * has a compound file status that matches the expected input.
   */
  public static void assertCompoundSegments(SolrCore core, boolean compound) {
    RefCounted<SolrIndexSearcher> searcherRef = core.getRegisteredSearcher();
    try {
      assertCompoundSegments(searcherRef.get().getRawReader(), compound);
    } finally {
      searcherRef.decref();
    }
  }

  /**
   * Given an IndexReader, asserts that there is at least one AtomcReader leaf,
   * and that all LeafReader leaves are SegmentReader's that have a compound 
   * file status that matches the expected input.
   */
  private static void assertCompoundSegments(IndexReader reader, 
                                             boolean compound) {

    assertNotNull("Null leaves", reader.leaves());
    assertTrue("no leaves", 0 < reader.leaves().size());

    for (LeafReaderContext atomic : reader.leaves()) {
      assertTrue("not a segment reader: " + atomic.reader().toString(), 
                 atomic.reader() instanceof SegmentReader);
      
      assertEquals("Compound status incorrect for: " + 
                   atomic.reader().toString(),
                   compound,
                   ((SegmentReader)atomic.reader()).getSegmentInfo().info.getUseCompoundFile());
    }
  }

}
