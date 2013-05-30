package org.apache.lucene.index;

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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestIndexWriterConfig extends LuceneTestCase {

  private static final class MySimilarity extends DefaultSimilarity {
    // Does not implement anything - used only for type checking on IndexWriterConfig.
  }

  private static final class MyIndexingChain extends IndexingChain {
    // Does not implement anything - used only for type checking on IndexWriterConfig.

    @Override
    DocConsumer getChain(DocumentsWriterPerThread documentsWriter) {
      return null;
    }

  }

  @Test
  public void testDefaults() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    assertEquals(MockAnalyzer.class, conf.getAnalyzer().getClass());
    assertNull(conf.getIndexCommit());
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    assertEquals(OpenMode.CREATE_OR_APPEND, conf.getOpenMode());
    // we don't need to assert this, it should be unspecified
    assertTrue(IndexSearcher.getDefaultSimilarity() == conf.getSimilarity());
    assertEquals(IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL, conf.getTermIndexInterval());
    assertEquals(IndexWriterConfig.getDefaultWriteLockTimeout(), conf.getWriteLockTimeout());
    assertEquals(IndexWriterConfig.WRITE_LOCK_TIMEOUT, IndexWriterConfig.getDefaultWriteLockTimeout());
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS, conf.getMaxBufferedDeleteTerms());
    assertEquals(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, conf.getRAMBufferSizeMB(), 0.0);
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS, conf.getMaxBufferedDocs());
    assertEquals(IndexWriterConfig.DEFAULT_READER_POOLING, conf.getReaderPooling());
    assertTrue(DocumentsWriterPerThread.defaultIndexingChain == conf.getIndexingChain());
    assertNull(conf.getMergedSegmentWarmer());
    assertEquals(IndexWriterConfig.DEFAULT_READER_TERMS_INDEX_DIVISOR, conf.getReaderTermsIndexDivisor());
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    assertEquals(ThreadAffinityDocumentsWriterThreadPool.class, conf.getIndexerThreadPool().getClass());
    assertEquals(FlushByRamOrCountsPolicy.class, conf.getFlushPolicy().getClass());
    assertEquals(IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB, conf.getRAMPerThreadHardLimitMB());
    assertEquals(Codec.getDefault(), conf.getCodec());
    assertEquals(InfoStream.getDefault(), conf.getInfoStream());
    // Sanity check - validate that all getters are covered.
    Set<String> getters = new HashSet<String>();
    getters.add("getAnalyzer");
    getters.add("getIndexCommit");
    getters.add("getIndexDeletionPolicy");
    getters.add("getMaxFieldLength");
    getters.add("getMergeScheduler");
    getters.add("getOpenMode");
    getters.add("getSimilarity");
    getters.add("getTermIndexInterval");
    getters.add("getWriteLockTimeout");
    getters.add("getDefaultWriteLockTimeout");
    getters.add("getMaxBufferedDeleteTerms");
    getters.add("getRAMBufferSizeMB");
    getters.add("getMaxBufferedDocs");
    getters.add("getIndexingChain");
    getters.add("getMergedSegmentWarmer");
    getters.add("getMergePolicy");
    getters.add("getMaxThreadStates");
    getters.add("getReaderPooling");
    getters.add("getIndexerThreadPool");
    getters.add("getReaderTermsIndexDivisor");
    getters.add("getFlushPolicy");
    getters.add("getRAMPerThreadHardLimitMB");
    getters.add("getCodec");
    getters.add("getInfoStream");
    
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getDeclaringClass() == IndexWriterConfig.class && m.getName().startsWith("get")) {
        assertTrue("method " + m.getName() + " is not tested for defaults", getters.contains(m.getName()));
      }
    }
  }

  @Test
  public void testSettersChaining() throws Exception {
    // Ensures that every setter returns IndexWriterConfig to allow chaining.
    HashSet<String> liveSetters = new HashSet<String>();
    HashSet<String> allSetters = new HashSet<String>();
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getName().startsWith("set") && !Modifier.isStatic(m.getModifiers())) {
        allSetters.add(m.getName());
        // setters overridden from LiveIndexWriterConfig are returned twice, once with 
        // IndexWriterConfig return type and second with LiveIndexWriterConfig. The ones
        // from LiveIndexWriterConfig are marked 'synthetic', so just collect them and
        // assert in the end that we also received them from IWC.
        if (m.isSynthetic()) {
          liveSetters.add(m.getName());
        } else {
          assertEquals("method " + m.getName() + " does not return IndexWriterConfig",
              IndexWriterConfig.class, m.getReturnType());
        }
      }
    }
    for (String setter : liveSetters) {
      assertTrue("setter method not overridden by IndexWriterConfig: " + setter, allSetters.contains(setter));
    }
  }

  @Test
  public void testReuse() throws Exception {
    Directory dir = newDirectory();
    // test that if the same IWC is reused across two IWs, it is cloned by each.
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, conf);
    LiveIndexWriterConfig liveConf1 = iw.w.getConfig();
    iw.close();
    
    iw = new RandomIndexWriter(random(), dir, conf);
    LiveIndexWriterConfig liveConf2 = iw.w.getConfig();
    iw.close();
    
    // LiveIndexWriterConfig's "copy" constructor doesn't clone objects.
    assertNotSame("IndexWriterConfig should have been cloned", liveConf1.getMergePolicy(), liveConf2.getMergePolicy());
    
    dir.close();
  }
  
  @Test
  public void testOverrideGetters() throws Exception {
    // Test that IndexWriterConfig overrides all getters, so that javadocs
    // contain all methods for the users. Also, ensures that IndexWriterConfig
    // doesn't declare getters that are not declared on LiveIWC.
    HashSet<String> liveGetters = new HashSet<String>();
    for (Method m : LiveIndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getName().startsWith("get") && !Modifier.isStatic(m.getModifiers())) {
        liveGetters.add(m.getName());
      }
    }
    
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getName().startsWith("get") && !Modifier.isStatic(m.getModifiers())) {
        assertEquals("method " + m.getName() + " not overrided by IndexWriterConfig", 
            IndexWriterConfig.class, m.getDeclaringClass());
        assertTrue("method " + m.getName() + " not declared on LiveIndexWriterConfig", 
            liveGetters.contains(m.getName()));
      }
    }
  }
  
  @Test
  public void testConstants() throws Exception {
    // Tests that the values of the constants does not change
    assertEquals(1000, IndexWriterConfig.WRITE_LOCK_TIMEOUT);
    assertEquals(32, IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL);
    assertEquals(-1, IndexWriterConfig.DISABLE_AUTO_FLUSH);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
    assertEquals(16.0, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, 0.0);
    assertEquals(false, IndexWriterConfig.DEFAULT_READER_POOLING);
    assertEquals(DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, IndexWriterConfig.DEFAULT_READER_TERMS_INDEX_DIVISOR);
  }

  @Test
  public void testToString() throws Exception {
    String str = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).toString();
    for (Field f : IndexWriterConfig.class.getDeclaredFields()) {
      int modifiers = f.getModifiers();
      if (Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
        // Skip static final fields, they are only constants
        continue;
      } else if ("indexingChain".equals(f.getName())) {
        // indexingChain is a package-private setting and thus is not output by
        // toString.
        continue;
      }
      if (f.getName().equals("inUseByIndexWriter")) {
        continue;
      }
      assertTrue(f.getName() + " not found in toString", str.indexOf(f.getName()) != -1);
    }
  }

  @Test
  public void testClone() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriterConfig clone = conf.clone();

    // Make sure parameters that can't be reused are cloned
    IndexDeletionPolicy delPolicy = conf.delPolicy;
    IndexDeletionPolicy delPolicyClone = clone.delPolicy;
    assertTrue(delPolicy.getClass() == delPolicyClone.getClass() && (delPolicy != delPolicyClone || delPolicy.clone() == delPolicyClone.clone()));

    FlushPolicy flushPolicy = conf.flushPolicy;
    FlushPolicy flushPolicyClone = clone.flushPolicy;
    assertTrue(flushPolicy.getClass() == flushPolicyClone.getClass() && (flushPolicy != flushPolicyClone || flushPolicy.clone() == flushPolicyClone.clone()));

    DocumentsWriterPerThreadPool pool = conf.indexerThreadPool;
    DocumentsWriterPerThreadPool poolClone = clone.indexerThreadPool;
    assertTrue(pool.getClass() == poolClone.getClass() && (pool != poolClone || pool.clone() == poolClone.clone()));

    MergePolicy mergePolicy = conf.mergePolicy;
    MergePolicy mergePolicyClone = clone.mergePolicy;
    assertTrue(mergePolicy.getClass() == mergePolicyClone.getClass() && (mergePolicy != mergePolicyClone || mergePolicy.clone() == mergePolicyClone.clone()));

    MergeScheduler mergeSched = conf.mergeScheduler;
    MergeScheduler mergeSchedClone = clone.mergeScheduler;
    assertTrue(mergeSched.getClass() == mergeSchedClone.getClass() && (mergeSched != mergeSchedClone || mergeSched.clone() == mergeSchedClone.clone()));

    conf.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(ConcurrentMergeScheduler.class, clone.getMergeScheduler().getClass());
  }

  @Test
  public void testInvalidValues() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

    // Test IndexDeletionPolicy
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(null));
    assertEquals(SnapshotDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    try {
      conf.setIndexDeletionPolicy(null);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }

    // Test MergeScheduler
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    conf.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(SerialMergeScheduler.class, conf.getMergeScheduler().getClass());
    try {
      conf.setMergeScheduler(null);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }

    // Test Similarity: 
    // we shouldnt assert what the default is, just that its not null.
    assertTrue(IndexSearcher.getDefaultSimilarity() == conf.getSimilarity());
    conf.setSimilarity(new MySimilarity());
    assertEquals(MySimilarity.class, conf.getSimilarity().getClass());
    try {
      conf.setSimilarity(null);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }

    // Test IndexingChain
    assertTrue(DocumentsWriterPerThread.defaultIndexingChain == conf.getIndexingChain());
    conf.setIndexingChain(new MyIndexingChain());
    assertEquals(MyIndexingChain.class, conf.getIndexingChain().getClass());
    try {
      conf.setIndexingChain(null);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }

    try {
      conf.setMaxBufferedDeleteTerms(0);
      fail("should not have succeeded to set maxBufferedDeleteTerms to 0");
    } catch (IllegalArgumentException e) {
      // this is expected
    }

    try {
      conf.setMaxBufferedDocs(1);
      fail("should not have succeeded to set maxBufferedDocs to 1");
    } catch (IllegalArgumentException e) {
      // this is expected
    }

    try {
      // Disable both MAX_BUF_DOCS and RAM_SIZE_MB
      conf.setMaxBufferedDocs(4);
      conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      conf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      fail("should not have succeeded to disable maxBufferedDocs when ramBufferSizeMB is disabled as well");
    } catch (IllegalArgumentException e) {
      // this is expected
    }

    conf.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    conf.setMaxBufferedDocs(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
    try {
      conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      fail("should not have succeeded to disable ramBufferSizeMB when maxBufferedDocs is disabled as well");
    } catch (IllegalArgumentException e) {
      // this is expected
    }

    // Test setReaderTermsIndexDivisor
    try {
      conf.setReaderTermsIndexDivisor(0);
      fail("should not have succeeded to set termsIndexDivisor to 0");
    } catch (IllegalArgumentException e) {
      // this is expected
    }
    
    // Setting to -1 is ok
    conf.setReaderTermsIndexDivisor(-1);
    try {
      conf.setReaderTermsIndexDivisor(-2);
      fail("should not have succeeded to set termsIndexDivisor to < -1");
    } catch (IllegalArgumentException e) {
      // this is expected
    }
    
    try {
      conf.setRAMPerThreadHardLimitMB(2048);
      fail("should not have succeeded to set RAMPerThreadHardLimitMB to >= 2048");
    } catch (IllegalArgumentException e) {
      // this is expected
    }
    
    try {
      conf.setRAMPerThreadHardLimitMB(0);
      fail("should not have succeeded to set RAMPerThreadHardLimitMB to 0");
    } catch (IllegalArgumentException e) {
      // this is expected
    }
    
    // Test MergePolicy
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    conf.setMergePolicy(new LogDocMergePolicy());
    assertEquals(LogDocMergePolicy.class, conf.getMergePolicy().getClass());
    try {
      conf.setMergePolicy(null);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }
  }

  public void testLiveChangeToCFS() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy(true));

    // Start false:
    ((LogMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false); 
    IndexWriter w = new IndexWriter(dir, iwc);

    // Change to true:
    LogMergePolicy lmp = ((LogMergePolicy) w.getConfig().getMergePolicy());
    lmp.setNoCFSRatio(1.0);
    lmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    lmp.setUseCompoundFile(true);

    Document doc = new Document();
    doc.add(newStringField("field", "foo", Store.NO));
    w.addDocument(doc);
    w.commit();

    for(String file : dir.listAll()) {
      // frq file should be stuck into CFS
      assertFalse(file.endsWith(".frq"));
    }
    w.close();
    dir.close();
  }

}
