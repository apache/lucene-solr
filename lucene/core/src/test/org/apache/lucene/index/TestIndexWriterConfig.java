package org.apache.lucene.index;

/**
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.DocumentsWriter.IndexingChain;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestIndexWriterConfig extends LuceneTestCase {

  private static final class MySimilarity extends DefaultSimilarity {
    // Does not implement anything - used only for type checking on IndexWriterConfig.
  }
  
  private static final class MyIndexingChain extends IndexingChain {
    // Does not implement anything - used only for type checking on IndexWriterConfig.

    @Override
    DocConsumer getChain(DocumentsWriter documentsWriter) {
      return null;
    }
    
  }

  private static final class MyWarmer extends IndexReaderWarmer {
    // Does not implement anything - used only for type checking on IndexWriterConfig.

    @Override
    public void warm(IndexReader reader) throws IOException {
    }
    
  }
  
  @Test
  public void testDefaults() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    assertEquals(MockAnalyzer.class, conf.getAnalyzer().getClass());
    assertNull(conf.getIndexCommit());
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    assertEquals(OpenMode.CREATE_OR_APPEND, conf.getOpenMode());
    assertTrue(Similarity.getDefault() == conf.getSimilarity());
    assertEquals(IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL, conf.getTermIndexInterval());
    assertEquals(IndexWriterConfig.getDefaultWriteLockTimeout(), conf.getWriteLockTimeout());
    assertEquals(IndexWriterConfig.WRITE_LOCK_TIMEOUT, IndexWriterConfig.getDefaultWriteLockTimeout());
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS, conf.getMaxBufferedDeleteTerms());
    assertEquals(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, conf.getRAMBufferSizeMB(), 0.0);
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS, conf.getMaxBufferedDocs());
    assertEquals(IndexWriterConfig.DEFAULT_READER_POOLING, conf.getReaderPooling());
    assertTrue(DocumentsWriter.defaultIndexingChain == conf.getIndexingChain());
    assertNull(conf.getMergedSegmentWarmer());
    assertEquals(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, conf.getMaxThreadStates());
    assertEquals(IndexWriterConfig.DEFAULT_READER_TERMS_INDEX_DIVISOR, conf.getReaderTermsIndexDivisor());
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    
    // Sanity check - validate that all getters are covered.
    Set<String> getters = new HashSet<String>();
    getters.add("getAnalyzer");
    getters.add("getIndexCommit");
    getters.add("getIndexDeletionPolicy");
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
    getters.add("getReaderTermsIndexDivisor");
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getDeclaringClass() == IndexWriterConfig.class && m.getName().startsWith("get")) {
        assertTrue("method " + m.getName() + " is not tested for defaults", getters.contains(m.getName()));
      }
    }
  }

  @Test
  public void testSettersChaining() throws Exception {
    // Ensures that every setter returns IndexWriterConfig to enable easy
    // chaining.
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getDeclaringClass() == IndexWriterConfig.class
          && m.getName().startsWith("set")
          && !Modifier.isStatic(m.getModifiers())) {
        assertEquals("method " + m.getName() + " does not return IndexWriterConfig", 
            IndexWriterConfig.class, m.getReturnType());
      }
    }
  }
  
  @Test
  public void testConstants() throws Exception {
    // Tests that the values of the constants does not change
    assertEquals(1000, IndexWriterConfig.WRITE_LOCK_TIMEOUT);
    assertEquals(128, IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL);
    assertEquals(-1, IndexWriterConfig.DISABLE_AUTO_FLUSH);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
    assertEquals(16.0, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, 0.0);
    assertEquals(false, IndexWriterConfig.DEFAULT_READER_POOLING);
    assertEquals(8, IndexWriterConfig.DEFAULT_MAX_THREAD_STATES);
    assertEquals(IndexReader.DEFAULT_TERMS_INDEX_DIVISOR, IndexWriterConfig.DEFAULT_READER_TERMS_INDEX_DIVISOR);
  }
  
  @Test
  public void testToString() throws Exception {
    String str = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).toString();
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
      assertTrue(f.getName() + " not found in toString", str.indexOf(f.getName()) != -1);
    }
  }
  
  @Test
  public void testClone() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    IndexWriterConfig clone = (IndexWriterConfig) conf.clone();
    
    // Clone is shallow since not all parameters are cloneable.
    assertTrue(conf.getIndexDeletionPolicy() == clone.getIndexDeletionPolicy());
    
    conf.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(ConcurrentMergeScheduler.class, clone.getMergeScheduler().getClass());
  }

  @Test
  public void testInvalidValues() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    
    // Test IndexDeletionPolicy
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(null));
    assertEquals(SnapshotDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    conf.setIndexDeletionPolicy(null);
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    
    // Test MergeScheduler
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    conf.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(SerialMergeScheduler.class, conf.getMergeScheduler().getClass());
    conf.setMergeScheduler(null);
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());

    // Test Similarity
    assertTrue(Similarity.getDefault() == conf.getSimilarity());
    conf.setSimilarity(new MySimilarity());
    assertEquals(MySimilarity.class, conf.getSimilarity().getClass());
    conf.setSimilarity(null);
    assertTrue(Similarity.getDefault() == conf.getSimilarity());

    // Test IndexingChain
    assertTrue(DocumentsWriter.defaultIndexingChain == conf.getIndexingChain());
    conf.setIndexingChain(new MyIndexingChain());
    assertEquals(MyIndexingChain.class, conf.getIndexingChain().getClass());
    conf.setIndexingChain(null);
    assertTrue(DocumentsWriter.defaultIndexingChain == conf.getIndexingChain());
    
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
    
    assertEquals(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, conf.getMaxThreadStates());
    conf.setMaxThreadStates(5);
    assertEquals(5, conf.getMaxThreadStates());
    conf.setMaxThreadStates(0);
    assertEquals(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, conf.getMaxThreadStates());
    
    // Test MergePolicy
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    conf.setMergePolicy(new LogDocMergePolicy());
    assertEquals(LogDocMergePolicy.class, conf.getMergePolicy().getClass());
    conf.setMergePolicy(null);
    assertEquals(LogByteSizeMergePolicy.class, conf.getMergePolicy().getClass());
  }

  /**
   * @deprecated should be removed once all the deprecated setters are removed
   *             from IndexWriter.
   */
  @Test @Deprecated
  public void testIndexWriterSetters() throws Exception {
    // This test intentionally tests deprecated methods. The purpose is to pass
    // whatever the user set on IW to IWC, so that if the user calls
    // iw.getConfig().getXYZ(), he'll get the same value he passed to
    // iw.setXYZ().
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf);

    writer.setSimilarity(new MySimilarity());
    assertEquals(MySimilarity.class, writer.getConfig().getSimilarity().getClass());

    writer.setMaxBufferedDeleteTerms(4);
    assertEquals(4, writer.getConfig().getMaxBufferedDeleteTerms());

    writer.setMaxBufferedDocs(10);
    assertEquals(10, writer.getConfig().getMaxBufferedDocs());
    
    writer.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(SerialMergeScheduler.class, writer.getConfig().getMergeScheduler().getClass());
    
    writer.setRAMBufferSizeMB(1.5);
    assertEquals(1.5, writer.getConfig().getRAMBufferSizeMB(), 0.0);
    
    writer.setTermIndexInterval(40);
    assertEquals(40, writer.getConfig().getTermIndexInterval());
    
    writer.setWriteLockTimeout(100);
    assertEquals(100, writer.getConfig().getWriteLockTimeout());
    
    writer.setMergedSegmentWarmer(new MyWarmer());
    assertEquals(MyWarmer.class, writer.getConfig().getMergedSegmentWarmer().getClass());
    
    writer.setMergePolicy(new LogDocMergePolicy());
    assertEquals(LogDocMergePolicy.class, writer.getConfig().getMergePolicy().getClass());
    writer.close();
    dir.close();
  }

}
