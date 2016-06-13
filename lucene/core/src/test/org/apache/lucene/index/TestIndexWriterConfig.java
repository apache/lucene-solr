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
package org.apache.lucene.index;


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
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestIndexWriterConfig extends LuceneTestCase {

  private static final class MySimilarity extends ClassicSimilarity {
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
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    assertEquals(MockAnalyzer.class, conf.getAnalyzer().getClass());
    assertNull(conf.getIndexCommit());
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    assertEquals(OpenMode.CREATE_OR_APPEND, conf.getOpenMode());
    // we don't need to assert this, it should be unspecified
    assertTrue(IndexSearcher.getDefaultSimilarity() == conf.getSimilarity());
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS, conf.getMaxBufferedDeleteTerms());
    assertEquals(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, conf.getRAMBufferSizeMB(), 0.0);
    assertEquals(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS, conf.getMaxBufferedDocs());
    assertEquals(IndexWriterConfig.DEFAULT_READER_POOLING, conf.getReaderPooling());
    assertTrue(DocumentsWriterPerThread.defaultIndexingChain == conf.getIndexingChain());
    assertNull(conf.getMergedSegmentWarmer());
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    assertEquals(DocumentsWriterPerThreadPool.class, conf.getIndexerThreadPool().getClass());
    assertEquals(FlushByRamOrCountsPolicy.class, conf.getFlushPolicy().getClass());
    assertEquals(IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB, conf.getRAMPerThreadHardLimitMB());
    assertEquals(Codec.getDefault(), conf.getCodec());
    assertEquals(InfoStream.getDefault(), conf.getInfoStream());
    assertEquals(IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM, conf.getUseCompoundFile());
    // Sanity check - validate that all getters are covered.
    Set<String> getters = new HashSet<>();
    getters.add("getAnalyzer");
    getters.add("getIndexCommit");
    getters.add("getIndexDeletionPolicy");
    getters.add("getMaxFieldLength");
    getters.add("getMergeScheduler");
    getters.add("getOpenMode");
    getters.add("getSimilarity");
    getters.add("getWriteLockTimeout");
    getters.add("getDefaultWriteLockTimeout");
    getters.add("getMaxBufferedDeleteTerms");
    getters.add("getRAMBufferSizeMB");
    getters.add("getMaxBufferedDocs");
    getters.add("getIndexingChain");
    getters.add("getMergedSegmentWarmer");
    getters.add("getMergePolicy");
    getters.add("getReaderPooling");
    getters.add("getIndexerThreadPool");
    getters.add("getFlushPolicy");
    getters.add("getRAMPerThreadHardLimitMB");
    getters.add("getCodec");
    getters.add("getInfoStream");
    getters.add("getUseCompoundFile");
    
    for (Method m : IndexWriterConfig.class.getDeclaredMethods()) {
      if (m.getDeclaringClass() == IndexWriterConfig.class && m.getName().startsWith("get")) {
        assertTrue("method " + m.getName() + " is not tested for defaults", getters.contains(m.getName()));
      }
    }
  }

  @Test
  public void testSettersChaining() throws Exception {
    // Ensures that every setter returns IndexWriterConfig to allow chaining.
    HashSet<String> liveSetters = new HashSet<>();
    HashSet<String> allSetters = new HashSet<>();
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
    // test that IWC cannot be reused across two IWs
    IndexWriterConfig conf = newIndexWriterConfig(null);
    new RandomIndexWriter(random(), dir, conf).close();

    // this should fail
    expectThrows(IllegalStateException.class, () -> {
      assertNotNull(new RandomIndexWriter(random(), dir, conf));
    });

    dir.close();
  }
  
  @Test
  public void testOverrideGetters() throws Exception {
    // Test that IndexWriterConfig overrides all getters, so that javadocs
    // contain all methods for the users. Also, ensures that IndexWriterConfig
    // doesn't declare getters that are not declared on LiveIWC.
    HashSet<String> liveGetters = new HashSet<>();
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
    assertEquals(-1, IndexWriterConfig.DISABLE_AUTO_FLUSH);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS);
    assertEquals(IndexWriterConfig.DISABLE_AUTO_FLUSH, IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
    assertEquals(16.0, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB, 0.0);
    assertEquals(false, IndexWriterConfig.DEFAULT_READER_POOLING);
    assertEquals(true, IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM);
  }

  @Test
  public void testToString() throws Exception {
    String str = new IndexWriterConfig(new MockAnalyzer(random())).toString();
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
  public void testInvalidValues() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));

    // Test IndexDeletionPolicy
    assertEquals(KeepOnlyLastCommitDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(null));
    assertEquals(SnapshotDeletionPolicy.class, conf.getIndexDeletionPolicy().getClass());
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setIndexDeletionPolicy(null);
    });

    // Test MergeScheduler
    assertEquals(ConcurrentMergeScheduler.class, conf.getMergeScheduler().getClass());
    conf.setMergeScheduler(new SerialMergeScheduler());
    assertEquals(SerialMergeScheduler.class, conf.getMergeScheduler().getClass());
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setMergeScheduler(null);
    });

    // Test Similarity: 
    // we shouldnt assert what the default is, just that it's not null.
    assertTrue(IndexSearcher.getDefaultSimilarity() == conf.getSimilarity());
    conf.setSimilarity(new MySimilarity());
    assertEquals(MySimilarity.class, conf.getSimilarity().getClass());
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setSimilarity(null);
    });

    // Test IndexingChain
    assertTrue(DocumentsWriterPerThread.defaultIndexingChain == conf.getIndexingChain());

    expectThrows(IllegalArgumentException.class, () -> {
      conf.setMaxBufferedDeleteTerms(0);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      conf.setMaxBufferedDocs(1);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      // Disable both MAX_BUF_DOCS and RAM_SIZE_MB
      conf.setMaxBufferedDocs(4);
      conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      conf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    });

    conf.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    conf.setMaxBufferedDocs(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setRAMPerThreadHardLimitMB(2048);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setRAMPerThreadHardLimitMB(0);
    });
    
    // Test MergePolicy
    assertEquals(TieredMergePolicy.class, conf.getMergePolicy().getClass());
    conf.setMergePolicy(new LogDocMergePolicy());
    assertEquals(LogDocMergePolicy.class, conf.getMergePolicy().getClass());
    expectThrows(IllegalArgumentException.class, () -> {
      conf.setMergePolicy(null);
    });
  }

  public void testLiveChangeToCFS() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy(true));
    // Start false:
    iwc.setUseCompoundFile(false); 
    iwc.getMergePolicy().setNoCFSRatio(0.0d);
    IndexWriter w = new IndexWriter(dir, iwc);
    // Change to true:
    w.getConfig().setUseCompoundFile(true);

    Document doc = new Document();
    doc.add(newStringField("field", "foo", Store.NO));
    w.addDocument(doc);
    w.commit();
    assertTrue("Expected CFS after commit", w.newestSegment().info.getUseCompoundFile());
    
    doc.add(newStringField("field", "foo", Store.NO));
    w.addDocument(doc);
    w.commit();
    w.forceMerge(1);
    w.commit();
   
    // no compound files after merge
    assertFalse("Expected Non-CFS after merge", w.newestSegment().info.getUseCompoundFile());
    
    MergePolicy lmp = w.getConfig().getMergePolicy();
    lmp.setNoCFSRatio(1.0);
    lmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    
    w.addDocument(doc);
    w.forceMerge(1);
    w.commit();
    assertTrue("Expected CFS after merge", w.newestSegment().info.getUseCompoundFile());
    w.close();
    dir.close();
  }

}
