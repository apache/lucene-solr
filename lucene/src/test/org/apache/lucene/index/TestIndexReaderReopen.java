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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexReaderReopen extends LuceneTestCase {

  // LUCENE-1228: IndexWriter.commit() does not update the index version
  // populate an index in iterations.
  // at the end of every iteration, commit the index and reopen/recreate the reader.
  // in each iteration verify the work of previous iteration. 
  // try this once with reopen once recreate, on both RAMDir and FSDir.
  public void testCommitReopen () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, true);
    dir.close();
  }
  public void testCommitRecreate () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, false);
    dir.close();
  }

  private void doTestReopenWithCommit (Random random, Directory dir, boolean withReopen) throws IOException {
    IndexWriter iwriter = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(
                                                              OpenMode.CREATE).setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(newLogMergePolicy()));
    iwriter.commit();
    IndexReader reader = IndexReader.open(dir, false);
    try {
      int M = 3;
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setTokenized(false);
      FieldType customType2 = new FieldType(TextField.TYPE_STORED);
      customType2.setTokenized(false);
      customType2.setOmitNorms(true);
      FieldType customType3 = new FieldType();
      customType3.setStored(true);
      for (int i=0; i<4; i++) {
        for (int j=0; j<M; j++) {
          Document doc = new Document();
          doc.add(newField("id", i+"_"+j, customType));
          doc.add(newField("id2", i+"_"+j, customType2));
          doc.add(newField("id3", i+"_"+j, customType3));
          iwriter.addDocument(doc);
          if (i>0) {
            int k = i-1;
            int n = j + k*M;
            Document prevItereationDoc = reader.document(n);
            assertNotNull(prevItereationDoc);
            String id = prevItereationDoc.get("id");
            assertEquals(k+"_"+j, id);
          }
        }
        iwriter.commit();
        if (withReopen) {
          // reopen
          IndexReader r2 = IndexReader.openIfChanged(reader);
          if (r2 != null) {
            reader.close();
            reader = r2;
          }
        } else {
          // recreate
          reader.close();
          reader = IndexReader.open(dir, false);
        }
      }
    } finally {
      iwriter.close();
      reader.close();
    }
  }
  
  public static void createIndex(Random random, Directory dir, boolean multiSegment) throws IOException {
    IndexWriter.unlock(dir);
    IndexWriter w = new IndexWriter(dir, LuceneTestCase.newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMergePolicy(new LogDocMergePolicy()));
    
    for (int i = 0; i < 100; i++) {
      w.addDocument(createDocument(i, 4));
      if (multiSegment && (i % 10) == 0) {
        w.commit();
      }
    }
    
    if (!multiSegment) {
      w.forceMerge(1);
    }
    
    w.close();

    IndexReader r = IndexReader.open(dir, false);
    if (multiSegment) {
      assertTrue(r.getSequentialSubReaders().length > 1);
    } else {
      assertTrue(r.getSequentialSubReaders().length == 1);
    }
    r.close();
  }

  public static Document createDocument(int n, int numFields) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    sb.append("a");
    sb.append(n);
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setTokenized(false);
    customType2.setOmitNorms(true);
    FieldType customType3 = new FieldType();
    customType3.setStored(true);
    doc.add(new Field("field1", sb.toString(), TextField.TYPE_STORED));
    doc.add(new Field("fielda", sb.toString(), customType2));
    doc.add(new Field("fieldb", sb.toString(), customType3));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i+1), sb.toString(), TextField.TYPE_STORED));
    }
    return doc;
  }

  private void assertReaderClosed(IndexReader reader, boolean checkSubReaders, boolean checkNormsClosed) {
    assertEquals(0, reader.getRefCount());
    
    if (checkNormsClosed && reader instanceof SegmentReader) {
      assertTrue(((SegmentReader) reader).normsClosed());
    }
    
    if (checkSubReaders) {
      if (reader instanceof DirectoryReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof MultiReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof ParallelReader) {
        IndexReader[] subReaders = ((ParallelReader) reader).getSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
    }
  }

  /*
  private void assertReaderOpen(IndexReader reader) {
    reader.ensureOpen();
    
    if (reader instanceof DirectoryReader) {
      IndexReader[] subReaders = reader.getSequentialSubReaders();
      for (int i = 0; i < subReaders.length; i++) {
        assertReaderOpen(subReaders[i]);
      }
    }
  }
  */

  private void assertRefCountEquals(int refCount, IndexReader reader) {
    assertEquals("Reader has wrong refCount value.", refCount, reader.getRefCount());
  }


  private abstract static class TestReopen {
    protected abstract IndexReader openReader() throws IOException;
  }
  
  public void testCloseOrig() throws Throwable {
    Directory dir = newDirectory();
    createIndex(random, dir, false);
    IndexReader r1 = IndexReader.open(dir, false);
    IndexReader r2 = IndexReader.open(dir, false);
    r2.deleteDocument(0);
    r2.close();

    IndexReader r3 = IndexReader.openIfChanged(r1);
    assertNotNull(r3);
    assertTrue(r1 != r3);
    r1.close();
    try {
      r1.document(2);
      fail("did not hit exception");
    } catch (AlreadyClosedException ace) {
      // expected
    }
    r3.close();
    dir.close();
  }

  private static class KeepAllCommits implements IndexDeletionPolicy {
    public void onInit(List<? extends IndexCommit> commits) {
    }
    public void onCommit(List<? extends IndexCommit> commits) {
    }
  }

  public void testReopenOnCommit() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setIndexDeletionPolicy(new KeepAllCommits()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(10))
    );
    for(int i=0;i<4;i++) {
      Document doc = new Document();
      doc.add(newField("id", ""+i, StringField.TYPE_UNSTORED));
      writer.addDocument(doc);
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", i+"");
      writer.commit(data);
    }
    for(int i=0;i<4;i++) {
      writer.deleteDocuments(new Term("id", ""+i));
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", (4+i)+"");
      writer.commit(data);
    }
    writer.close();

    IndexReader r = IndexReader.open(dir, false);
    assertEquals(0, r.numDocs());

    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit commit : commits) {
      IndexReader r2 = IndexReader.openIfChanged(r, commit);
      assertNotNull(r2);
      assertTrue(r2 != r);

      // Reader should be readOnly
      try {
        r2.deleteDocument(0);
        fail("no exception hit");
      } catch (UnsupportedOperationException uoe) {
        // expected
      }

      final Map<String,String> s = commit.getUserData();
      final int v;
      if (s.size() == 0) {
        // First commit created by IW
        v = -1;
      } else {
        v = Integer.parseInt(s.get("index"));
      }
      if (v < 4) {
        assertEquals(1+v, r2.numDocs());
      } else {
        assertEquals(7-v, r2.numDocs());
      }
      r.close();
      r = r2;
    }
    r.close();
    dir.close();
  }
  
  // LUCENE-1579: Make sure all SegmentReaders are new when
  // reopen switches readOnly
  public void testReopenChangeReadonly() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(10))
    );
    Document doc = new Document();
    doc.add(newField("number", "17", StringField.TYPE_UNSTORED));
    writer.addDocument(doc);
    writer.commit();

    // Open reader1
    IndexReader r = IndexReader.open(dir, false);
    assertTrue(r instanceof DirectoryReader);
    IndexReader r1 = getOnlySegmentReader(r);
    final int[] ints = FieldCache.DEFAULT.getInts(r1, "number", false);
    assertEquals(1, ints.length);
    assertEquals(17, ints[0]);

    // Reopen to readonly w/ no chnages
    IndexReader r3 = IndexReader.openIfChanged(r, true);
    assertNotNull(r3);
    assertTrue(((DirectoryReader) r3).readOnly);
    r3.close();

    // Add new segment
    writer.addDocument(doc);
    writer.commit();

    // Reopen reader1 --> reader2
    IndexReader r2 = IndexReader.openIfChanged(r, true);
    assertNotNull(r2);
    r.close();
    assertTrue(((DirectoryReader) r2).readOnly);
    IndexReader[] subs = r2.getSequentialSubReaders();
    final int[] ints2 = FieldCache.DEFAULT.getInts(subs[0], "number", false);
    r2.close();

    assertTrue(((SegmentReader) subs[0]).readOnly);
    assertTrue(((SegmentReader) subs[1]).readOnly);
    assertTrue(ints == ints2);

    writer.close();
    dir.close();
  }
}
