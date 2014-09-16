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

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
@TimeoutSuite(millis = 6 * TimeUnits.HOUR)
public class TestIndexWriterMaxDocs extends LuceneTestCase {

  @Monster("takes a long time")
  public void testExactlyAtTrueLimit() throws Exception {
    Directory dir = newFSDirectory(createTempDir("2BDocs3"));
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    doc.add(newStringField("field", "text", Field.Store.NO));
    for (int i = 0; i < IndexWriter.MAX_DOCS; i++) {
      iw.addDocument(doc);
      /*
      if (i%1000000 == 0) {
        System.out.println((i/1000000) + " M docs...");
      }
      */
    }
    iw.commit();

    // First unoptimized, then optimized:
    for(int i=0;i<2;i++) {
      DirectoryReader ir = DirectoryReader.open(dir);
      assertEquals(IndexWriter.MAX_DOCS, ir.maxDoc());
      assertEquals(IndexWriter.MAX_DOCS, ir.numDocs());
      IndexSearcher searcher = new IndexSearcher(ir);
      TopDocs hits = searcher.search(new TermQuery(new Term("field", "text")), 10);
      assertEquals(IndexWriter.MAX_DOCS, hits.totalHits);

      // Sort by docID reversed:
      hits = searcher.search(new TermQuery(new Term("field", "text")), null, 10, new Sort(new SortField(null, SortField.Type.DOC, true)));
      assertEquals(IndexWriter.MAX_DOCS, hits.totalHits);
      assertEquals(10, hits.scoreDocs.length);
      assertEquals(IndexWriter.MAX_DOCS-1, hits.scoreDocs[0].doc);
      ir.close();

      iw.forceMerge(1);
    }

    iw.close();
    dir.close();
  }

  public void testAddDocument() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.addDocument(new Document());
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  public void testAddDocuments() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.addDocuments(Collections.singletonList(new Document()));
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  public void testUpdateDocument() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.updateDocument(new Term("field", "foo"), new Document());
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  public void testUpdateDocuments() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.updateDocuments(new Term("field", "foo"), Collections.singletonList(new Document()));
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  public void testReclaimedDeletes() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        Document doc = new Document();
        doc.add(newStringField("id", ""+i, Field.Store.NO));
        w.addDocument(doc);
      }

      // Delete 5 of them:
      for(int i=0;i<5;i++) {
        w.deleteDocuments(new Term("id", ""+i));
      }

      w.forceMerge(1);

      assertEquals(5, w.maxDoc());

      // Add 5 more docs
      for(int i=0;i<5;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.addDocument(new Document());
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  // Tests that 100% deleted segments (which IW "specializes" by dropping entirely) are not mis-counted
  public void testReclaimedDeletesWholeSegments() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      IndexWriter w = new IndexWriter(dir, iwc);
      for(int i=0;i<10;i++) {
        Document doc = new Document();
        doc.add(newStringField("id", ""+i, Field.Store.NO));
        w.addDocument(doc);
        if (i % 2 == 0) {
          // Make a new segment every 2 docs:
          w.commit();
        }
      }

      // Delete 5 of them:
      for(int i=0;i<5;i++) {
        w.deleteDocuments(new Term("id", ""+i));
      }

      w.forceMerge(1);

      assertEquals(5, w.maxDoc());

      // Add 5 more docs
      for(int i=0;i<5;i++) {
        w.addDocument(new Document());
      }

      // 11th document should fail:
      try {
        w.addDocument(new Document());
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w.close();
      dir.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  public void testAddIndexes() throws Exception {
    setIndexWriterMaxDocs(10);
    try {
      Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      for(int i=0;i<10;i++) {
        w.addDocument(new Document());
      }
      w.close();

      Directory dir2 = newDirectory();
      IndexWriter w2 = new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
      w2.addDocument(new Document());
      try {
        w2.addIndexes(new Directory[] {dir});
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      assertEquals(1, w2.maxDoc());
      IndexReader ir = DirectoryReader.open(dir);
      try {
        w2.addIndexes(new IndexReader[] {ir});
        fail("didn't hit exception");
      } catch (IllegalStateException ise) {
        // expected
      }
      w2.close();
      ir.close();
      dir.close();
      dir2.close();
    } finally {
      restoreIndexWriterMaxDocs();
    }
  }

  // Make sure MultiReader lets you search exactly the limit number of docs:
  public void testMultiReaderExactLimit() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    for (int i = 0; i < 100000; i++) {
      w.addDocument(doc);
    }
    w.close();

    int remainder = IndexWriter.MAX_DOCS % 100000;
    Directory dir2 = newDirectory();
    w = new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    for (int i = 0; i < remainder; i++) {
      w.addDocument(doc);
    }
    w.close();

    int copies = IndexWriter.MAX_DOCS / 100000;

    DirectoryReader ir = DirectoryReader.open(dir);
    DirectoryReader ir2 = DirectoryReader.open(dir2);
    IndexReader subReaders[] = new IndexReader[copies+1];
    Arrays.fill(subReaders, ir);
    subReaders[subReaders.length-1] = ir2;

    MultiReader mr = new MultiReader(subReaders);
    assertEquals(IndexWriter.MAX_DOCS, mr.maxDoc());
    assertEquals(IndexWriter.MAX_DOCS, mr.numDocs());
    ir.close();
    ir2.close();
    dir.close();
    dir2.close();
  }

  // Make sure MultiReader is upset if you exceed the limit
  public void testMultiReaderBeyondLimit() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    for (int i = 0; i < 100000; i++) {
      w.addDocument(doc);
    }
    w.close();

    int remainder = IndexWriter.MAX_DOCS % 100000;

    // One too many:
    remainder++;

    Directory dir2 = newDirectory();
    w = new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    for (int i = 0; i < remainder; i++) {
      w.addDocument(doc);
    }
    w.close();

    int copies = IndexWriter.MAX_DOCS / 100000;

    DirectoryReader ir = DirectoryReader.open(dir);
    DirectoryReader ir2 = DirectoryReader.open(dir2);
    IndexReader subReaders[] = new IndexReader[copies+1];
    Arrays.fill(subReaders, ir);
    subReaders[subReaders.length-1] = ir2;

    try {
      new MultiReader(subReaders);
      fail("didn't hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    ir.close();
    ir2.close();
    dir.close();
    dir2.close();
  }

  public void testTooLargeMaxDocs() throws Exception {
    try {
      IndexWriter.setMaxDocs(Integer.MAX_VALUE);
      fail("didn't hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }
}
