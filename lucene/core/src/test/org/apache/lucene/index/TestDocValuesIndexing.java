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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 * 
 * Tests DocValues integration into IndexWriter
 * 
 */
@SuppressCodecs("Lucene3x")
public class TestDocValuesIndexing extends LuceneTestCase {
  /*
   * - add test for multi segment case with deletes
   * - add multithreaded tests / integrate into stress indexing?
   */
  
  public void testAddIndexes() throws IOException {
    Directory d1 = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d1);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv", 1));
    w.addDocument(doc);
    IndexReader r1 = w.getReader();
    w.close();

    Directory d2 = newDirectory();
    w = new RandomIndexWriter(random(), d2);
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(new NumericDocValuesField("dv", 2));
    w.addDocument(doc);
    IndexReader r2 = w.getReader();
    w.close();

    Directory d3 = newDirectory();
    w = new RandomIndexWriter(random(), d3);
    w.addIndexes(SlowCompositeReaderWrapper.wrap(r1), SlowCompositeReaderWrapper.wrap(r2));
    r1.close();
    d1.close();
    r2.close();
    d2.close();

    w.forceMerge(1);
    DirectoryReader r3 = w.getReader();
    w.close();
    AtomicReader sr = getOnlySegmentReader(r3);
    assertEquals(2, sr.numDocs());
    NumericDocValues docValues = sr.getNumericDocValues("dv");
    assertNotNull(docValues);
    r3.close();
    d3.close();
  }

  public void testMultiValuedDocValuesField() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    Field f = new NumericDocValuesField("field", 17);
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    doc.add(f);
    doc.add(f);
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    doc = new Document();
    doc.add(f);
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    w.close();
    assertEquals(17, FieldCache.DEFAULT.getInts(getOnlySegmentReader(r), "field", false).get(0));
    r.close();
    d.close();
  }

  public void testDifferentTypedDocValuesField() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    Field f;
    doc.add(f = new NumericDocValuesField("field", 17));
    doc.add(new BinaryDocValuesField("field", new BytesRef("blah")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    doc = new Document();
    doc.add(f);
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    w.close();
    assertEquals(17, FieldCache.DEFAULT.getInts(getOnlySegmentReader(r), "field", false).get(0));
    r.close();
    d.close();
  }

  public void testDifferentTypedDocValuesField2() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    Field f = new NumericDocValuesField("field", 17);
    doc.add(f);
    doc.add(new SortedDocValuesField("field", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    doc = new Document();
    doc.add(f);
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    assertEquals(17, getOnlySegmentReader(r).getNumericDocValues("field").get(0));
    r.close();
    w.close();
    d.close();
  }

  // LUCENE-3870
  public void testLengthPrefixAcrossTwoPages() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    byte[] bytes = new byte[32764];
    BytesRef b = new BytesRef();
    b.bytes = bytes;
    b.length = bytes.length;
    doc.add(new SortedDocValuesField("field", b));
    w.addDocument(doc);
    bytes[0] = 1;
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    BinaryDocValues s = FieldCache.DEFAULT.getTerms(getOnlySegmentReader(r), "field");

    BytesRef bytes1 = new BytesRef();
    s.get(0, bytes1);
    assertEquals(bytes.length, bytes1.length);
    bytes[0] = 0;
    assertEquals(b, bytes1);
    
    s.get(1, bytes1);
    assertEquals(bytes.length, bytes1.length);
    bytes[0] = 1;
    assertEquals(b, bytes1);
    r.close();
    w.close();
    d.close();
  }

  public void testDocValuesUnstored() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwconfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwconfig.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(dir, iwconfig);
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("dv", i));
      doc.add(new TextField("docId", "" + i, Field.Store.YES));
      writer.addDocument(doc);
    }
    DirectoryReader r = writer.getReader();
    SlowCompositeReaderWrapper slow = new SlowCompositeReaderWrapper(r);
    FieldInfos fi = slow.getFieldInfos();
    FieldInfo dvInfo = fi.fieldInfo("dv");
    assertTrue(dvInfo.hasDocValues());
    NumericDocValues dv = slow.getNumericDocValues("dv");
    for (int i = 0; i < 50; i++) {
      assertEquals(i, dv.get(i));
      Document d = slow.document(i);
      // cannot use d.get("dv") due to another bug!
      assertNull(d.getField("dv"));
      assertEquals(Integer.toString(i), d.get("docId"));
    }
    slow.close();
    writer.close();
    dir.close();
  }

  // Same field in one document as different types:
  public void testMixedTypesSameDocument() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  // Two documents with same field as different types:
  public void testMixedTypesDifferentDocuments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }
  
  public void testAddSortedTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    doc.add(new SortedDocValuesField("dv", new BytesRef("bar!")));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    
    iwriter.close();
    directory.close();
  }
  
  public void testAddBinaryTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("foo!")));
    doc.add(new BinaryDocValuesField("dv", new BytesRef("bar!")));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    
    iwriter.close();
    directory.close();
  }
  
  public void testAddNumericTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 1));
    doc.add(new NumericDocValuesField("dv", 2));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    
    iwriter.close();
    directory.close();
  }
  
  public void testTooLargeBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    byte bytes[] = new byte[100000];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.add(new BinaryDocValuesField("dv", b));
    try {
      iwriter.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    iwriter.close();

    directory.close();
  }
  
  public void testTooLargeSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    byte bytes[] = new byte[100000];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.add(new SortedDocValuesField("dv", b));
    try {
      iwriter.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    iwriter.close();
    directory.close();
  }
  
  public void testTooLargeTermSortedSetBytes() throws IOException {
    assumeTrue("codec does not support SORTED_SET", defaultCodecSupportsSortedSet());
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    byte bytes[] = new byte[100000];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.add(new SortedSetDocValuesField("dv", b));
    try {
      iwriter.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    iwriter.close();
    directory.close();
  }

  // Two documents across segments
  public void testMixedTypesDifferentSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  // Add inconsistent document after deleteAll
  public void testMixedTypesAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.deleteAll();

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Add inconsistent document after reopening IW w/ create
  public void testMixedTypesAfterReopenCreate() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Two documents with same field as different types, added
  // from separate threads:
  public void testMixedTypesDifferentThreads() throws Exception {
    Directory dir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    final CountDownLatch startingGun = new CountDownLatch(1);
    final AtomicBoolean hitExc = new AtomicBoolean();
    Thread[] threads = new Thread[3];
    for(int i=0;i<3;i++) {
      Field field;
      if (i == 0) {
        field = new SortedDocValuesField("foo", new BytesRef("hello"));
      } else if (i == 1) {
        field = new NumericDocValuesField("foo", 0);
      } else {
        field = new BinaryDocValuesField("foo", new BytesRef("bazz"));
      }
      final Document doc = new Document();
      doc.add(field);

      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              w.addDocument(doc);
            } catch (IllegalArgumentException iae) {
              // expected
              hitExc.set(true);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].start();
    }

    startingGun.countDown();

    for(Thread t : threads) {
      t.join();
    }
    assertTrue(hitExc.get());
    w.close();
    dir.close();
  }

  // Adding documents via addIndexes
  public void testMixedTypesViaAddIndexes() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);

    // Make 2nd index w/ inconsistent field
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    w2.addDocument(doc);
    w2.close();

    try {
      w.addIndexes(new Directory[] {dir2});
    } catch (IllegalArgumentException iae) {
      // expected
    }

    IndexReader r = DirectoryReader.open(dir2);
    try {
      w.addIndexes(new IndexReader[] {r});
    } catch (IllegalArgumentException iae) {
      // expected
    }

    r.close();
    dir2.close();
    w.close();
    dir.close();
  }

  public void testIllegalTypeChange() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    try {
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    dir.close();
  }

  public void testIllegalTypeChangeAcrossSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    writer = new IndexWriter(dir, conf);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    try {
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterCloseAndDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    writer = new IndexWriter(dir, conf);
    writer.deleteAll();
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.deleteAll();
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterCommitAndDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.commit();
    writer.deleteAll();
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterOpenCreate() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();
    conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    writer = new IndexWriter(dir, conf);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeViaAddIndexes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    writer = new IndexWriter(dir2, conf);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    try {
      writer.addIndexes(dir);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();

    dir.close();
    dir2.close();
  }

  public void testTypeChangeViaAddIndexesIR() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    writer = new IndexWriter(dir2, conf);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    IndexReader[] readers = new IndexReader[] {DirectoryReader.open(dir)};
    try {
      writer.addIndexes(readers);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    readers[0].close();
    writer.close();

    dir.close();
    dir2.close();
  }

  public void testTypeChangeViaAddIndexes2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    writer = new IndexWriter(dir2, conf);
    writer.addIndexes(dir);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    try {
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    dir2.close();
    dir.close();
  }

  public void testTypeChangeViaAddIndexesIR2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    writer = new IndexWriter(dir2, conf);
    IndexReader[] readers = new IndexReader[] {DirectoryReader.open(dir)};
    writer.addIndexes(readers);
    readers[0].close();
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    try {
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    dir2.close();
    dir.close();
  }

  public void testDocsWithField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new TextField("dv", "some text", Field.Store.NO));
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    
    DirectoryReader r = writer.getReader();
    writer.close();

    AtomicReader subR = r.leaves().get(0).reader();
    assertEquals(2, subR.numDocs());

    Bits bits = FieldCache.DEFAULT.getDocsWithField(subR, "dv");
    assertTrue(bits.get(0));
    assertTrue(bits.get(1));
    r.close();
    dir.close();
  }

}
