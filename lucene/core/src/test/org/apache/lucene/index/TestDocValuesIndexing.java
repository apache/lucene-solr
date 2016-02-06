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


import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * 
 * Tests DocValues integration into IndexWriter
 * 
 */
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
    w.addIndexes(SlowCodecReaderWrapper.wrap(SlowCompositeReaderWrapper.wrap(r1)), SlowCodecReaderWrapper.wrap(SlowCompositeReaderWrapper.wrap(r2)));
    r1.close();
    d1.close();
    r2.close();
    d2.close();

    w.forceMerge(1);
    DirectoryReader r3 = w.getReader();
    w.close();
    LeafReader sr = getOnlySegmentReader(r3);
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
    doc.add(f);
    
    // add the doc
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    doc.add(f);
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    DirectoryReader r = w.getReader();
    w.close();
    assertEquals(17, DocValues.getNumeric(getOnlySegmentReader(r), "field").get(0));
    r.close();
    d.close();
  }

  public void testDifferentTypedDocValuesField() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", 17));
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    doc.add(new BinaryDocValuesField("field", new BytesRef("blah")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    DirectoryReader r = w.getReader();
    w.close();
    assertEquals(17, DocValues.getNumeric(getOnlySegmentReader(r), "field").get(0));
    r.close();
    d.close();
  }

  public void testDifferentTypedDocValuesField2() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", 17));
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    doc.add(new SortedDocValuesField("field", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    DirectoryReader r = w.getReader();
    assertEquals(17, getOnlySegmentReader(r).getNumericDocValues("field").get(0));
    r.close();
    w.close();
    d.close();
  }

  // LUCENE-3870
  public void testLengthPrefixAcrossTwoPages() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
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
    BinaryDocValues s = DocValues.getSorted(getOnlySegmentReader(r), "field");

    BytesRef bytes1 = s.get(0);
    assertEquals(bytes.length, bytes1.length);
    bytes[0] = 0;
    assertEquals(b, bytes1);
    
    bytes1 = s.get(1);
    assertEquals(bytes.length, bytes1.length);
    bytes[0] = 1;
    assertEquals(b, bytes1);
    r.close();
    w.close();
    d.close();
  }

  public void testDocValuesUnstored() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwconfig = newIndexWriterConfig(new MockAnalyzer(random()));
    iwconfig.setMergePolicy(newLogMergePolicy());
    IndexWriter writer = new IndexWriter(dir, iwconfig);
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("dv", i));
      doc.add(new TextField("docId", "" + i, Field.Store.YES));
      writer.addDocument(doc);
    }
    DirectoryReader r = writer.getReader();
    LeafReader slow = SlowCompositeReaderWrapper.wrap(r);
    FieldInfos fi = slow.getFieldInfos();
    FieldInfo dvInfo = fi.fieldInfo("dv");
    assertTrue(dvInfo.getDocValuesType() != DocValuesType.NONE);
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    w.addDocument(new Document());
    
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IndexReader ir = w.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    w.close();
    dir.close();
  }

  // Two documents with same field as different types:
  public void testMixedTypesDifferentDocuments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IndexReader ir = w.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    w.close();
    dir.close();
  }
  
  public void testAddSortedTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    iwriter.addDocument(doc);
    
    doc.add(new SortedDocValuesField("dv", new BytesRef("bar!")));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
      if (VERBOSE) {
        System.out.println("hit exc:");
        expected.printStackTrace(System.out);
      }
    }
    IndexReader ir = iwriter.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    iwriter.close();
    directory.close();
  }
  
  public void testAddBinaryTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("dv", new BytesRef("foo!")));
    iwriter.addDocument(doc);
    
    doc.add(new BinaryDocValuesField("dv", new BytesRef("bar!")));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    
    IndexReader ir = iwriter.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    
    iwriter.close();
    directory.close();
  }
  
  public void testAddNumericTwice() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 1));
    iwriter.addDocument(doc);
    
    doc.add(new NumericDocValuesField("dv", 2));
    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    IndexReader ir = iwriter.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    iwriter.close();
    directory.close();
  }

  public void testTooLargeSortedBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("just fine")));
    iwriter.addDocument(doc);
    
    doc = new Document();
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
    IndexReader ir = iwriter.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    iwriter.close();
    directory.close();
  }
  
  public void testTooLargeTermSortedSetBytes() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!1
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("dv", new BytesRef("just fine")));
    iwriter.addDocument(doc);
    
    doc = new Document();
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
    IndexReader ir = iwriter.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    iwriter.close();
    directory.close();
  }

  // Two documents across segments
  public void testMixedTypesDifferentSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  // Add inconsistent document after deleteAll
  public void testMixedTypesAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend1() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    try {
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend2() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))) ;
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("foo")));
    w.addDocument(doc);
    w.close();

    doc = new Document();
    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    doc.add(new BinaryDocValuesField("foo", new BytesRef("foo")));
    try {
      // NOTE: this case follows a different code path inside
      // DefaultIndexingChain/FieldInfos, because the field (foo)
      // is first added without DocValues:
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.forceMerge(1);
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend3() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))) ;
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("foo")));
    w.addDocument(doc);
    w.close();

    doc = new Document();
    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    doc.add(new BinaryDocValuesField("foo", new BytesRef("foo")));
    try {
      // NOTE: this case follows a different code path inside
      // DefaultIndexingChain/FieldInfos, because the field (foo)
      // is first added without DocValues:
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    // Also add another document so there is a segment to write here:
    w.addDocument(new Document());
    w.forceMerge(1);
    w.close();
    dir.close();
  }

  // Two documents with same field as different types, added
  // from separate threads:
  public void testMixedTypesDifferentThreads() throws Exception {
    Directory dir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 0));
    w.addDocument(doc);

    // Make 2nd index w/ inconsistent field
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("hello")));
    w2.addDocument(doc);
    w2.close();

    try {
      w.addIndexes(new Directory[] {dir2});
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    DirectoryReader r = DirectoryReader.open(dir2);
    try {
      TestUtil.addIndexesSlowly(w, r);
      fail("didn't hit expected exception");
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexReader ir = writer.getReader();
    assertEquals(1, ir.numDocs());
    ir.close();
    writer.close();
    dir.close();
  }

  public void testIllegalTypeChangeAcrossSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo")));
    writer.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(writer, reader);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    reader.close();
    writer.close();

    dir.close();
    dir2.close();
  }

  public void testTypeChangeViaAddIndexes2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv", 0L));
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    DirectoryReader reader = DirectoryReader.open(dir);
    TestUtil.addIndexesSlowly(writer, reader);
    reader.close();
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
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

    LeafReader subR = r.leaves().get(0).reader();
    assertEquals(2, subR.numDocs());

    Bits bits = DocValues.getDocsWithField(subR, "dv");
    assertTrue(bits.get(0));
    assertTrue(bits.get(1));
    r.close();
    dir.close();
  }

  public void testSameFieldNameForPostingAndDocValue() throws Exception {
    // LUCENE-5192: FieldInfos.Builder neglected to update
    // globalFieldNumbers.docValuesType map if the field existed, resulting in
    // potentially adding the same field with different DV types.
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("f", "mock-value", Store.NO));
    doc.add(new NumericDocValuesField("f", 5));
    writer.addDocument(doc);
    writer.commit();
    
    doc = new Document();
    doc.add(new BinaryDocValuesField("f", new BytesRef("mock")));
    try {
      writer.addDocument(doc);
      fail("should not have succeeded to add a field with different DV type than what already exists");
    } catch (IllegalArgumentException e) {
      writer.rollback();
    }
    
    dir.close();
  }

  // LUCENE-6049
  public void testExcIndexingDocBeforeDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setDocValuesType(DocValuesType.SORTED);
    ft.freeze();
    Field field = new Field("test", "value", ft);
    field.setTokenStream(new TokenStream() {
        @Override
        public boolean incrementToken() {
          throw new RuntimeException("no");
        }
      });
    doc.add(field);
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (RuntimeException re) {
      // expected
    }
    w.addDocument(new Document());
    w.close();
    dir.close();
  }
}
