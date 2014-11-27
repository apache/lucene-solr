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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

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
    Document2 doc = w.newDocument();
    doc.addUniqueAtom("id", "1");
    doc.addInt("dv", 1);
    w.addDocument(doc);
    IndexReader r1 = w.getReader();
    w.close();

    Directory d2 = newDirectory();
    w = new RandomIndexWriter(random(), d2);
    doc = w.newDocument();
    doc.addUniqueAtom("id", "2");
    doc.addInt("dv", 2);
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
    Document2 doc = w.newDocument();
    doc.addInt("field", 17);
    
    // add the doc
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    try {
      doc.addInt("field", 16);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("field=\"field\": this field is added more than once but is not multiValued", iae.getMessage());
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
    Document2 doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    try {
      doc.addBinary("field", new BytesRef("blah"));
      fail("didn't hit expected exception");
    } catch (IllegalStateException ise) {
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
    Document2 doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);
    
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    try {
      doc.addAtom("field", new BytesRef("hello"));
      fail("didn't hit expected exception");
    } catch (IllegalStateException ise) {
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
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("field", IndexOptions.NONE);

    Document2 doc = w.newDocument();
    byte[] bytes = new byte[32764];
    BytesRef b = new BytesRef();
    b.bytes = bytes;
    b.length = bytes.length;
    doc.addAtom("field", b);
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
      Document2 doc = writer.newDocument();
      doc.addInt("dv", i);
      doc.addLargeText("docId", "" + i);
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
      Document2 d = slow.document(i);
      // cannot use d.get("dv") due to another bug!
      // nocommit why is this here?
      // assertNull(d.getString("dv"));
      assertEquals(Integer.toString(i), d.getString("docId"));
    }
    slow.close();
    writer.close();
    dir.close();
  }

  // Same field in one document as different types:
  public void testMixedTypesSameDocument() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    w.addDocument(w.newDocument());
    
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    try {
      doc.addAtom("foo", new BytesRef("hello"));
      fail("didn't hit expected exception");
    } catch (IllegalStateException ise) {
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
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);

    doc = w.newDocument();
    try {
      doc.addAtom("foo", new BytesRef("hello"));
      fail("didn't hit expected exception");
    } catch (IllegalStateException ise) {
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
    Document2 doc = iwriter.newDocument();
    doc.addAtom("dv", new BytesRef("foo!"));
    iwriter.addDocument(doc);
    
    try {
      doc.addAtom("dv", new BytesRef("bar!"));
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
      assertEquals("field=\"dv\": this field is added more than once but is not multiValued", expected.getMessage());
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
    Document2 doc = iwriter.newDocument();
    doc.addBinary("dv", new BytesRef("foo!"));
    iwriter.addDocument(doc);
    
    try {
      doc.addBinary("dv", new BytesRef("bar!"));
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
      assertEquals("field=\"dv\": this field is added more than once but is not multiValued", expected.getMessage());
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
    Document2 doc = iwriter.newDocument();
    doc.addInt("dv", 1);
    iwriter.addDocument(doc);
    
    try {
      doc.addInt("dv", 2);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      assertEquals("field=\"dv\": this field is added more than once but is not multiValued", expected.getMessage());
      // EXPECTED
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
    Document2 doc = iwriter.newDocument();
    doc.addAtom("dv", new BytesRef("just fine"));
    iwriter.addDocument(doc);
    
    doc = iwriter.newDocument();
    byte bytes[] = new byte[100000];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.addAtom("dv", b);
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
    FieldTypes fieldTypes = iwriter.getFieldTypes();
    Document2 doc = iwriter.newDocument();
    fieldTypes.setMultiValued("dv");
    doc.addAtom("dv", new BytesRef("just fine"));
    iwriter.addDocument(doc);
    
    doc = iwriter.newDocument();
    byte bytes[] = new byte[100000];
    BytesRef b = new BytesRef(bytes);
    random().nextBytes(bytes);
    doc.addAtom("dv", b);
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
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);
    w.commit();

    doc = w.newDocument();
    try {
      doc.addAtom("foo", new BytesRef("hello"));
      fail("did not get expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    dir.close();
  }

  // Add inconsistent document after deleteAll
  public void testMixedTypesAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);
    w.deleteAll();

    doc = w.newDocument();
    doc.addAtom("foo", new BytesRef("hello"));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Add inconsistent document after reopening IW w/ create
  public void testMixedTypesAfterReopenCreate() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    w = new IndexWriter(dir, iwc);
    doc = w.newDocument();
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend1() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = w.newDocument();
    try {
      doc.addAtom("foo", new BytesRef("hello"));
      fail("did not get expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend2() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))) ;
    Document2 doc = w.newDocument();
    doc.addAtom("foo", new BytesRef("foo"));
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = w.newDocument();
    doc.addAtom("foo", "bar");
    try {
      doc.addBinary("foo", new BytesRef("foo"));
      fail("did not get expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.forceMerge(1);
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend3() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))) ;
    Document2 doc = w.newDocument();
    doc.addAtom("foo", new BytesRef("foo"));
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = w.newDocument();
    doc.addAtom("foo", "bar");
    try {
      doc.addBinary("foo", new BytesRef("foo"));
      fail("did not get expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    // Also add another document so there is a segment to write here:
    w.addDocument(w.newDocument());
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
      final int what = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              Document2 doc = w.newDocument();
              if (what == 0) {
                doc.addAtom("foo", new BytesRef("hello"));
              } else if (what == 1) {
                doc.addInt("foo", 0);
              } else {
                doc.addAtom("foo", new BytesRef("bazz"));
              }
            } catch (IllegalStateException ise) {
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
    Document2 doc = w.newDocument();
    doc.addInt("foo", 0);
    w.addDocument(doc);

    // Make 2nd index w/ inconsistent field
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = w2.newDocument();
    doc.addAtom("foo", new BytesRef("hello"));
    w2.addDocument(doc);
    w2.close();

    try {
      w.addIndexes(new Directory[] {dir2});
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    IndexReader r = DirectoryReader.open(dir2);
    try {
      w.addIndexes(new IndexReader[] {r});
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
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    doc = writer.newDocument();
    try {
      doc.addAtom("dv", new BytesRef("foo"));
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
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
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();

    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir, conf);
    doc = writer.newDocument();
    try {
      doc.addAtom("dv", new BytesRef("foo"));
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterCloseAndDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();

    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir, conf);
    writer.deleteAll();
    doc = writer.newDocument();
    doc.addAtom("dv", new BytesRef("foo"));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.deleteAll();
    doc = writer.newDocument();
    doc.addAtom("dv", new BytesRef("foo"));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterCommitAndDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.commit();
    writer.deleteAll();
    doc = writer.newDocument();
    doc.addAtom("dv", new BytesRef("foo"));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeAfterOpenCreate() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    writer = new IndexWriter(dir, conf);
    doc = writer.newDocument();
    doc.addAtom("dv", new BytesRef("foo"));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testTypeChangeViaAddIndexes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    doc = writer.newDocument();
    try {
      doc.addAtom("dv", new BytesRef("foo"));
      writer.addIndexes(dir);
      // nocommit must fix addIndexes to verify schema
      //fail("did not hit exception");
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
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    doc = writer.newDocument();
    doc.addAtom("dv", new BytesRef("foo"));
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
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    writer.addIndexes(dir);
    doc = writer.newDocument();
    try {
      doc.addAtom("dv", new BytesRef("foo"));
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
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
    Document2 doc = writer.newDocument();
    doc.addLong("dv", 0L);
    writer.addDocument(doc);

    doc = writer.newDocument();
    doc.addLargeText("text", "some text");
    doc.addLong("dv", 0L);
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
}
