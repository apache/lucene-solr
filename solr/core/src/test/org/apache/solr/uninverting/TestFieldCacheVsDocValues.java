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
package org.apache.solr.uninverting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.index.SlowCompositeReaderWrapper;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestFieldCacheVsDocValues extends SolrTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("test unsupported on J9 temporarily, see https://issues.apache.org/jira/browse/LUCENE-6522",
                Constants.JAVA_VENDOR.startsWith("IBM"));
  }
  
  public void testByteMissingVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestMissingVsFieldCache(Byte.MIN_VALUE, Byte.MAX_VALUE);
    }
  }
  
  public void testShortMissingVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestMissingVsFieldCache(Short.MIN_VALUE, Short.MAX_VALUE);
    }
  }
  
  public void testIntMissingVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestMissingVsFieldCache(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
  }
  
  public void testLongMissingVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestMissingVsFieldCache(Long.MIN_VALUE, Long.MAX_VALUE);
    }
  }
  
  public void testSortedFixedLengthVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedVsFieldCache(fixedLength, fixedLength);
    }
  }
  
  public void testSortedVariableLengthVsFieldCache() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsFieldCache(1, 10);
    }
  }
  
  public void testSortedSetFixedLengthVsUninvertedField() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int fixedLength = TestUtil.nextInt(random(), 1, 10);
      doTestSortedSetVsUninvertedField(fixedLength, fixedLength);
    }
  }
  
  public void testSortedSetVariableLengthVsUninvertedField() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsUninvertedField(1, 10);
    }
  }
  
  // LUCENE-4853
  public void testHugeBinaryValues() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // FSDirectory because SimpleText will consume gobbs of
    // space when storing big binary values:
    try (Directory d = newFSDirectory(createTempDir("hugeBinaryValues"))) {
      boolean doFixed = random().nextBoolean();
      int numDocs;
      int fixedLength = 0;
      if (doFixed) {
        // Sometimes make all values fixed length since some
        // codecs have different code paths for this:
        numDocs = TestUtil.nextInt(random(), 10, 20);
        fixedLength = TestUtil.nextInt(random(), 65537, 256 * 1024);
      } else {
        numDocs = TestUtil.nextInt(random(), 100, 200);
      }
      try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig(analyzer))) {
        List<byte[]> docBytes = new ArrayList<>();
        long totalBytes = 0;
        for (int docID = 0; docID < numDocs; docID++) {
          // we don't use RandomIndexWriter because it might add
          // more docvalues than we expect !!!!

          // Must be > 64KB in size to ensure more than 2 pages in
          // PagedBytes would be needed:
          int numBytes;
          if (doFixed) {
            numBytes = fixedLength;
          } else if (docID == 0 || random().nextInt(5) == 3) {
            numBytes = TestUtil.nextInt(random(), 65537, 3 * 1024 * 1024);
          } else {
            numBytes = TestUtil.nextInt(random(), 1, 1024 * 1024);
          }
          totalBytes += numBytes;
          if (totalBytes > 5 * 1024 * 1024) {
            break;
          }
          byte[] bytes = new byte[numBytes];
          random().nextBytes(bytes);
          docBytes.add(bytes);
          Document doc = new Document();
          BytesRef b = new BytesRef(bytes);
          b.length = bytes.length;
          doc.add(new BinaryDocValuesField("field", b));
          doc.add(new StringField("id", "" + docID, Field.Store.YES));
          w.addDocument(doc);
        }


        DirectoryReader r = DirectoryReader.open(w);

        try (LeafReader ar = SlowCompositeReaderWrapper.wrap(r)) {
          TestUtil.checkReader(ar);

          BinaryDocValues s = FieldCache.DEFAULT.getTerms(ar, "field");
          for (int docID = 0; docID < docBytes.size(); docID++) {
            Document doc = ar.document(docID);
            assertEquals(docID, s.nextDoc());
            BytesRef bytes = s.binaryValue();
            byte[] expected = docBytes.get(Integer.parseInt(doc.get("id")));
            assertEquals(expected.length, bytes.length);
            assertEquals(new BytesRef(expected), bytes);
          }
        }
      }
    }
  }

  private void doTestSortedVsFieldCache(int minLength, int maxLength) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    Field indexedField = new StringField("indexed", "", Field.Store.NO);
    Field dvField = new SortedDocValuesField("dv", new BytesRef());
    doc.add(idField);
    doc.add(indexedField);
    doc.add(dvField);
    
    // index some docs
    int numDocs = atLeast(300);
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      final int length;
      if (minLength == maxLength) {
        length = minLength; // fixed length
      } else {
        length = TestUtil.nextInt(random(), minLength, maxLength);
      }
      String value = TestUtil.randomSimpleString(random(), length);
      indexedField.setStringValue(value);
      dvField.setBytesValue(new BytesRef(value));
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedDocValues expected = FieldCache.DEFAULT.getTermsIndex(r, "indexed");
      SortedDocValues actual = r.getSortedDocValues("dv");
      assertEquals(r.maxDoc(), expected, actual);
    }
    ir.close();
    dir.close();
  }
  
  private void doTestSortedSetVsUninvertedField(int minLength, int maxLength) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    
    // index some docs
    int numDocs = atLeast(300);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
      doc.add(idField);
      final int length = TestUtil.nextInt(random(), minLength, maxLength);
      int numValues = random().nextInt(17);
      // create a random list of strings
      List<String> values = new ArrayList<>();
      for (int v = 0; v < numValues; v++) {
        values.add(TestUtil.randomSimpleString(random(), minLength, length));
      }
      
      // add in any order to the indexed field
      ArrayList<String> unordered = new ArrayList<>(values);
      Collections.shuffle(unordered, random());
      for (String v : values) {
        doc.add(newStringField("indexed", v, Field.Store.NO));
      }

      // add in any order to the dv field
      ArrayList<String> unordered2 = new ArrayList<>(values);
      Collections.shuffle(unordered2, random());
      for (String v : unordered2) {
        doc.add(new SortedSetDocValuesField("dv", new BytesRef(v)));
      }

      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    
    // compare per-segment
    DirectoryReader ir = writer.getReader();
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedSetDocValues expected = FieldCache.DEFAULT.getDocTermOrds(r, "indexed", null);
      SortedSetDocValues actual = r.getSortedSetDocValues("dv");
      assertEquals(r.maxDoc(), expected, actual);
    }
    ir.close();
    
    writer.forceMerge(1);
    
    // now compare again after the merge
    ir = writer.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    SortedSetDocValues expected = FieldCache.DEFAULT.getDocTermOrds(ar, "indexed", null);
    SortedSetDocValues actual = ar.getSortedSetDocValues("dv");
    assertEquals(ir.maxDoc(), expected, actual);
    ir.close();
    
    writer.close();
    dir.close();
  }
  
  private void doTestMissingVsFieldCache(LongProducer longs) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    Field idField = new StringField("id", "", Field.Store.NO);
    Field indexedField = newStringField("indexed", "", Field.Store.NO);
    Field dvField = new NumericDocValuesField("dv", 0);

    
    // index some docs
    int numDocs = atLeast(300);
    // numDocs should be always > 256 so that in case of a codec that optimizes
    // for numbers of values <= 256, all storage layouts are tested
    assert numDocs > 256;
    for (int i = 0; i < numDocs; i++) {
      idField.setStringValue(Integer.toString(i));
      long value = longs.next();
      indexedField.setStringValue(Long.toString(value));
      dvField.setLongValue(value);
      Document doc = new Document();
      doc.add(idField);
      // 1/4 of the time we neglect to add the fields
      if (random().nextInt(4) > 0) {
        doc.add(indexedField);
        doc.add(dvField);
      }
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    
    // delete some docs
    int numDeletions = random().nextInt(numDocs/10);
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }

    // merge some segments and ensure that at least one of them has more than
    // 256 values
    writer.forceMerge(numDocs / 256);

    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      Bits expected = FieldCache.DEFAULT.getDocsWithField(r, "indexed", null);
      Bits actual = FieldCache.DEFAULT.getDocsWithField(r, "dv", null);
      assertEquals(expected, actual);
    }
    ir.close();
    dir.close();
  }
  
  private void doTestMissingVsFieldCache(final long minValue, final long maxValue) throws Exception {
    doTestMissingVsFieldCache(new LongProducer() {
      @Override
      long next() {
        return TestUtil.nextLong(random(), minValue, maxValue);
      }
    });
  }
  
  static abstract class LongProducer {
    abstract long next();
  }

  private void assertEquals(Bits expected, Bits actual) throws Exception {
    assertEquals(expected.length(), actual.length());
    for (int i = 0; i < expected.length(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  private void assertEquals(int maxDoc, SortedDocValues expected, SortedDocValues actual) throws Exception {
    // can be null for the segment if no docs actually had any SortedDocValues
    // in this case FC.getDocTermsOrds returns EMPTY
    if (actual == null) {
      assertEquals(expected.getValueCount(), 0);
      return;
    }
    assertEquals(expected.getValueCount(), actual.getValueCount());

    // compare ord lists
    while (true) {
      int docID = expected.nextDoc();
      if (docID == NO_MORE_DOCS) {
        assertEquals(NO_MORE_DOCS, actual.nextDoc());
        break;
      }
      assertEquals(docID, actual.nextDoc());
      assertEquals(expected.ordValue(), actual.ordValue());
      assertEquals(expected.binaryValue(), actual.binaryValue());
    }
    
    // compare ord dictionary
    for (long i = 0; i < expected.getValueCount(); i++) {
      final BytesRef expectedBytes = BytesRef.deepCopyOf(expected.lookupOrd((int) i));
      final BytesRef actualBytes = actual.lookupOrd((int) i);
      assertEquals(expectedBytes, actualBytes);
    }
    
    // compare termsenum
    assertEquals(expected.getValueCount(), expected.termsEnum(), actual.termsEnum());
  }
  
  private void assertEquals(int maxDoc, SortedSetDocValues expected, SortedSetDocValues actual) throws Exception {
    // can be null for the segment if no docs actually had any SortedDocValues
    // in this case FC.getDocTermsOrds returns EMPTY
    if (actual == null) {
      assertEquals(expected.getValueCount(), 0);
      return;
    }
    assertEquals(expected.getValueCount(), actual.getValueCount());
    while (true) {
      int docID = expected.nextDoc();
      assertEquals(docID, actual.nextDoc());
      if (docID == NO_MORE_DOCS) {
        break;
      }
      long expectedOrd;
      while ((expectedOrd = expected.nextOrd()) != NO_MORE_ORDS) {
        assertEquals(expectedOrd, actual.nextOrd());
      }
      assertEquals(NO_MORE_ORDS, actual.nextOrd());
    }
    
    // compare ord dictionary
    for (long i = 0; i < expected.getValueCount(); i++) {
      final BytesRef expectedBytes = BytesRef.deepCopyOf(expected.lookupOrd(i));
      final BytesRef actualBytes = actual.lookupOrd(i);
      assertEquals(expectedBytes, actualBytes);
    }
    
    // compare termsenum
    assertEquals(expected.getValueCount(), expected.termsEnum(), actual.termsEnum());
  }
  
  private void assertEquals(long numOrds, TermsEnum expected, TermsEnum actual) throws Exception {
    BytesRef ref;
    
    // sequential next() through all terms
    while ((ref = expected.next()) != null) {
      assertEquals(ref, actual.next());
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    assertNull(actual.next());
    
    // sequential seekExact(ord) through all terms
    for (long i = 0; i < numOrds; i++) {
      expected.seekExact(i);
      actual.seekExact(i);
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    
    // sequential seekExact(BytesRef) through all terms
    for (long i = 0; i < numOrds; i++) {
      expected.seekExact(i);
      assertTrue(actual.seekExact(expected.term()));
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    
    // sequential seekCeil(BytesRef) through all terms
    for (long i = 0; i < numOrds; i++) {
      expected.seekExact(i);
      assertEquals(SeekStatus.FOUND, actual.seekCeil(expected.term()));
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    
    // random seekExact(ord)
    for (long i = 0; i < numOrds; i++) {
      long randomOrd = TestUtil.nextLong(random(), 0, numOrds - 1);
      expected.seekExact(randomOrd);
      actual.seekExact(randomOrd);
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    
    // random seekExact(BytesRef)
    for (long i = 0; i < numOrds; i++) {
      long randomOrd = TestUtil.nextLong(random(), 0, numOrds - 1);
      expected.seekExact(randomOrd);
      actual.seekExact(expected.term());
      assertEquals(expected.ord(), actual.ord());
      assertEquals(expected.term(), actual.term());
    }
    
    // random seekCeil(BytesRef)
    for (long i = 0; i < numOrds; i++) {
      BytesRef target = new BytesRef(TestUtil.randomUnicodeString(random()));
      SeekStatus expectedStatus = expected.seekCeil(target);
      assertEquals(expectedStatus, actual.seekCeil(target));
      if (expectedStatus != SeekStatus.END) {
        assertEquals(expected.ord(), actual.ord());
        assertEquals(expected.term(), actual.term());
      }
    }
  }
}
