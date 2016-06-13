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
package org.apache.lucene.codecs.lucene54;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesProducer.SparseBits;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesProducer.SparseLongValues;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BaseCompressingDocValuesFormatTestCase;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.TestUtil;

/**
 * Tests Lucene54DocValuesFormat
 */
public class TestLucene54DocValuesFormat extends BaseCompressingDocValuesFormatTestCase {
  private final Codec codec = TestUtil.alwaysDocValuesFormat(new Lucene54DocValuesFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }
  
  // TODO: these big methods can easily blow up some of the other ram-hungry codecs...
  // for now just keep them here, as we want to test this for this format.
  
  @Slow
  public void testSortedSetVariableLengthBigVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(atLeast(300), 1, 32766, 16, 100);
    }
  }
  
  @Nightly
  public void testSortedSetVariableLengthManyVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedSetVsStoredFields(TestUtil.nextInt(random(), 1024, 2049), 1, 500, 16, 100);
    }
  }
  
  @Slow
  public void testSortedVariableLengthBigVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsStoredFields(atLeast(300), 1, 32766);
    }
  }
  
  @Nightly
  public void testSortedVariableLengthManyVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsStoredFields(TestUtil.nextInt(random(), 1024, 2049), 1, 500);
    }
  }
  
  @Slow
  public void testTermsEnumFixedWidth() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 5121), 10, 10);
    }
  }
  
  @Slow
  public void testTermsEnumVariableWidth() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 5121), 1, 500);
    }
  }
  
  @Nightly
  public void testTermsEnumRandomMany() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 8121), 1, 500);
    }
  }

  @Slow
  public void testSparseDocValuesVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSparseDocValuesVsStoredFields();
    }
  }

  private void doTestSparseDocValuesVsStoredFields() throws Exception {
    final long[] values = new long[TestUtil.nextInt(random(), 1, 500)];
    for (int i = 0; i < values.length; ++i) {
      values[i] = random().nextLong();
    }

    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

    // sparse compression is only enabled if less than 1% of docs have a value
    final int avgGap = 100;

    final int numDocs = atLeast(200);
    for (int i = random().nextInt(avgGap * 2); i >= 0; --i) {
      writer.addDocument(new Document());
    }
    final int maxNumValuesPerDoc = random().nextBoolean() ? 1 : TestUtil.nextInt(random(), 2, 5);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();

      // single-valued
      long docValue = values[random().nextInt(values.length)];
      doc.add(new NumericDocValuesField("numeric", docValue));
      doc.add(new SortedDocValuesField("sorted", new BytesRef(Long.toString(docValue))));
      doc.add(new BinaryDocValuesField("binary", new BytesRef(Long.toString(docValue))));
      doc.add(new StoredField("value", docValue));

      // multi-valued
      final int numValues = TestUtil.nextInt(random(), 1, maxNumValuesPerDoc);
      for (int j = 0; j < numValues; ++j) {
        docValue = values[random().nextInt(values.length)];
        doc.add(new SortedNumericDocValuesField("sorted_numeric", docValue));
        doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef(Long.toString(docValue))));
        doc.add(new StoredField("values", docValue));
      }

      writer.addDocument(doc);

      // add a gap
      for (int j = TestUtil.nextInt(random(), 0, avgGap * 2); j >= 0; --j) {
        writer.addDocument(new Document());
      }
    }

    if (random().nextBoolean()) {
      writer.forceMerge(1);
    }

    final IndexReader indexReader = writer.getReader();
    writer.close();

    for (LeafReaderContext context : indexReader.leaves()) {
      final LeafReader reader = context.reader();
      final NumericDocValues numeric = DocValues.getNumeric(reader, "numeric");
      final Bits numericBits = DocValues.getDocsWithField(reader, "numeric");

      final SortedDocValues sorted = DocValues.getSorted(reader, "sorted");
      final Bits sortedBits = DocValues.getDocsWithField(reader, "sorted");

      final BinaryDocValues binary = DocValues.getBinary(reader, "binary");
      final Bits binaryBits = DocValues.getDocsWithField(reader, "binary");

      final SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(reader, "sorted_numeric");
      final Bits sortedNumericBits = DocValues.getDocsWithField(reader, "sorted_numeric");

      final SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, "sorted_set");
      final Bits sortedSetBits = DocValues.getDocsWithField(reader, "sorted_set");

      for (int i = 0; i < reader.maxDoc(); ++i) {
        final Document doc = reader.document(i);
        final IndexableField valueField = doc.getField("value");
        final Long value = valueField == null ? null : valueField.numericValue().longValue();

        if (value == null) {
          assertEquals(0, numeric.get(i));
          assertEquals(-1, sorted.getOrd(i));
          assertEquals(new BytesRef(), binary.get(i));

          assertFalse(numericBits.get(i));
          assertFalse(sortedBits.get(i));
          assertFalse(binaryBits.get(i));
        } else {
          assertEquals(value.longValue(), numeric.get(i));
          assertTrue(sorted.getOrd(i) >= 0);
          assertEquals(new BytesRef(Long.toString(value)), sorted.lookupOrd(sorted.getOrd(i)));
          assertEquals(new BytesRef(Long.toString(value)), binary.get(i));

          assertTrue(numericBits.get(i));
          assertTrue(sortedBits.get(i));
          assertTrue(binaryBits.get(i));
        }

        final IndexableField[] valuesFields = doc.getFields("values");
        final Set<Long> valueSet = new HashSet<>();
        for (IndexableField sf : valuesFields) {
          valueSet.add(sf.numericValue().longValue());
        }

        sortedNumeric.setDocument(i);
        assertEquals(valuesFields.length, sortedNumeric.count());
        for (int j = 0; j < sortedNumeric.count(); ++j) {
          assertTrue(valueSet.contains(sortedNumeric.valueAt(j)));
        }
        sortedSet.setDocument(i);
        int sortedSetCount = 0;
        while (true) {
          long ord = sortedSet.nextOrd();
          if (ord == SortedSetDocValues.NO_MORE_ORDS) {
            break;
          }
          assertTrue(valueSet.contains(Long.parseLong(sortedSet.lookupOrd(ord).utf8ToString())));
          sortedSetCount++;
        }
        assertEquals(valueSet.size(), sortedSetCount);

        assertEquals(!valueSet.isEmpty(), sortedNumericBits.get(i));
        assertEquals(!valueSet.isEmpty(), sortedSetBits.get(i));
      }
    }

    indexReader.close();
    dir.close();
  }

  // TODO: try to refactor this and some termsenum tests into the base class.
  // to do this we need to fix the test class to get a DVF not a Codec so we can setup
  // the postings format correctly.
  private void doTestTermsEnumRandom(int numDocs, int minLength, int maxLength) throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergeScheduler(new SerialMergeScheduler());
    // set to duel against a codec which has ordinals:
    final PostingsFormat pf = TestUtil.getPostingsFormatWithOrds(random());
    final DocValuesFormat dv = new Lucene54DocValuesFormat();
    conf.setCodec(new AssertingCodec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return pf;
      }

      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return dv;
      }
    });
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    
    // index some docs
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
      Terms terms = r.terms("indexed");
      if (terms != null) {
        SortedSetDocValues ssdv = r.getSortedSetDocValues("dv");
        assertEquals(terms.size(), ssdv.getValueCount());
        TermsEnum expected = terms.iterator();
        TermsEnum actual = r.getSortedSetDocValues("dv").termsEnum();
        assertEquals(terms.size(), expected, actual);

        doTestSortedSetEnumAdvanceIndependently(ssdv);
      }
    }
    ir.close();
    
    writer.forceMerge(1);
    
    // now compare again after the merge
    ir = writer.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    Terms terms = ar.terms("indexed");
    if (terms != null) {
      assertEquals(terms.size(), ar.getSortedSetDocValues("dv").getValueCount());
      TermsEnum expected = terms.iterator();
      TermsEnum actual = ar.getSortedSetDocValues("dv").termsEnum();
      assertEquals(terms.size(), expected, actual);
    }
    ir.close();
    
    writer.close();
    dir.close();
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

  public void testSparseLongValues() {
    final int iters = atLeast(5);
    for (int iter = 0; iter < iters; ++iter) {
      final int numDocs = TestUtil.nextInt(random(), 0, 100);
      final long[] docIds = new long[numDocs];
      final long[] values = new long[numDocs];
      final long maxDoc;
      if (numDocs == 0) {
        maxDoc = 1 + random().nextInt(10);
      } else {
        docIds[0] = random().nextInt(10);
        for (int i = 1; i < docIds.length; ++i) {
          docIds[i] = docIds[i - 1] + 1 + random().nextInt(100);
        }
        maxDoc = docIds[numDocs - 1] + 1 + random().nextInt(10);
      }
      for (int i = 0; i < values.length; ++i) {
        values[i] = random().nextLong();
      }
      final long missingValue = random().nextLong();
      final LongValues docIdsValues = new LongValues() {
        @Override
        public long get(long index) {
          return docIds[Math.toIntExact(index)];
        }
      };
      final LongValues valuesValues = new LongValues() {
        @Override
        public long get(long index) {
          return values[Math.toIntExact(index)];
        }
      };
      final SparseBits liveBits = new SparseBits(maxDoc, numDocs, docIdsValues);
      // random-access
      for (int i = 0; i < 2000; ++i) {
        final long docId = TestUtil.nextLong(random(), 0, maxDoc - 1);
        final boolean exists = liveBits.get(Math.toIntExact(docId));
        assertEquals(Arrays.binarySearch(docIds, docId) >= 0, exists);
      }
      // sequential access
      for (int docId = 0; docId < maxDoc; docId += random().nextInt(3)) {
        final boolean exists = liveBits.get(Math.toIntExact(docId));
        assertEquals(Arrays.binarySearch(docIds, docId) >= 0, exists);
      }

      final SparseLongValues sparseValues = new SparseLongValues(liveBits, valuesValues, missingValue);
      // random-access
      for (int i = 0; i < 2000; ++i) {
        final long docId = TestUtil.nextLong(random(), 0, maxDoc - 1);
        final int idx = Arrays.binarySearch(docIds, docId);
        final long value = sparseValues.get(docId);
        if (idx >= 0) {
          assertEquals(values[idx], value);
        } else {
          assertEquals(missingValue, value);
        }
      }
      // sequential access
      for (int docId = 0; docId < maxDoc; docId += random().nextInt(3)) {
        final int idx = Arrays.binarySearch(docIds, docId);
        final long value = sparseValues.get(docId);
        if (idx >= 0) {
          assertEquals(values[idx], value);
        } else {
          assertEquals(missingValue, value);
        }
      }
    }
  }

  @Slow
  public void testSortedSetAroundBlockSize() throws IOException {
    final int frontier = 1 << Lucene54DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
    for (int maxDoc = frontier - 1; maxDoc <= frontier + 1; ++maxDoc) {
      final Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
      RAMFile buffer = new RAMFile();
      RAMOutputStream out = new RAMOutputStream(buffer, false);
      Document doc = new Document();
      SortedSetDocValuesField field1 = new SortedSetDocValuesField("sset", new BytesRef());
      doc.add(field1);
      SortedSetDocValuesField field2 = new SortedSetDocValuesField("sset", new BytesRef());
      doc.add(field2);
      for (int i = 0; i < maxDoc; ++i) {
        BytesRef s1 = new BytesRef(TestUtil.randomSimpleString(random(), 2));
        BytesRef s2 = new BytesRef(TestUtil.randomSimpleString(random(), 2));
        field1.setBytesValue(s1);
        field2.setBytesValue(s2);
        w.addDocument(doc);
        Set<BytesRef> set = new TreeSet<>(Arrays.asList(s1, s2));
        out.writeVInt(set.size());
        for (BytesRef ref : set) {
          out.writeVInt(ref.length);
          out.writeBytes(ref.bytes, ref.offset, ref.length);
        }
      }
      out.close();
      w.forceMerge(1);
      DirectoryReader r = DirectoryReader.open(w);
      w.close();
      LeafReader sr = getOnlyLeafReader(r);
      assertEquals(maxDoc, sr.maxDoc());
      SortedSetDocValues values = sr.getSortedSetDocValues("sset");
      assertNotNull(values);
      RAMInputStream in = new RAMInputStream("", buffer);
      BytesRefBuilder b = new BytesRefBuilder();
      for (int i = 0; i < maxDoc; ++i) {
        values.setDocument(i);
        final int numValues = in.readVInt();

        for (int j = 0; j < numValues; ++j) {
          b.setLength(in.readVInt());
          b.grow(b.length());
          in.readBytes(b.bytes(), 0, b.length());
          assertEquals(b.get(), values.lookupOrd(values.nextOrd()));
        }

        assertEquals(SortedSetDocValues.NO_MORE_ORDS, values.nextOrd());
      }
      r.close();
      dir.close();
    }
  }

  @Slow
  public void testSortedNumericAroundBlockSize() throws IOException {
    final int frontier = 1 << Lucene54DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
    for (int maxDoc = frontier - 1; maxDoc <= frontier + 1; ++maxDoc) {
      final Directory dir = newDirectory();
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
      RAMFile buffer = new RAMFile();
      RAMOutputStream out = new RAMOutputStream(buffer, false);
      Document doc = new Document();
      SortedNumericDocValuesField field1 = new SortedNumericDocValuesField("snum", 0L);
      doc.add(field1);
      SortedNumericDocValuesField field2 = new SortedNumericDocValuesField("snum", 0L);
      doc.add(field2);
      for (int i = 0; i < maxDoc; ++i) {
        long s1 = random().nextInt(100);
        long s2 = random().nextInt(100);
        field1.setLongValue(s1);
        field2.setLongValue(s2);
        w.addDocument(doc);
        out.writeVLong(Math.min(s1, s2));
        out.writeVLong(Math.max(s1, s2));
      }
      out.close();
      w.forceMerge(1);
      DirectoryReader r = DirectoryReader.open(w);
      w.close();
      LeafReader sr = getOnlyLeafReader(r);
      assertEquals(maxDoc, sr.maxDoc());
      SortedNumericDocValues values = sr.getSortedNumericDocValues("snum");
      assertNotNull(values);
      RAMInputStream in = new RAMInputStream("", buffer);
      for (int i = 0; i < maxDoc; ++i) {
        values.setDocument(i);
        assertEquals(2, values.count());
        assertEquals(in.readVLong(), values.valueAt(0));
        assertEquals(in.readVLong(), values.valueAt(1));
      }
      r.close();
      dir.close();
    }
  }
}
