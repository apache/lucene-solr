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
package org.apache.lucene.codecs.lucene70;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.lucene70.Lucene70DocValuesFormat;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.TestUtil;

/**
 * Tests Lucene70DocValuesFormat
 */
public class TestLucene70DocValuesFormat extends BaseCompressingDocValuesFormatTestCase {
  private final Codec codec = TestUtil.alwaysDocValuesFormat(new Lucene70DocValuesFormat());

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
      doTestSortedVsStoredFields(atLeast(300), 1d, 1, 32766);
    }
  }
  
  @Nightly
  public void testSortedVariableLengthManyVsStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestSortedVsStoredFields(TestUtil.nextInt(random(), 1024, 2049), 1d, 1, 500);
    }
  }
  
  @Slow
  public void testTermsEnumFixedWidth() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 5121), () -> TestUtil.randomSimpleString(random(), 10, 10));
    }
  }
  
  @Slow
  public void testTermsEnumVariableWidth() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 5121), () -> TestUtil.randomSimpleString(random(), 1, 500));
    }
  }
  
  @Nightly
  public void testTermsEnumRandomMany() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 8121), () -> TestUtil.randomSimpleString(random(), 1, 500));
    }
  }

  public void testTermsEnumLongSharedPrefixes() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      doTestTermsEnumRandom(TestUtil.nextInt(random(), 1025, 5121), () -> {
        char[] chars = new char[random().nextInt(500)];
        Arrays.fill(chars, 'a');
        if (chars.length > 0) {
          chars[random().nextInt(chars.length)] = 'b';
        }
        return new String(chars);
      });
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

      final SortedDocValues sorted = DocValues.getSorted(reader, "sorted");

      final BinaryDocValues binary = DocValues.getBinary(reader, "binary");

      final SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(reader, "sorted_numeric");

      final SortedSetDocValues sortedSet = DocValues.getSortedSet(reader, "sorted_set");

      for (int i = 0; i < reader.maxDoc(); ++i) {
        final Document doc = reader.document(i);
        final IndexableField valueField = doc.getField("value");
        final Long value = valueField == null ? null : valueField.numericValue().longValue();

        if (value == null) {
          assertTrue(numeric.docID() + " vs " + i, numeric.docID() < i);
        } else {
          assertEquals(i, numeric.nextDoc());
          assertEquals(i, binary.nextDoc());
          assertEquals(i, sorted.nextDoc());
          assertEquals(value.longValue(), numeric.longValue());
          assertTrue(sorted.ordValue() >= 0);
          assertEquals(new BytesRef(Long.toString(value)), sorted.lookupOrd(sorted.ordValue()));
          assertEquals(new BytesRef(Long.toString(value)), binary.binaryValue());
        }

        final IndexableField[] valuesFields = doc.getFields("values");
        if (valuesFields.length == 0) {
          assertTrue(sortedNumeric.docID() + " vs " + i, sortedNumeric.docID() < i);
        } else {
          final Set<Long> valueSet = new HashSet<>();
          for (IndexableField sf : valuesFields) {
            valueSet.add(sf.numericValue().longValue());
          }

          assertEquals(i, sortedNumeric.nextDoc());
          assertEquals(valuesFields.length, sortedNumeric.docValueCount());
          for (int j = 0; j < sortedNumeric.docValueCount(); ++j) {
            assertTrue(valueSet.contains(sortedNumeric.nextValue()));
          }
          assertEquals(i, sortedSet.nextDoc());
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
        }
      }
    }

    indexReader.close();
    dir.close();
  }

  // TODO: try to refactor this and some termsenum tests into the base class.
  // to do this we need to fix the test class to get a DVF not a Codec so we can setup
  // the postings format correctly.
  private void doTestTermsEnumRandom(int numDocs, Supplier<String> valuesProducer) throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergeScheduler(new SerialMergeScheduler());
    // set to duel against a codec which has ordinals:
    final PostingsFormat pf = TestUtil.getPostingsFormatWithOrds(random());
    final DocValuesFormat dv = new Lucene70DocValuesFormat();
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
      int numValues = random().nextInt(17);
      // create a random list of strings
      List<String> values = new ArrayList<>();
      for (int v = 0; v < numValues; v++) {
        values.add(valuesProducer.get());
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

  @Slow
  public void testSortedSetAroundBlockSize() throws IOException {
    final int frontier = 1 << Lucene70DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
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
        assertEquals(i, values.nextDoc());
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
    final int frontier = 1 << Lucene70DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
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
        assertEquals(i, values.nextDoc());
        assertEquals(2, values.docValueCount());
        assertEquals(in.readVLong(), values.nextValue());
        assertEquals(in.readVLong(), values.nextValue());
      }
      r.close();
      dir.close();
    }
  }

  @Slow
  public void testSortedNumericBlocksOfVariousBitsPerValue() throws Exception {
    doTestSortedNumericBlocksOfVariousBitsPerValue(() -> TestUtil.nextInt(random(), 1, 3));
  }

  @Slow
  public void testSparseSortedNumericBlocksOfVariousBitsPerValue() throws Exception {
    doTestSortedNumericBlocksOfVariousBitsPerValue(() -> TestUtil.nextInt(random(), 0, 2));
  }

  @Slow
  public void testNumericBlocksOfVariousBitsPerValue() throws Exception {
    doTestSparseNumericBlocksOfVariousBitsPerValue(1);
  }

  @Slow
  public void testSparseNumericBlocksOfVariousBitsPerValue() throws Exception {
    doTestSparseNumericBlocksOfVariousBitsPerValue(random().nextDouble());
  }

  private static LongSupplier blocksOfVariousBPV() {
    final long mul = TestUtil.nextInt(random(), 1, 100);
    final long min = random().nextInt();
    return new LongSupplier() {
      int i = Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE;
      int maxDelta;
      @Override
      public long getAsLong() {
        if (i == Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE) {
          maxDelta = 1 << random().nextInt(5);
          i = 0;
        }
        i++;
        return min + mul * random().nextInt(maxDelta);
      }
    };
  }

  private void doTestSortedNumericBlocksOfVariousBitsPerValue(LongSupplier counts) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(atLeast(Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE));
    conf.setRAMBufferSizeMB(-1);
    conf.setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    final int numDocs = atLeast(Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE*3);
    final LongSupplier values = blocksOfVariousBPV();
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      
      int valueCount = (int) counts.getAsLong();
      long valueArray[] = new long[valueCount];
      for (int j = 0; j < valueCount; j++) {
        long value = values.getAsLong();
        valueArray[j] = value;
        doc.add(new SortedNumericDocValuesField("dv", value));
      }
      Arrays.sort(valueArray);
      for (int j = 0; j < valueCount; j++) {
        doc.add(new StoredField("stored", Long.toString(valueArray[j])));
      }
      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }
    writer.forceMerge(1);

    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(r, "dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        if (i > docValues.docID()) {
          docValues.nextDoc();
        }
        String expected[] = r.document(i).getValues("stored");
        if (i < docValues.docID()) {
          assertEquals(0, expected.length);
        } else {
          String actual[] = new String[docValues.docValueCount()];
          for (int j = 0; j < actual.length; j++) {
            actual[j] = Long.toString(docValues.nextValue());
          }
          assertArrayEquals(expected, actual);
        }
      }
    }
    ir.close();
    dir.close();
  }

  private void doTestSparseNumericBlocksOfVariousBitsPerValue(double density) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(atLeast(Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE));
    conf.setRAMBufferSizeMB(-1);
    conf.setMergePolicy(newLogMergePolicy(random().nextBoolean()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    Field storedField = newStringField("stored", "", Field.Store.YES);
    Field dvField = new NumericDocValuesField("dv", 0);
    doc.add(storedField);
    doc.add(dvField);

    final int numDocs = atLeast(Lucene70DocValuesFormat.NUMERIC_BLOCK_SIZE*3);
    final LongSupplier longs = blocksOfVariousBPV();
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() > density) {
        writer.addDocument(new Document());
        continue;
      }
      long value = longs.getAsLong();
      storedField.setStringValue(Long.toString(value));
      dvField.setLongValue(value);
      writer.addDocument(doc);
    }

    writer.forceMerge(1);

    writer.close();
    
    // compare
    DirectoryReader ir = DirectoryReader.open(dir);
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues docValues = DocValues.getNumeric(r, "dv");
      docValues.nextDoc();
      for (int i = 0; i < r.maxDoc(); i++) {
        String storedValue = r.document(i).get("stored");
        if (storedValue == null) {
          assertTrue(docValues.docID() > i);
        } else {
          assertEquals(i, docValues.docID());
          assertEquals(Long.parseLong(storedValue), docValues.longValue());
          docValues.nextDoc();
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
    }
    ir.close();
    dir.close();
  }
}
