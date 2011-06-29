package org.apache.lucene.index.values;

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
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.IndexDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MultiPerDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util._TestUtil;
import org.junit.Before;

/**
 * 
 * Tests DocValues integration into IndexWriter & Codecs
 * 
 */
public class TestDocValuesIndexing extends LuceneTestCase {
  /*
   * - add test for unoptimized case with deletes
   * - add multithreaded tests / integrate into stress indexing?
   */

  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("cannot work with preflex codec", CodecProvider.getDefault().getDefaultFieldCodec().equals("PreFlex"));
  }
  
  /*
   * Simple test case to show how to use the API
   */
  public void testDocValuesSimple() throws CorruptIndexException, IOException,
      ParseException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, writerConfig(false));
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      IndexDocValuesField valuesField = new IndexDocValuesField("docId");
      valuesField.setInt(i);
      doc.add(valuesField);
      doc.add(new Field("docId", "" + i, Store.NO, Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.commit();
    writer.optimize(true);

    writer.close(true);

    IndexReader reader = IndexReader.open(dir, null, true, 1);
    assertTrue(reader.isOptimized());

    IndexSearcher searcher = new IndexSearcher(reader);
    QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "docId",
        new MockAnalyzer(random));
    TopDocs search = searcher.search(parser.parse("0 OR 1 OR 2 OR 3 OR 4"), 10);
    assertEquals(5, search.totalHits);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    IndexDocValues docValues = MultiPerDocValues.getPerDocs(reader).docValues("docId");
    Source source = docValues.getSource();
    for (int i = 0; i < scoreDocs.length; i++) {
      assertEquals(i, scoreDocs[i].doc);
      assertEquals(i, source.getInt(scoreDocs[i].doc));
    }
    reader.close();
    dir.close();
  }

  public void testIndexBytesNoDeletes() throws IOException {
    runTestIndexBytes(writerConfig(random.nextBoolean()), false);
  }

  public void testIndexBytesDeletes() throws IOException {
    runTestIndexBytes(writerConfig(random.nextBoolean()), true);
  }

  public void testIndexNumericsNoDeletes() throws IOException {
    runTestNumerics(writerConfig(random.nextBoolean()), false);
  }

  public void testIndexNumericsDeletes() throws IOException {
    runTestNumerics(writerConfig(random.nextBoolean()), true);
  }

  public void testAddIndexes() throws IOException {
    int valuesPerIndex = 10;
    List<ValueType> values = Arrays.asList(ValueType.values());
    Collections.shuffle(values, random);
    ValueType first = values.get(0);
    ValueType second = values.get(1);
    String msg = "[first=" + first.name() + ", second=" + second.name() + "]";
    // index first index
    Directory d_1 = newDirectory();
    IndexWriter w_1 = new IndexWriter(d_1, writerConfig(random.nextBoolean()));
    indexValues(w_1, valuesPerIndex, first, values, false, 7);
    w_1.commit();
    assertEquals(valuesPerIndex, w_1.maxDoc());
    _TestUtil.checkIndex(d_1, w_1.getConfig().getCodecProvider());

    // index second index
    Directory d_2 = newDirectory();
    IndexWriter w_2 = new IndexWriter(d_2, writerConfig(random.nextBoolean()));
    indexValues(w_2, valuesPerIndex, second, values, false, 7);
    w_2.commit();
    assertEquals(valuesPerIndex, w_2.maxDoc());
    _TestUtil.checkIndex(d_2, w_2.getConfig().getCodecProvider());

    Directory target = newDirectory();
    IndexWriter w = new IndexWriter(target, writerConfig(random.nextBoolean()));
    IndexReader r_1 = IndexReader.open(w_1, true);
    IndexReader r_2 = IndexReader.open(w_2, true);
    if (random.nextBoolean()) {
      w.addIndexes(d_1, d_2);
    } else {
      w.addIndexes(r_1, r_2);
    }
    w.optimize(true);
    w.commit();
    
    _TestUtil.checkIndex(target, w.getConfig().getCodecProvider());
    assertEquals(valuesPerIndex * 2, w.maxDoc());

    // check values
    
    IndexReader merged = IndexReader.open(w, true);
    ValuesEnum vE_1 = getValuesEnum(getDocValues(r_1, first.name()));
    ValuesEnum vE_2 = getValuesEnum(getDocValues(r_2, second.name()));
    ValuesEnum vE_1_merged = getValuesEnum(getDocValues(merged, first.name()));
    ValuesEnum vE_2_merged = getValuesEnum(getDocValues(merged, second
        .name()));
    switch (second) { // these variants don't advance over missing values
    case BYTES_FIXED_STRAIGHT:
    case BYTES_VAR_STRAIGHT:
    case FLOAT_32:
    case FLOAT_64:
    case VAR_INTS:
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
      assertEquals(msg, valuesPerIndex-1, vE_2_merged.advance(valuesPerIndex-1));
    }
    
    for (int i = 0; i < valuesPerIndex; i++) {
      assertEquals(msg, i, vE_1.nextDoc());
      assertEquals(msg, i, vE_1_merged.nextDoc());

      assertEquals(msg, i, vE_2.nextDoc());
      assertEquals(msg, i + valuesPerIndex, vE_2_merged.nextDoc());
    }
    assertEquals(msg, ValuesEnum.NO_MORE_DOCS, vE_1.nextDoc());
    assertEquals(msg, ValuesEnum.NO_MORE_DOCS, vE_2.nextDoc());
    assertEquals(msg, ValuesEnum.NO_MORE_DOCS, vE_1_merged.advance(valuesPerIndex*2));
    assertEquals(msg, ValuesEnum.NO_MORE_DOCS, vE_2_merged.nextDoc());

    // close resources
    r_1.close();
    r_2.close();
    merged.close();
    w_1.close(true);
    w_2.close(true);
    w.close(true);
    d_1.close();
    d_2.close();
    target.close();
  }

  private IndexWriterConfig writerConfig(boolean useCompoundFile) {
    final IndexWriterConfig cfg = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random));
    cfg.setMergePolicy(newLogMergePolicy(random));
    LogMergePolicy policy = new LogDocMergePolicy();
    cfg.setMergePolicy(policy);
    policy.setUseCompoundFile(useCompoundFile);
    return cfg;
  }

  public void runTestNumerics(IndexWriterConfig cfg, boolean withDeletions)
      throws IOException {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final int numValues = 50 + atLeast(10);
    final List<ValueType> numVariantList = new ArrayList<ValueType>(NUMERICS);

    // run in random order to test if fill works correctly during merges
    Collections.shuffle(numVariantList, random);
    for (ValueType val : numVariantList) {
      OpenBitSet deleted = indexValues(w, numValues, val, numVariantList,
          withDeletions, 7);
      List<Closeable> closeables = new ArrayList<Closeable>();
      IndexReader r = IndexReader.open(w, true);
      final int numRemainingValues = (int) (numValues - deleted.cardinality());
      final int base = r.numDocs() - numRemainingValues;
      // for FIXED_INTS_8 we use value mod 128 - to enable testing in 
      // one go we simply use numValues as the mod for all other INT types
      int mod = numValues;
      switch (val) {
      case FIXED_INTS_8:
        mod = 128;
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case VAR_INTS: {
        IndexDocValues intsReader = getDocValues(r, val.name());
        assertNotNull(intsReader);

        Source ints = getSource(intsReader);

        for (int i = 0; i < base; i++) {
          long value = ints.getInt(i);
          assertEquals("index " + i, 0, value);
        }

        ValuesEnum intsEnum = getValuesEnum(intsReader);
        assertTrue(intsEnum.advance(base) >= base);

        intsEnum = getValuesEnum(intsReader);
        LongsRef enumRef = intsEnum.getInt();

        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs", i, intsEnum.advance(i));
          assertEquals(val + " mod: " + mod + " index: " +  i, expected%mod, ints.getInt(i));
          assertEquals(expected%mod, enumRef.get());

        }
      }
        break;
      case FLOAT_32:
      case FLOAT_64: {
        IndexDocValues floatReader = getDocValues(r, val.name());
        assertNotNull(floatReader);
        Source floats = getSource(floatReader);
        for (int i = 0; i < base; i++) {
          double value = floats.getFloat(i);
          assertEquals(val + " failed for doc: " + i + " base: " + base,
              0.0d, value, 0.0d);
        }
        ValuesEnum floatEnum = getValuesEnum(floatReader);
        assertTrue(floatEnum.advance(base) >= base);

        floatEnum = getValuesEnum(floatReader);
        FloatsRef enumRef = floatEnum.getFloat();
        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs base:" + base, i, floatEnum.advance(i));
          assertEquals(floatEnum.getClass() + " index " + i, 2.0 * expected,
              enumRef.get(), 0.00001);
          assertEquals("index " + i, 2.0 * expected, floats.getFloat(i),
              0.00001);
        }
      }
        break;
      default:
        fail("unexpected value " + val);
      }

      closeables.add(r);
      for (Closeable toClose : closeables) {
        toClose.close();
      }
    }
    w.close();
    d.close();
  }

  public void runTestIndexBytes(IndexWriterConfig cfg, boolean withDeletions)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    final Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final List<ValueType> byteVariantList = new ArrayList<ValueType>(BYTES);
    // run in random order to test if fill works correctly during merges
    Collections.shuffle(byteVariantList, random);
    final int numValues = 50 + atLeast(10);
    for (ValueType byteIndexValue : byteVariantList) {
      List<Closeable> closeables = new ArrayList<Closeable>();
      final int bytesSize = 1 + atLeast(50);
      OpenBitSet deleted = indexValues(w, numValues, byteIndexValue,
          byteVariantList, withDeletions, bytesSize);
      final IndexReader r = IndexReader.open(w, withDeletions);
      assertEquals(0, r.numDeletedDocs());
      final int numRemainingValues = (int) (numValues - deleted.cardinality());
      final int base = r.numDocs() - numRemainingValues;
      IndexDocValues bytesReader = getDocValues(r, byteIndexValue.name());
      assertNotNull("field " + byteIndexValue.name()
          + " returned null reader - maybe merged failed", bytesReader);
      Source bytes = getSource(bytesReader);
      byte upto = 0;

      // test the filled up slots for correctness
      for (int i = 0; i < base; i++) {

        BytesRef br = bytes.getBytes(i, new BytesRef());
        String msg = " field: " + byteIndexValue.name() + " at index: " + i
            + " base: " + base + " numDocs:" + r.numDocs();
        switch (byteIndexValue) {
        case BYTES_VAR_STRAIGHT:
        case BYTES_FIXED_STRAIGHT:
          // fixed straight returns bytesref with zero bytes all of fixed
          // length
          assertNotNull("expected none null - " + msg, br);
          if (br.length != 0) {
            assertEquals("expected zero bytes of length " + bytesSize + " - "
                + msg + br.utf8ToString(), bytesSize, br.length);
            for (int j = 0; j < br.length; j++) {
              assertEquals("Byte at index " + j + " doesn't match - " + msg, 0,
                  br.bytes[br.offset + j]);
            }
          }
          break;
        case BYTES_VAR_SORTED:
        case BYTES_FIXED_SORTED:
        case BYTES_VAR_DEREF:
        case BYTES_FIXED_DEREF:
        default:
          assertNotNull("expected none null - " + msg, br);
          assertEquals(0, br.length);
          // make sure we advance at least until base
          ValuesEnum bytesEnum = getValuesEnum(bytesReader);
          final int advancedTo = bytesEnum.advance(0);
          assertTrue(byteIndexValue.name() + " advanced failed base:" + base
              + " advancedTo: " + advancedTo, base <= advancedTo);
        }
      }

      ValuesEnum bytesEnum = getValuesEnum(bytesReader);
      final BytesRef enumRef = bytesEnum.bytes();
      // test the actual doc values added in this iteration
      assertEquals(base + numRemainingValues, r.numDocs());
      int v = 0;
      for (int i = base; i < r.numDocs(); i++) {
        String msg = " field: " + byteIndexValue.name() + " at index: " + i
            + " base: " + base + " numDocs:" + r.numDocs() + " bytesSize: "
            + bytesSize + " src: " + bytes;
        while (withDeletions && deleted.get(v++)) {
          upto += bytesSize;
        }
        BytesRef br = bytes.getBytes(i, new BytesRef());
        if (bytesEnum.docID() != i) {
          assertEquals("seek failed for index " + i + " " + msg, i, bytesEnum
              .advance(i));
        }
        assertTrue(msg, br.length > 0);
        for (int j = 0; j < br.length; j++, upto++) {
          assertTrue(" enumRef not initialized " + msg,
              enumRef.bytes.length > 0);
          assertEquals(
              "EnumRef Byte at index " + j + " doesn't match - " + msg, upto,
              enumRef.bytes[enumRef.offset + j]);
          if (!(br.bytes.length > br.offset + j))
            br = bytes.getBytes(i, new BytesRef());
          assertTrue("BytesRef index exceeded [" + msg + "] offset: "
              + br.offset + " length: " + br.length + " index: "
              + (br.offset + j), br.bytes.length > br.offset + j);
          assertEquals("SourceRef Byte at index " + j + " doesn't match - "
              + msg, upto, br.bytes[br.offset + j]);
        }
      }

      // clean up
      closeables.add(r);
      for (Closeable toClose : closeables) {
        toClose.close();
      }
    }

    w.close();
    d.close();
  }

  private IndexDocValues getDocValues(IndexReader reader, String field)
      throws IOException {
    boolean optimized = reader.isOptimized();
    PerDocValues perDoc = optimized ? reader.getSequentialSubReaders()[0].perDocValues()
        : MultiPerDocValues.getPerDocs(reader);
    switch (random.nextInt(optimized ? 3 : 2)) { // case 2 only if optimized
    case 0:
      return perDoc.docValues(field);
    case 1:
      IndexDocValues docValues = perDoc.docValues(field);
      if (docValues != null) {
        return docValues;
      }
      throw new RuntimeException("no such field " + field);
    case 2:// this only works if we are on an optimized index!
      return reader.getSequentialSubReaders()[0].docValues(field);
    }
    throw new RuntimeException();
  }

  private Source getSource(IndexDocValues values) throws IOException {
    Source source;
    if (random.nextInt(10) == 0) {
      source = values.load();
    } else {
      // getSource uses cache internally
      source = values.getSource();
    }
    assertNotNull(source);
    return source;
  }

  private ValuesEnum getValuesEnum(IndexDocValues values) throws IOException {
    ValuesEnum valuesEnum;
    if (!(values instanceof MultiIndexDocValues) && random.nextInt(10) == 0) {
      // TODO not supported by MultiDocValues yet!
      valuesEnum = getSource(values).getEnum();
    } else {
      valuesEnum = values.getEnum();

    }
    assertNotNull(valuesEnum);
    return valuesEnum;
  }

  private static EnumSet<ValueType> BYTES = EnumSet.of(ValueType.BYTES_FIXED_DEREF,
      ValueType.BYTES_FIXED_SORTED, ValueType.BYTES_FIXED_STRAIGHT, ValueType.BYTES_VAR_DEREF,
      ValueType.BYTES_VAR_SORTED, ValueType.BYTES_VAR_STRAIGHT);

  private static EnumSet<ValueType> NUMERICS = EnumSet.of(ValueType.VAR_INTS,
      ValueType.FIXED_INTS_16, ValueType.FIXED_INTS_32,
      ValueType.FIXED_INTS_64, 
      ValueType.FIXED_INTS_8,
      ValueType.FLOAT_32,
      ValueType.FLOAT_64);

  private static Index[] IDX_VALUES = new Index[] { Index.ANALYZED,
      Index.ANALYZED_NO_NORMS, Index.NOT_ANALYZED, Index.NOT_ANALYZED_NO_NORMS,
      Index.NO };

  private OpenBitSet indexValues(IndexWriter w, int numValues, ValueType value,
      List<ValueType> valueVarList, boolean withDeletions, int bytesSize)
      throws CorruptIndexException, IOException {
    final boolean isNumeric = NUMERICS.contains(value);
    OpenBitSet deleted = new OpenBitSet(numValues);
    Document doc = new Document();
    Index idx = IDX_VALUES[random.nextInt(IDX_VALUES.length)];
    AbstractField field = random.nextBoolean() ? new IndexDocValuesField(value.name())
        : newField(value.name(), _TestUtil.randomRealisticUnicodeString(random,
            10), idx == Index.NO ? Store.YES : Store.NO, idx);
    doc.add(field);
    IndexDocValuesField valField = new IndexDocValuesField("prototype");
    final BytesRef bytesRef = new BytesRef();

    final String idBase = value.name() + "_";
    final byte[] b = new byte[bytesSize];
    if (bytesRef != null) {
      bytesRef.bytes = b;
      bytesRef.length = b.length;
      bytesRef.offset = 0;
    }
    byte upto = 0;
    for (int i = 0; i < numValues; i++) {
      if (isNumeric) {
        switch (value) {
        case VAR_INTS:
          valField.setInt((long)i);
          break;
        case FIXED_INTS_16:
          valField.setInt((short)i, random.nextInt(10) != 0);
          break;
        case FIXED_INTS_32:
          valField.setInt(i, random.nextInt(10) != 0);
          break;
        case FIXED_INTS_64:
          valField.setInt((long)i, random.nextInt(10) != 0);
          break;
        case FIXED_INTS_8:
          valField.setInt((byte)(0xFF & (i % 128)), random.nextInt(10) != 0);
          break;
        case FLOAT_32:
          valField.setFloat(2.0f * i);
          break;
        case FLOAT_64:
          valField.setFloat(2.0d * i);
          break;
       
        default:
          fail("unexpected value " + value);
        }
      } else {
        for (int j = 0; j < b.length; j++) {
          b[j] = upto++;
        }
        if (bytesRef != null) {
          valField.setBytes(bytesRef, value);
        }
      }
      doc.removeFields("id");
      doc.add(new Field("id", idBase + i, Store.YES,
          Index.NOT_ANALYZED_NO_NORMS));
      valField.set(field);
      w.addDocument(doc);

      if (i % 7 == 0) {
        if (withDeletions && random.nextBoolean()) {
          ValueType val = valueVarList.get(random.nextInt(1 + valueVarList
              .indexOf(value)));
          final int randInt = val == value ? random.nextInt(1 + i) : random
              .nextInt(numValues);
          w.deleteDocuments(new Term("id", val.name() + "_" + randInt));
          if (val == value) {
            deleted.set(randInt);
          }
        }
        if (random.nextInt(10) == 0) {
          w.commit();
        }
      }
    }
    w.commit();

    // TODO test unoptimized with deletions
    if (withDeletions || random.nextBoolean())
      w.optimize(true);
    return deleted;
  }
}
