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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.ValuesField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.values.DocValues.SortedSource;
import org.apache.lucene.index.values.DocValues.Source;
import org.apache.lucene.index.values.codec.DocValuesCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestIndexValues extends LuceneTestCase {

  // TODO test addIndexes
  private static DocValuesCodec docValuesCodec;

  @BeforeClass
  public static void beforeClassLuceneTestCaseJ4() {
    LuceneTestCase.beforeClassLuceneTestCaseJ4();
    final CodecProvider cp = CodecProvider.getDefault();
    docValuesCodec = new DocValuesCodec(cp.lookup(CodecProvider.getDefaultCodec()));
    cp.register(docValuesCodec);
    CodecProvider.setDefaultCodec(docValuesCodec.name);
  }
  
  @AfterClass
  public static void afterClassLuceneTestCaseJ4() {
    final CodecProvider cp = CodecProvider.getDefault();
    cp.unregister(docValuesCodec);
    LuceneTestCase.afterClassLuceneTestCaseJ4();    
  }
  
  
  public void testBytesStraight() throws IOException {
    runTestBytes(Bytes.Mode.STRAIGHT, true);
    runTestBytes(Bytes.Mode.STRAIGHT, false);
  }

  public void testBytesDeref() throws IOException {
    runTestBytes(Bytes.Mode.DEREF, true);
    runTestBytes(Bytes.Mode.DEREF, false);
  }

  public void testBytesSorted() throws IOException {
    runTestBytes(Bytes.Mode.SORTED, true);
    runTestBytes(Bytes.Mode.SORTED, false);
  }

  // nocommit -- for sorted test, do our own Sort of the
  // values and verify it's identical
  public void runTestBytes(final Bytes.Mode mode, final boolean fixedSize)
      throws IOException {

    final BytesRef bytesRef = new BytesRef();

    final Comparator<BytesRef> comp = mode == Bytes.Mode.SORTED ? BytesRef
        .getUTF8SortedAsUnicodeComparator() : null;

    Directory dir = newDirectory();
    Writer w = Bytes.getWriter(dir, "test", mode, comp, fixedSize);
    int maxDoc = 220;
    final String[] values = new String[maxDoc];
    final int lenMin, lenMax;
    if (fixedSize) {
      lenMin = lenMax = 3 + random.nextInt(7);
    } else {
      lenMin = 1;
      lenMax = 15 + random.nextInt(6);
    }
    for (int i = 0; i < 100; i++) {
      final String s;
      if (i > 0 && random.nextInt(5) <= 2) {
        // use prior value
        s = values[2 * random.nextInt(i)];
      } else {
        s = _TestUtil.randomUnicodeString(random, lenMin, lenMax);
      }
      values[2 * i] = s;

      UnicodeUtil.UTF16toUTF8(s, 0, s.length(), bytesRef);
      w.add(2 * i, bytesRef);
    }
    w.finish(maxDoc);

    DocValues r = Bytes.getValues(dir, "test", mode, fixedSize, maxDoc);
    for (int iter = 0; iter < 2; iter++) {
      ValuesEnum bytesEnum = r.getEnum();
      assertNotNull("enum is null", bytesEnum);
      ValuesAttribute attr = bytesEnum.addAttribute(ValuesAttribute.class);
      assertNotNull("attribute is null", attr);
      BytesRef ref = attr.bytes();
      assertNotNull("BytesRef is null - enum not initialized to use bytes",
          attr);

      for (int i = 0; i < 2; i++) {
        final int idx = 2 * i;
        assertEquals("doc: " + idx, idx, bytesEnum.advance(idx));
        String utf8String = ref.utf8ToString();
        assertEquals("doc: " + idx + " lenLeft: " + values[idx].length()
            + " lenRight: " + utf8String.length(), values[idx], utf8String);
      }
      assertEquals(ValuesEnum.NO_MORE_DOCS, bytesEnum.advance(maxDoc));
      assertEquals(ValuesEnum.NO_MORE_DOCS, bytesEnum.advance(maxDoc + 1));

      bytesEnum.close();
    }

    // Verify we can load source twice:
    for (int iter = 0; iter < 2; iter++) {
      Source s;
      DocValues.SortedSource ss;
      if (mode == Bytes.Mode.SORTED) {
        s = ss = r.loadSorted(comp);
      } else {
        s = r.load();
        ss = null;
      }

      for (int i = 0; i < 100; i++) {
        final int idx = 2 * i;
        assertNotNull("doc " + idx + "; value=" + values[idx], s.bytes(idx));
        assertEquals("doc " + idx, values[idx], s.bytes(idx).utf8ToString());
        if (ss != null) {
          assertEquals("doc " + idx, values[idx], ss.getByOrd(ss.ord(idx))
              .utf8ToString());
          DocValues.SortedSource.LookupResult result = ss
              .getByValue(new BytesRef(values[idx]));
          assertTrue(result.found);
          assertEquals(ss.ord(idx), result.ord);
        }
      }

      // Lookup random strings:
      if (mode == Bytes.Mode.SORTED) {
        final int numValues = ss.getValueCount();
        for (int i = 0; i < 1000; i++) {
          BytesRef bytesValue = new BytesRef(_TestUtil.randomUnicodeString(
              random, lenMin, lenMax));
          SortedSource.LookupResult result = ss.getByValue(bytesValue);
          if (result.found) {
            assert result.ord > 0;
            assertTrue(bytesValue.bytesEquals(ss.getByOrd(result.ord)));
            int count = 0;
            for (int k = 0; k < 100; k++) {
              if (bytesValue.utf8ToString().equals(values[2 * k])) {
                assertEquals(ss.ord(2 * k), result.ord);
                count++;
              }
            }
            assertTrue(count > 0);
          } else {
            assert result.ord >= 0;
            if (result.ord == 0) {
              final BytesRef firstRef = ss.getByOrd(1);
              // random string was before our first
              assertTrue(firstRef.compareTo(bytesValue) > 0);
            } else if (result.ord == numValues) {
              final BytesRef lastRef = ss.getByOrd(numValues);
              // random string was after our last
              assertTrue(lastRef.compareTo(bytesValue) < 0);
            } else {
              // random string fell between two of our values
              final BytesRef before = (BytesRef) ss.getByOrd(result.ord)
                  .clone();
              final BytesRef after = ss.getByOrd(result.ord + 1);
              assertTrue(before.compareTo(bytesValue) < 0);
              assertTrue(bytesValue.compareTo(after) < 0);

            }
          }
        }
      }
    }

    r.close();
    dir.close();
  }

  public void testInts() throws IOException {
    long maxV = 1;
    final int NUM_VALUES = 1000;
    final long[] values = new long[NUM_VALUES];
    for (int rx = 1; rx < 63; rx++, maxV *= 2) {
      for (int b = 0; b < 2; b++) {
        Directory dir = newDirectory();
        boolean useFixedArrays = b == 0;
        Writer w = Ints.getWriter(dir, "test", useFixedArrays);
        for (int i = 0; i < NUM_VALUES; i++) {
          final long v = random.nextLong() % (1 + maxV);
          values[i] = v;
          w.add(i, v);
        }
        final int additionalDocs = 1 + random.nextInt(9);
        w.finish(NUM_VALUES + additionalDocs);

        DocValues r = Ints.getValues(dir, "test", useFixedArrays);
        for (int iter = 0; iter < 2; iter++) {
          Source s = r.load();
          for (int i = 0; i < NUM_VALUES; i++) {
            final long v = s.ints(i);
            assertEquals("index " + i + " b: " + b, values[i], v);
          }
        }

        for (int iter = 0; iter < 2; iter++) {
          ValuesEnum iEnum = r.getEnum();
          ValuesAttribute attr = iEnum.addAttribute(ValuesAttribute.class);
          LongsRef ints = attr.ints();
          for (int i = 0; i < NUM_VALUES; i++) {
            assertEquals(i, iEnum.nextDoc());
            assertEquals(values[i], ints.get());
          }
          for (int i = NUM_VALUES; i < NUM_VALUES + additionalDocs; i++) {
            assertEquals(i, iEnum.nextDoc());
            assertEquals("" + i, 0, ints.get());
          }

          iEnum.close();
        }

        for (int iter = 0; iter < 2; iter++) {
          ValuesEnum iEnum = r.getEnum();
          ValuesAttribute attr = iEnum.addAttribute(ValuesAttribute.class);
          LongsRef ints = attr.ints();
          for (int i = 0; i < NUM_VALUES; i += 1 + random.nextInt(25)) {
            assertEquals(i, iEnum.advance(i));
            assertEquals(values[i], ints.get());
          }
          for (int i = NUM_VALUES; i < NUM_VALUES + additionalDocs; i++) {
            assertEquals(i, iEnum.advance(i));
            assertEquals("" + i, 0, ints.get());
          }

          iEnum.close();
        }
        r.close();
        dir.close();
      }
    }
  }

  public void testFloats4() throws IOException {
    runTestFloats(4, 0.00001);
  }

  private void runTestFloats(int precision, double delta) throws IOException {
    Directory dir = newDirectory();
    Writer w = Floats.getWriter(dir, "test", precision);
    final int NUM_VALUES = 1000;
    final double[] values = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      final double v = precision == 4 ? random.nextFloat() : random
          .nextDouble();
      values[i] = v;
      w.add(i, v);
    }
    final int additionalValues = 1 + random.nextInt(10);
    w.finish(NUM_VALUES + additionalValues);

    DocValues r = Floats.getValues(dir, "test", NUM_VALUES + additionalValues);
    for (int iter = 0; iter < 2; iter++) {
      Source s = r.load();
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(values[i], s.floats(i), 0.0f);
      }
    }

    for (int iter = 0; iter < 2; iter++) {
      ValuesEnum fEnum = r.getEnum();
      ValuesAttribute attr = fEnum.addAttribute(ValuesAttribute.class);
      FloatsRef floats = attr.floats();
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(i, fEnum.nextDoc());
        assertEquals(values[i], floats.get(), delta);
      }
      for (int i = NUM_VALUES; i < NUM_VALUES + additionalValues; i++) {
        assertEquals(i, fEnum.nextDoc());
        assertEquals(0.0, floats.get(), delta);
      }
      fEnum.close();
    }
    for (int iter = 0; iter < 2; iter++) {
      ValuesEnum fEnum = r.getEnum();
      ValuesAttribute attr = fEnum.addAttribute(ValuesAttribute.class);
      FloatsRef floats = attr.floats();
      for (int i = 0; i < NUM_VALUES; i += 1 + random.nextInt(25)) {
        assertEquals(i, fEnum.advance(i));
        assertEquals(values[i], floats.get(), delta);
      }
      for (int i = NUM_VALUES; i < NUM_VALUES + additionalValues; i++) {
        assertEquals(i, fEnum.advance(i));
        assertEquals(0.0, floats.get(), delta);
      }
      fEnum.close();
    }

    r.close();
    dir.close();
  }

  public void testFloats8() throws IOException {
    runTestFloats(8, 0.0);
  }

  /**
   * Tests complete indexing of {@link Values} including deletions, merging and
   * sparse value fields on Compound-File
   */
  public void testCFSIndex() throws IOException {
    // without deletions
    IndexWriterConfig cfg = writerConfig(true);
    // primitives - no deletes
    runTestNumerics(cfg, false);

    cfg = writerConfig(true);
    // bytes - no deletes
    runTestIndexBytes(cfg, false);

    // with deletions
    cfg = writerConfig(true);
    // primitives
    runTestNumerics(cfg, true);

    cfg = writerConfig(true);
    // bytes
    runTestIndexBytes(cfg, true);
  }

  /**
   * Tests complete indexing of {@link Values} including deletions, merging and
   * sparse value fields on None-Compound-File
   */
  public void testIndex() throws IOException {
    //
    // without deletions
    IndexWriterConfig cfg = writerConfig(false);
    // primitives - no deletes
    runTestNumerics(cfg, false);

    cfg = writerConfig(false);
    // bytes - no deletes
    runTestIndexBytes(cfg, false);

    // with deletions
    cfg = writerConfig(false);
    // primitives
    runTestNumerics(cfg, true);

    cfg = writerConfig(false);
    // bytes
    runTestIndexBytes(cfg, true);
  }

  private IndexWriterConfig writerConfig(boolean useCompoundFile) {
    final IndexWriterConfig cfg = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer());
    MergePolicy mergePolicy = cfg.getMergePolicy();
    if (mergePolicy instanceof LogMergePolicy) {
      ((LogMergePolicy) mergePolicy).setUseCompoundFile(useCompoundFile);
    } else if (useCompoundFile) {
      LogMergePolicy policy = new LogDocMergePolicy();
      policy.setUseCompoundFile(useCompoundFile);
      cfg.setMergePolicy(policy);
    }
    return cfg;
  }

  public void runTestNumerics(IndexWriterConfig cfg, boolean withDeletions)
      throws IOException {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final int numValues = 350;
    final List<Values> numVariantList = new ArrayList<Values>(NUMERICS);

    // run in random order to test if fill works correctly during merges
    Collections.shuffle(numVariantList, random);
    for (Values val : numVariantList) {
      OpenBitSet deleted = indexValues(w, numValues, val, numVariantList,
          withDeletions, 7);
      List<Closeable> closeables = new ArrayList<Closeable>();
      IndexReader r = IndexReader.open(w);
      final int numRemainingValues = (int) (numValues - deleted.cardinality());
      final int base = r.numDocs() - numRemainingValues;
      switch (val) {
      case PACKED_INTS:
      case PACKED_INTS_FIXED: {
        DocValues intsReader = getDocValues(r, val.name());
        Source ints = intsReader.load();
        ValuesEnum intsEnum = intsReader.getEnum();
        assertNotNull(intsEnum);
        LongsRef enumRef = intsEnum.addAttribute(ValuesAttribute.class).ints();
        for (int i = 0; i < base; i++) {
          assertEquals(0, ints.ints(i));
          assertEquals(val.name() + " base: " + base + " index: " + i, i,
              random.nextBoolean() ? intsEnum.advance(i) : intsEnum.nextDoc());
          assertEquals(0, enumRef.get());
        }
        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs", i, intsEnum.advance(i));
          assertEquals(expected, ints.ints(i));
          assertEquals(expected, enumRef.get());

        }
      }
        break;
      case SIMPLE_FLOAT_4BYTE:
      case SIMPLE_FLOAT_8BYTE: {
        DocValues floatReader = getDocValues(r, val.name());
        Source floats = floatReader.load();
        ValuesEnum floatEnum = floatReader.getEnum();
        assertNotNull(floatEnum);
        FloatsRef enumRef = floatEnum.addAttribute(ValuesAttribute.class)
            .floats();

        for (int i = 0; i < base; i++) {
          assertEquals(0.0d, floats.floats(i), 0.0d);
          assertEquals(i, random.nextBoolean() ? floatEnum.advance(i)
              : floatEnum.nextDoc());
          assertEquals("index " + i, 0.0, enumRef.get(), 0.0);
        }
        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs base:" + base, i, floatEnum.advance(i));
          assertEquals("index " + i, 2.0 * expected, enumRef.get(), 0.00001);
          assertEquals("index " + i, 2.0 * expected, floats.floats(i), 0.00001);
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

  private static EnumSet<Values> BYTES = EnumSet.of(Values.BYTES_FIXED_DEREF,
      Values.BYTES_FIXED_SORTED, Values.BYTES_FIXED_STRAIGHT,
      Values.BYTES_VAR_DEREF, Values.BYTES_VAR_SORTED,
      Values.BYTES_VAR_STRAIGHT);

  private static EnumSet<Values> STRAIGHT_BYTES = EnumSet.of(
      Values.BYTES_FIXED_STRAIGHT, Values.BYTES_VAR_STRAIGHT);

  private static EnumSet<Values> NUMERICS = EnumSet.of(Values.PACKED_INTS,
      Values.PACKED_INTS_FIXED, Values.SIMPLE_FLOAT_4BYTE,
      Values.SIMPLE_FLOAT_8BYTE);

  private static Index[] IDX_VALUES = new Index[] { Index.ANALYZED,
      Index.ANALYZED_NO_NORMS, Index.NOT_ANALYZED, Index.NOT_ANALYZED_NO_NORMS };

  private OpenBitSet indexValues(IndexWriter w, int numValues, Values value,
      List<Values> valueVarList, boolean withDeletions, int multOfSeven)
      throws CorruptIndexException, IOException {
    final boolean isNumeric = NUMERICS.contains(value);
    OpenBitSet deleted = new OpenBitSet(numValues);
    Document doc = new Document();
    Fieldable field = random.nextBoolean() ? new ValuesField(value.name())
        : newField(value.name(), _TestUtil.randomRealisticUnicodeString(random,
            10), IDX_VALUES[random.nextInt(IDX_VALUES.length)]);
    doc.add(field);

    ValuesAttribute valuesAttribute = ValuesField.values(field);
    valuesAttribute.setType(value);
    final LongsRef intsRef = valuesAttribute.ints();
    final FloatsRef floatsRef = valuesAttribute.floats();
    final BytesRef bytesRef = valuesAttribute.bytes();

    final String idBase = value.name() + "_";
    final byte[] b = new byte[multOfSeven];
    if (bytesRef != null) {
      bytesRef.bytes = b;
      bytesRef.length = b.length;
      bytesRef.offset = 0;
    }
    // 
    byte upto = 0;
    for (int i = 0; i < numValues; i++) {
      if (isNumeric) {
        switch (value) {
        case PACKED_INTS:
        case PACKED_INTS_FIXED:
          intsRef.set(i);
          break;
        case SIMPLE_FLOAT_4BYTE:
        case SIMPLE_FLOAT_8BYTE:
          floatsRef.set(2.0f * i);
          break;
        default:
          fail("unexpected value " + value);
        }
      } else {
        for (int j = 0; j < b.length; j++) {
          b[j] = upto++;
        }
      }
      doc.removeFields("id");
      doc.add(new Field("id", idBase + i, Store.YES,
          Index.NOT_ANALYZED_NO_NORMS));
      w.addDocument(doc);

      if (i % 7 == 0) {
        if (withDeletions && random.nextBoolean()) {
          Values val = valueVarList.get(random.nextInt(1 + valueVarList
              .indexOf(value)));
          final int randInt = val == value ? random.nextInt(1 + i) : random
              .nextInt(numValues);
          w.deleteDocuments(new Term("id", val.name() + "_" + randInt));
          if (val == value) {
            deleted.set(randInt);
          }
        }
        w.commit();

      }
    }
    w.commit();

    // nocommit test unoptimized with deletions
    if (true || withDeletions || random.nextBoolean())
      w.optimize();
    return deleted;
  }

  public void runTestIndexBytes(IndexWriterConfig cfg, boolean withDeletions)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final List<Values> byteVariantList = new ArrayList<Values>(BYTES);

    // run in random order to test if fill works correctly during merges
    Collections.shuffle(byteVariantList, random);
    final int numValues = 350;
    for (Values byteIndexValue : byteVariantList) {
      List<Closeable> closeables = new ArrayList<Closeable>();

      int bytesSize = 7 + random.nextInt(128);
      OpenBitSet deleted = indexValues(w, numValues, byteIndexValue,
          byteVariantList, withDeletions, bytesSize);
      final IndexReader r = IndexReader.open(w);
      assertEquals(0, r.numDeletedDocs());
      final int numRemainingValues = (int) (numValues - deleted.cardinality());
      final int base = r.numDocs() - numRemainingValues;

      DocValues bytesReader = getDocValues(r, byteIndexValue.name());
      assertNotNull("field " + byteIndexValue.name()
          + " returned null reader - maybe merged failed", bytesReader);
      Source bytes = bytesReader.load();
      ValuesEnum bytesEnum = bytesReader.getEnum();
      assertNotNull(bytesEnum);
      final ValuesAttribute attr = bytesEnum
          .addAttribute(ValuesAttribute.class);
      byte upto = 0;
      // test the filled up slots for correctness
      for (int i = 0; i < base; i++) {
        final BytesRef br = bytes.bytes(i);
        String msg = " field: " + byteIndexValue.name() + " at index: " + i
            + " base: " + base + " numDocs:" + r.numDocs();
        switch (byteIndexValue) {
        case BYTES_VAR_STRAIGHT:
        case BYTES_FIXED_STRAIGHT:
          assertEquals(i, bytesEnum.advance(i));
          // fixed straight returns bytesref with zero bytes all of fixed
          // length
          assertNotNull("expected none null - " + msg, br);
          if (br.length != 0) {
            assertEquals("expected zero bytes of length " + bytesSize + " - "
                + msg, bytesSize, br.length);
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
          if (br.length != 0) {
            bytes.bytes(i);
          }
          assertEquals("expected empty bytes - " + br.utf8ToString() + msg, 0,
              br.length);
        }
      }
      final BytesRef enumRef = attr.bytes();

      // test the actual doc values added in this iteration
      assertEquals(base + numRemainingValues, r.numDocs());
      int v = 0;
      for (int i = base; i < r.numDocs(); i++) {

        String msg = " field: " + byteIndexValue.name() + " at index: " + i
            + " base: " + base + " numDocs:" + r.numDocs() + " bytesSize: "
            + bytesSize;
        while (withDeletions && deleted.get(v++)) {
          upto += bytesSize;
        }

        BytesRef br = bytes.bytes(i);
        if (bytesEnum.docID() != i)
          assertEquals("seek failed for index " + i + " " + msg, i, bytesEnum
              .advance(i));
        for (int j = 0; j < br.length; j++, upto++) {
          assertEquals(
              "EnumRef Byte at index " + j + " doesn't match - " + msg, upto,
              enumRef.bytes[enumRef.offset + j]);
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

  private DocValues getDocValues(IndexReader reader, String field)
      throws IOException {
    boolean optimized = reader.isOptimized();
    Fields fields = optimized ? reader.getSequentialSubReaders()[0].fields() : MultiFields
        .getFields(reader);
//    return fields.docValues(field);
    switch (random.nextInt(optimized ? 3 : 2)) {
    case 0:
      return fields.docValues(field);
    case 1:
      FieldsEnum iterator = fields.iterator();
      String name;
      while ((name = iterator.next()) != null) {
        if (name.equals(field))
          return iterator.docValues();
      }
      throw new RuntimeException("no such field " + field);
    case 2:
      return reader.getSequentialSubReaders()[0].docValues(field);
    }
throw new RuntimeException();
}

}
