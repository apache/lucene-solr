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
import java.util.EnumSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.apache.lucene.index.codecs.docvalues.DocValuesCodec;
import org.apache.lucene.index.values.DocValues.MissingValue;
import org.apache.lucene.index.values.DocValues.Source;
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
   * TODO:
   * Roadmap to land on trunk
   *   
   *   - Add documentation for:
   *      - Source and ValuesEnum
   *      - DocValues
   *      - ValuesField
   *      - ValuesAttribute
   *      - Values
   *   - Add @lucene.experimental to all necessary classes
   *   - add test for unoptimized case with deletes
   *   - add a test for addIndexes
   *   - split up existing testcases and give them meaningfull names
   *   - use consistent naming throughout DocValues
   *     - Values -> DocValueType
   *     - PackedIntsImpl -> Ints
   *   - run RAT
   *   - add tests for FieldComparator FloatIndexValuesComparator vs. FloatValuesComparator etc.
   */

  private DocValuesCodec docValuesCodec;
  private CodecProvider provider;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    String defaultFieldCodec = CodecProvider.getDefault().getDefaultFieldCodec();
    provider = new CodecProvider();
    docValuesCodec = new DocValuesCodec(CodecProvider.getDefault().lookup(defaultFieldCodec));
    provider.register(docValuesCodec);
    provider.setDefaultFieldCodec(docValuesCodec.name);
  }
  
  
  /*
   * Simple test case to show how to use the API
   */
  public void testDocValuesSimple() throws CorruptIndexException, IOException, ParseException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, writerConfig(false));
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      ValuesField valuesField = new ValuesField("docId");
      valuesField.setInt(i);
      doc.add(valuesField);
      doc.add(new Field("docId", "" + i, Store.NO, Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.commit();
    writer.optimize(true);
   
    writer.close();
    
    IndexReader reader = IndexReader.open(dir, null, true, 1, provider);
    assertTrue(reader.isOptimized());
   
    IndexSearcher searcher = new IndexSearcher(reader);
    QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "docId", new MockAnalyzer());
    TopDocs search = searcher.search(parser.parse("0 OR 1 OR 2 OR 3 OR 4"), 10);
    assertEquals(5, search.totalHits);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    DocValues docValues = MultiFields.getDocValues(reader, "docId");
    Source source = docValues.getSource();
    for (int i = 0; i < scoreDocs.length; i++) {
      assertEquals(i, scoreDocs[i].doc);
      assertEquals(i, source.getInt(scoreDocs[i].doc));
    }
    reader.close();
    dir.close();
  }

  /**
   * Tests complete indexing of {@link Values} including deletions, merging and
   * sparse value fields on Compound-File
   */
  public void testIndexBytesNoDeletesCFS() throws IOException {
    runTestIndexBytes(writerConfig(true), false);
  }

  public void testIndexBytesDeletesCFS() throws IOException {
    runTestIndexBytes(writerConfig(true), true);
  }

  public void testIndexNumericsNoDeletesCFS() throws IOException {
    runTestNumerics(writerConfig(true), false);
  }

  public void testIndexNumericsDeletesCFS() throws IOException {
    runTestNumerics(writerConfig(true), true);
  }

  /**
   * Tests complete indexing of {@link Values} including deletions, merging and
   * sparse value fields on None-Compound-File
   */
  public void testIndexBytesNoDeletes() throws IOException {
    runTestIndexBytes(writerConfig(false), false);
  }

  public void testIndexBytesDeletes() throws IOException {
    runTestIndexBytes(writerConfig(false), true);
  }

  public void testIndexNumericsNoDeletes() throws IOException {
    runTestNumerics(writerConfig(false), false);
  }

  public void testIndexNumericsDeletes() throws IOException {
    runTestNumerics(writerConfig(false), true);
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
    cfg.setCodecProvider(provider);
    return cfg;
  }

  public void runTestNumerics(IndexWriterConfig cfg, boolean withDeletions)
      throws IOException {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final int numValues = 179 + random.nextInt(151);
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
      case PACKED_INTS: {
        DocValues intsReader = getDocValues(r, val.name());
        assertNotNull(intsReader);

        Source ints = getSource(intsReader);
        MissingValue missing = ints.getMissing();

        for (int i = 0; i < base; i++) {
          long value = ints.getInt(i);
          assertEquals("index " + i, missing.longValue, value);
        }

        ValuesEnum intsEnum = getValuesEnum(intsReader);
        assertTrue(intsEnum.advance(0) >= base);

        intsEnum = getValuesEnum(intsReader);
        LongsRef enumRef = intsEnum.getInt();

        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs", i, intsEnum.advance(i));
          assertEquals(expected, enumRef.get());
          assertEquals(expected, ints.getInt(i));

        }
      }
        break;
      case SIMPLE_FLOAT_4BYTE:
      case SIMPLE_FLOAT_8BYTE: {
        DocValues floatReader = getDocValues(r, val.name());
        assertNotNull(floatReader);
        Source floats = getSource(floatReader);
        MissingValue missing = floats.getMissing();

        for (int i = 0; i < base; i++) {
          double value = floats.getFloat(i);
          assertEquals(" floats failed for doc: " + i + " base: " + base,
              missing.doubleValue, value, 0.0d);
        }
        ValuesEnum floatEnum = getValuesEnum(floatReader);
        assertTrue(floatEnum.advance(0) >= base);

        floatEnum = getValuesEnum(floatReader);
        FloatsRef enumRef = floatEnum.getFloat();
        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals("advance failed at index: " + i + " of " + r.numDocs()
              + " docs base:" + base, i, floatEnum.advance(i));
          assertEquals(floatEnum.getClass() + " index " + i, 2.0 * expected, enumRef.get(), 0.00001);
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
    final List<Values> byteVariantList = new ArrayList<Values>(BYTES);
    // run in random order to test if fill works correctly during merges
    Collections.shuffle(byteVariantList, random);
    final int numValues = 179 + random.nextInt(151);
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
      Source bytes = getSource(bytesReader);
      byte upto = 0;

      // test the filled up slots for correctness
      MissingValue missing = bytes.getMissing();
      for (int i = 0; i < base; i++) {

        BytesRef br = bytes.getBytes(i, new BytesRef());
        String msg = " field: " + byteIndexValue.name() + " at index: " + i
            + " base: " + base + " numDocs:" + r.numDocs();
        switch (byteIndexValue) {
        case BYTES_VAR_STRAIGHT:
        case BYTES_FIXED_STRAIGHT:
          // fixed straight returns bytesref with zero bytes all of fixed
          // length
          if (missing.bytesValue != null) {
            assertNotNull("expected none null - " + msg, br);
            if (br.length != 0) {
              assertEquals("expected zero bytes of length " + bytesSize + " - "
                  + msg, bytesSize, br.length);
              for (int j = 0; j < br.length; j++) {
                assertEquals("Byte at index " + j + " doesn't match - " + msg,
                    0, br.bytes[br.offset + j]);
              }
            }
          } else {
            assertNull("expected null - " + msg + " " + br, br);
          }
          break;
        case BYTES_VAR_SORTED:
        case BYTES_FIXED_SORTED:
        case BYTES_VAR_DEREF:
        case BYTES_FIXED_DEREF:
        default:
          assertNull("expected null - " + msg + " " + br, br);
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
            + bytesSize;
        while (withDeletions && deleted.get(v++)) {
          upto += bytesSize;
        }

        BytesRef br = bytes.getBytes(i, new BytesRef());
        if (bytesEnum.docID() != i) {
          assertEquals("seek failed for index " + i + " " + msg, i, bytesEnum
              .advance(i));
        }
        for (int j = 0; j < br.length; j++, upto++) {
          assertTrue(bytesEnum.getClass() + " enumRef not initialized " + msg, enumRef.bytes.length > 0);
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

  private DocValues getDocValues(IndexReader reader, String field)
      throws IOException {
    boolean optimized = reader.isOptimized();
    Fields fields = optimized ? reader.getSequentialSubReaders()[0].fields()
        : MultiFields.getFields(reader);
    switch (random.nextInt(optimized ? 3 : 2)) { // case 2 only if optimized
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
    case 2:// this only works if we are on an optimized index!
      return reader.getSequentialSubReaders()[0].docValues(field);
    }
    throw new RuntimeException();
  }

  private Source getSource(DocValues values) throws IOException {
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

  private ValuesEnum getValuesEnum(DocValues values) throws IOException {
    ValuesEnum valuesEnum;
    if (!(values instanceof MultiDocValues) && random.nextInt(10) == 0) {
      // TODO not supported by MultiDocValues yet!
      valuesEnum = getSource(values).getEnum();
    } else {
      valuesEnum = values.getEnum();

    }
    assertNotNull(valuesEnum);
    return valuesEnum;
  }

  private static EnumSet<Values> BYTES = EnumSet.of(Values.BYTES_FIXED_DEREF,
      Values.BYTES_FIXED_SORTED, Values.BYTES_FIXED_STRAIGHT,
      Values.BYTES_VAR_DEREF, Values.BYTES_VAR_SORTED,
      Values.BYTES_VAR_STRAIGHT);

  private static EnumSet<Values> NUMERICS = EnumSet.of(Values.PACKED_INTS,
      Values.SIMPLE_FLOAT_4BYTE, Values.SIMPLE_FLOAT_8BYTE);

  private static Index[] IDX_VALUES = new Index[] { Index.ANALYZED,
      Index.ANALYZED_NO_NORMS, Index.NOT_ANALYZED, Index.NOT_ANALYZED_NO_NORMS,
      Index.NO };

  private OpenBitSet indexValues(IndexWriter w, int numValues, Values value,
      List<Values> valueVarList, boolean withDeletions, int multOfSeven)
      throws CorruptIndexException, IOException {
    final boolean isNumeric = NUMERICS.contains(value);
    OpenBitSet deleted = new OpenBitSet(numValues);
    Document doc = new Document();
    Index idx = IDX_VALUES[random.nextInt(IDX_VALUES.length)];
    AbstractField field = random.nextBoolean() ? new ValuesField(value.name())
        : newField(value.name(), _TestUtil.randomRealisticUnicodeString(random,
            10), idx == Index.NO ? Store.YES : Store.NO, idx);
    doc.add(field);
    ValuesField valField = new ValuesField("prototype");
    final BytesRef bytesRef = new BytesRef();

    final String idBase = value.name() + "_";
    final byte[] b = new byte[multOfSeven];
    if (bytesRef != null) {
      bytesRef.bytes = b;
      bytesRef.length = b.length;
      bytesRef.offset = 0;
    }
    byte upto = 0;
    for (int i = 0; i < numValues; i++) {
      if (isNumeric) {
        switch (value) {
        case PACKED_INTS:
          valField.setInt(i);
          break;
        case SIMPLE_FLOAT_4BYTE:
        case SIMPLE_FLOAT_8BYTE:
          valField.setFloat(2.0f * i);
          break;
        default:
          fail("unexpected value " + value);
        }
      } else {
        for (int j = 0; j < b.length; j++) {
          b[j] = upto++;
        }
        valField.setBytes(bytesRef, value);
      }
      doc.removeFields("id");
      doc.add(new Field("id", idBase + i, Store.YES,
          Index.NOT_ANALYZED_NO_NORMS));
      valField.set(field);
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
        if (random.nextInt(10) == 0) {
          w.commit();
        }
      }
    }
    w.commit();

    // TODO test unoptimized with deletions
    if (withDeletions || random.nextBoolean())
      w.optimize();
    return deleted;
  }

}
