package org.apache.lucene.index;

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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Before;

/**
 * 
 * Tests DocValues integration into IndexWriter & Codecs
 * 
 */
public class TestDocValuesIndexing extends LuceneTestCase {
  /*
   * - add test for multi segment case with deletes
   * - add multithreaded tests / integrate into stress indexing?
   */

  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("cannot work with preflex codec", Codec.getDefault().getName().equals("Lucene3x"));
  }
  
  /*
   * Simple test case to show how to use the API
   */
  public void testDocValuesSimple() throws CorruptIndexException, IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, writerConfig(false));
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      DocValuesField valuesField = new DocValuesField("docId");
      valuesField.setInt(i);
      doc.add(valuesField);
      doc.add(new TextField("docId", "" + i));
      writer.addDocument(doc);
    }
    writer.commit();
    writer.forceMerge(1, true);

    writer.close(true);

    IndexReader reader = IndexReader.open(dir, 1);
    assertEquals(1, reader.getSequentialSubReaders().length);

    IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("docId", "0")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "1")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "2")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "3")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("docId", "4")), BooleanClause.Occur.SHOULD);

    TopDocs search = searcher.search(query, 10);
    assertEquals(5, search.totalHits);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    DocValues docValues = MultiDocValues.getDocValues(reader, "docId");
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
    List<Type> values = Arrays.asList(Type.values());
    Collections.shuffle(values, random);
    Type first = values.get(0);
    Type second = values.get(1);
    // index first index
    Directory d_1 = newDirectory();
    IndexWriter w_1 = new IndexWriter(d_1, writerConfig(random.nextBoolean()));
    indexValues(w_1, valuesPerIndex, first, values, false, 7);
    w_1.commit();
    assertEquals(valuesPerIndex, w_1.maxDoc());
    _TestUtil.checkIndex(d_1);

    // index second index
    Directory d_2 = newDirectory();
    IndexWriter w_2 = new IndexWriter(d_2, writerConfig(random.nextBoolean()));
    indexValues(w_2, valuesPerIndex, second, values, false, 7);
    w_2.commit();
    assertEquals(valuesPerIndex, w_2.maxDoc());
    _TestUtil.checkIndex(d_2);

    Directory target = newDirectory();
    IndexWriter w = new IndexWriter(target, writerConfig(random.nextBoolean()));
    IndexReader r_1 = IndexReader.open(w_1, true);
    IndexReader r_2 = IndexReader.open(w_2, true);
    if (random.nextBoolean()) {
      w.addIndexes(d_1, d_2);
    } else {
      w.addIndexes(r_1, r_2);
    }
    w.forceMerge(1, true);
    w.commit();
    
    _TestUtil.checkIndex(target);
    assertEquals(valuesPerIndex * 2, w.maxDoc());

    // check values
    
    IndexReader merged = IndexReader.open(w, true);
    Source source_1 = getSource(getDocValues(r_1, first.name()));
    Source source_2 = getSource(getDocValues(r_2, second.name()));
    Source source_1_merged = getSource(getDocValues(merged, first.name()));
    Source source_2_merged = getSource(getDocValues(merged, second
        .name()));
    for (int i = 0; i < r_1.maxDoc(); i++) {
      switch (first) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_FIXED_SORTED:
      case BYTES_VAR_SORTED:
        assertEquals(source_1.getBytes(i, new BytesRef()),
            source_1_merged.getBytes(i, new BytesRef()));
        break;
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        assertEquals(source_1.getInt(i), source_1_merged.getInt(i));
        break;
      case FLOAT_32:
      case FLOAT_64:
        assertEquals(source_1.getFloat(i), source_1_merged.getFloat(i), 0.0d);
        break;
      default:
        fail("unkonwn " + first);
      }
    }

    for (int i = r_1.maxDoc(); i < merged.maxDoc(); i++) {
      switch (second) {
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_STRAIGHT:
      case BYTES_FIXED_SORTED:
      case BYTES_VAR_SORTED:
        assertEquals(source_2.getBytes(i - r_1.maxDoc(), new BytesRef()),
            source_2_merged.getBytes(i, new BytesRef()));
        break;
      case FIXED_INTS_16:
      case FIXED_INTS_32:
      case FIXED_INTS_64:
      case FIXED_INTS_8:
      case VAR_INTS:
        assertEquals(source_2.getInt(i - r_1.maxDoc()),
            source_2_merged.getInt(i));
        break;
      case FLOAT_32:
      case FLOAT_64:
        assertEquals(source_2.getFloat(i - r_1.maxDoc()),
            source_2_merged.getFloat(i), 0.0d);
        break;
      default:
        fail("unkonwn " + first);
      }
    }
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

  @SuppressWarnings("fallthrough")
  public void runTestNumerics(IndexWriterConfig cfg, boolean withDeletions)
      throws IOException {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, cfg);
    final int numValues = 50 + atLeast(10);
    final List<Type> numVariantList = new ArrayList<Type>(NUMERICS);

    // run in random order to test if fill works correctly during merges
    Collections.shuffle(numVariantList, random);
    for (Type val : numVariantList) {
      FixedBitSet deleted = indexValues(w, numValues, val, numVariantList,
          withDeletions, 7);
      List<Closeable> closeables = new ArrayList<Closeable>();
      IndexReader r = IndexReader.open(w, true);
      final int numRemainingValues = numValues - deleted.cardinality();
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
        DocValues intsReader = getDocValues(r, val.name());
        assertNotNull(intsReader);

        Source ints = getSource(intsReader);

        for (int i = 0; i < base; i++) {
          long value = ints.getInt(i);
          assertEquals("index " + i, 0, value);
        }

        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
          assertEquals(val + " mod: " + mod + " index: " +  i, expected%mod, ints.getInt(i));
        }
      }
        break;
      case FLOAT_32:
      case FLOAT_64: {
        DocValues floatReader = getDocValues(r, val.name());
        assertNotNull(floatReader);
        Source floats = getSource(floatReader);
        for (int i = 0; i < base; i++) {
          double value = floats.getFloat(i);
          assertEquals(val + " failed for doc: " + i + " base: " + base,
              0.0d, value, 0.0d);
        }
        int expected = 0;
        for (int i = base; i < r.numDocs(); i++, expected++) {
          while (deleted.get(expected)) {
            expected++;
          }
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
    final List<Type> byteVariantList = new ArrayList<Type>(BYTES);
    // run in random order to test if fill works correctly during merges
    Collections.shuffle(byteVariantList, random);
    final int numValues = 50 + atLeast(10);
    for (Type byteIndexValue : byteVariantList) {
      List<Closeable> closeables = new ArrayList<Closeable>();
      final int bytesSize = 1 + atLeast(50);
      FixedBitSet deleted = indexValues(w, numValues, byteIndexValue,
          byteVariantList, withDeletions, bytesSize);
      final IndexReader r = IndexReader.open(w, withDeletions);
      assertEquals(0, r.numDeletedDocs());
      final int numRemainingValues = numValues - deleted.cardinality();
      final int base = r.numDocs() - numRemainingValues;
      DocValues bytesReader = getDocValues(r, byteIndexValue.name());
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
        case BYTES_FIXED_DEREF:
        case BYTES_FIXED_SORTED:
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
        default:
          assertNotNull("expected none null - " + msg, br);
          assertEquals(byteIndexValue + "", 0, br.length);
          // make sure we advance at least until base
        }
      }

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
        assertTrue(msg, br.length > 0);
        for (int j = 0; j < br.length; j++, upto++) {
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

  private DocValues getDocValues(IndexReader reader, String field) throws IOException {
    return MultiDocValues.getDocValues(reader, field);
  }

  private Source getSource(DocValues values) throws IOException {
    // getSource uses cache internally
    switch(random.nextInt(5)) {
    case 3:
      return values.load();
    case 2:
      return values.getDirectSource();
    case 1:
      return values.getSource();
    default:
      return values.getSource();
    }
  }


  private static EnumSet<Type> BYTES = EnumSet.of(Type.BYTES_FIXED_DEREF,
      Type.BYTES_FIXED_STRAIGHT, Type.BYTES_VAR_DEREF,
      Type.BYTES_VAR_STRAIGHT, Type.BYTES_FIXED_SORTED, Type.BYTES_VAR_SORTED);

  private static EnumSet<Type> NUMERICS = EnumSet.of(Type.VAR_INTS,
      Type.FIXED_INTS_16, Type.FIXED_INTS_32,
      Type.FIXED_INTS_64, 
      Type.FIXED_INTS_8,
      Type.FLOAT_32,
      Type.FLOAT_64);

  private FixedBitSet indexValues(IndexWriter w, int numValues, Type value,
      List<Type> valueVarList, boolean withDeletions, int bytesSize)
      throws CorruptIndexException, IOException {
    final boolean isNumeric = NUMERICS.contains(value);
    FixedBitSet deleted = new FixedBitSet(numValues);
    Document doc = new Document();
    DocValuesField valField = new DocValuesField(value.name());
    doc.add(valField);
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
      doc.add(new Field("id", idBase + i, StringField.TYPE_STORED));
      w.addDocument(doc);

      if (i % 7 == 0) {
        if (withDeletions && random.nextBoolean()) {
          Type val = valueVarList.get(random.nextInt(1 + valueVarList
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

    // TODO test multi seg with deletions
    if (withDeletions || random.nextBoolean()) {
      w.forceMerge(1, true);
    }
    return deleted;
  }

  public void testMultiValuedDocValuesField() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, d);
    Document doc = new Document();
    DocValuesField f = new DocValuesField("field");
    f.setInt(17);
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
    IndexReader r = w.getReader();
    w.close();
    assertEquals(17, r.getSequentialSubReaders()[0].docValues("field").load().getInt(0));
    r.close();
    d.close();
  }

  public void testDifferentTypedDocValuesField() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, d);
    Document doc = new Document();
    DocValuesField f = new DocValuesField("field");
    f.setInt(17);
    // Index doc values are single-valued so we should not
    // be able to add same field more than once:
    doc.add(f);
    DocValuesField f2 = new DocValuesField("field");
    f2.setFloat(22.0);
    doc.add(f2);
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
    IndexReader r = w.getReader();
    w.close();
    assertEquals(17, r.getSequentialSubReaders()[0].docValues("field").load().getInt(0));
    r.close();
    d.close();
  }
}
