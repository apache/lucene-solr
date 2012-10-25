package org.apache.lucene.queries.function;

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
import java.util.Date;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.ByteDocValuesField;
import org.apache.lucene.document.DerefBytesDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.IntDocValuesField;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.document.PackedLongDocValuesField;
import org.apache.lucene.document.ShortDocValuesField;
import org.apache.lucene.document.SortedBytesDocValuesField;
import org.apache.lucene.document.StraightBytesDocValuesField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.DateDocValuesFieldSource;
import org.apache.lucene.queries.function.valuesource.NumericDocValuesFieldSource;
import org.apache.lucene.queries.function.valuesource.StrDocValuesFieldSource;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

public class TestDocValuesFieldSources extends LuceneTestCase {

  public void test(DocValues.Type type) throws IOException {
    Directory d = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    final int nDocs = atLeast(50);
    final Field id = new IntDocValuesField("id", 0);
    final Field f;
    switch (type) {
      case BYTES_FIXED_DEREF:
        f = new DerefBytesDocValuesField("dv", new BytesRef(), true);
        break;
      case BYTES_FIXED_SORTED:
        f = new SortedBytesDocValuesField("dv", new BytesRef(), true);
        break;
      case BYTES_FIXED_STRAIGHT:
        f = new StraightBytesDocValuesField("dv", new BytesRef(), true);
        break;
      case BYTES_VAR_DEREF:
        f = new DerefBytesDocValuesField("dv", new BytesRef(), false);
        break;
      case BYTES_VAR_SORTED:
        f = new SortedBytesDocValuesField("dv", new BytesRef(), false);
        break;
      case BYTES_VAR_STRAIGHT:
        f = new StraightBytesDocValuesField("dv", new BytesRef(), false);
        break;
      case FIXED_INTS_8:
        f = new ByteDocValuesField("dv", (byte) 0);
        break;
      case FIXED_INTS_16:
        f = new ShortDocValuesField("dv", (short) 0);
        break;
      case FIXED_INTS_32:
        f = new IntDocValuesField("dv", 0);
        break;
      case FIXED_INTS_64:
        f = new LongDocValuesField("dv", 0L);
        break;
      case VAR_INTS:
        f = new PackedLongDocValuesField("dv", 0L);
        break;
      case FLOAT_32:
        f = new FloatDocValuesField("dv", 0f);
        break;
      case FLOAT_64:
        f = new DoubleDocValuesField("dv", 0d);
        break;
      default:
        throw new AssertionError();
    }
    Document document = new Document();
    document.add(id);
    document.add(f);

    final Object[] vals = new Object[nDocs];

    RandomIndexWriter iw = new RandomIndexWriter(random(), d, iwConfig);
    for (int i = 0; i < nDocs; ++i) {
      id.setIntValue(i);
      switch (type) {
        case BYTES_FIXED_DEREF:
        case BYTES_FIXED_SORTED:
        case BYTES_FIXED_STRAIGHT:
          vals[i] = _TestUtil.randomFixedByteLengthUnicodeString(random(), 10);
          f.setBytesValue(new BytesRef((String) vals[i]));
          break;
        case BYTES_VAR_DEREF:
        case BYTES_VAR_SORTED:
        case BYTES_VAR_STRAIGHT:
          vals[i] = _TestUtil.randomSimpleString(random(), 20);
          f.setBytesValue(new BytesRef((String) vals[i]));
          break;
        case FIXED_INTS_8:
          vals[i] = (byte) random().nextInt(256);
          f.setByteValue((Byte) vals[i]);
          break;
        case FIXED_INTS_16:
          vals[i] = (short) random().nextInt(1 << 16);
          f.setShortValue((Short) vals[i]);
          break;
        case FIXED_INTS_32:
          vals[i] = random().nextInt();
          f.setIntValue((Integer) vals[i]);
          break;
        case FIXED_INTS_64:
        case VAR_INTS:
          final int bitsPerValue = RandomInts.randomIntBetween(random(), 1, 31); // keep it an int
          vals[i] = (long) random().nextInt((int) PackedInts.maxValue(bitsPerValue));
          f.setLongValue((Long) vals[i]);
          break;
        case FLOAT_32:
          vals[i] = random().nextFloat();
          f.setFloatValue((Float) vals[i]);
          break;
        case FLOAT_64:
          vals[i] = random().nextDouble();
          f.setDoubleValue((Double) vals[i]);
          break;
      }
      iw.addDocument(document);
      if (random().nextBoolean() && i % 10 == 9) {
        iw.commit();
      }
    }
    iw.close();

    DirectoryReader rd = DirectoryReader.open(d);
    for (AtomicReaderContext leave : rd.leaves()) {
      final FunctionValues ids = new NumericDocValuesFieldSource("id", false).getValues(null, leave);
      final ValueSource vs;
      final boolean direct = random().nextBoolean();
      switch (type) {
        case BYTES_FIXED_DEREF:
        case BYTES_FIXED_SORTED:
        case BYTES_FIXED_STRAIGHT:
        case BYTES_VAR_DEREF:
        case BYTES_VAR_SORTED:
        case BYTES_VAR_STRAIGHT:
          vs = new StrDocValuesFieldSource("dv", direct);
          break;
        case FLOAT_32:
        case FLOAT_64:
        case FIXED_INTS_8:
        case FIXED_INTS_16:
        case FIXED_INTS_32:
          vs = new NumericDocValuesFieldSource("dv", direct);
          break;
        case FIXED_INTS_64:
        case VAR_INTS:
          if (random().nextBoolean()) {
            vs = new NumericDocValuesFieldSource("dv", direct);
          } else {
            vs = new DateDocValuesFieldSource("dv", direct);
          }
          break;
        default:
          throw new AssertionError();
      }
      final FunctionValues values = vs.getValues(null, leave);
      BytesRef bytes = new BytesRef();
      for (int i = 0; i < leave.reader().maxDoc(); ++i) {
        assertTrue(values.exists(i));
        if (vs instanceof StrDocValuesFieldSource) {
          assertTrue(values.objectVal(i) instanceof String);
        } else if (vs instanceof NumericDocValuesFieldSource) {
          assertTrue(values.objectVal(i) instanceof Number);
          switch (type) {
            case FIXED_INTS_8:
              assertTrue(values.objectVal(i) instanceof Byte);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(1, bytes.length);
              break;
            case FIXED_INTS_16:
              assertTrue(values.objectVal(i) instanceof Short);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(2, bytes.length);
              break;
            case FIXED_INTS_32:
              assertTrue(values.objectVal(i) instanceof Integer);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(4, bytes.length);
              break;
            case FIXED_INTS_64:
            case VAR_INTS:
              assertTrue(values.objectVal(i) instanceof Long);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(8, bytes.length);
              break;
            case FLOAT_32:
              assertTrue(values.objectVal(i) instanceof Float);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(4, bytes.length);
              break;
            case FLOAT_64:
              assertTrue(values.objectVal(i) instanceof Double);
              assertTrue(values.bytesVal(i, bytes));
              assertEquals(8, bytes.length);
              break;
            default:
              throw new AssertionError();
          }
        } else if (vs instanceof DateDocValuesFieldSource) {
          assertTrue(values.objectVal(i) instanceof Date);
        } else {
          throw new AssertionError();
        }
        
        Object expected = vals[ids.intVal(i)];
        switch (type) {
          case BYTES_VAR_SORTED:
          case BYTES_FIXED_SORTED:
            values.ordVal(i); // no exception
            assertTrue(values.numOrd() >= 1);
          case BYTES_FIXED_DEREF:
          case BYTES_FIXED_STRAIGHT:
          case BYTES_VAR_DEREF:
          case BYTES_VAR_STRAIGHT:
            assertEquals(expected, values.objectVal(i));
            assertEquals(expected, values.strVal(i));
            assertEquals(expected, values.objectVal(i));
            assertEquals(expected, values.strVal(i));
            assertTrue(values.bytesVal(i, bytes));
            assertEquals(new BytesRef((String) expected), bytes);
            break;
          case FLOAT_32:
            assertEquals(((Number) expected).floatValue(), values.floatVal(i), 0.001);
            break;
          case FLOAT_64:
            assertEquals(((Number) expected).doubleValue(), values.doubleVal(i), 0.001d);
            break;
          case FIXED_INTS_8:
          case FIXED_INTS_16:
          case FIXED_INTS_32:
          case FIXED_INTS_64:
          case VAR_INTS:
            assertEquals(((Number) expected).longValue(), values.longVal(i));
            break;
        }
      }
    }
    rd.close();
    d.close();
  }

  public void test() throws IOException {
    for (DocValues.Type type : DocValues.Type.values()) {
      test(type);
    }
  }

}
