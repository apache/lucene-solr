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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

@SuppressCodecs("Lucene3x")
public class TestDocValuesFieldSources extends LuceneTestCase {

  public void test(DocValuesType type) throws IOException {
    Directory d = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    final int nDocs = atLeast(50);
    final Field id = new NumericDocValuesField("id", 0);
    final Field f;
    switch (type) {
      case BINARY:
        f = new BinaryDocValuesField("dv", new BytesRef());
        break;
      case SORTED:
        f = new SortedDocValuesField("dv", new BytesRef());
        break;
      case NUMERIC:
        f = new NumericDocValuesField("dv", 0);
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
      id.setLongValue(i);
      switch (type) {
        case SORTED:
        case BINARY:
          do {
            vals[i] = _TestUtil.randomSimpleString(random(), 20);
          } while (((String) vals[i]).isEmpty());
          f.setBytesValue(new BytesRef((String) vals[i]));
          break;
        case NUMERIC:
          final int bitsPerValue = RandomInts.randomIntBetween(random(), 1, 31); // keep it an int
          vals[i] = (long) random().nextInt((int) PackedInts.maxValue(bitsPerValue));
          f.setLongValue((Long) vals[i]);
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
      final FunctionValues ids = new LongFieldSource("id").getValues(null, leave);
      final ValueSource vs;
      switch (type) {
        case BINARY:
        case SORTED:
          vs = new BytesRefFieldSource("dv");
          break;
        case NUMERIC:
          vs = new LongFieldSource("dv");
          break;
        default:
          throw new AssertionError();
      }
      final FunctionValues values = vs.getValues(null, leave);
      BytesRef bytes = new BytesRef();
      for (int i = 0; i < leave.reader().maxDoc(); ++i) {
        assertTrue(values.exists(i));
        if (vs instanceof BytesRefFieldSource) {
          assertTrue(values.objectVal(i) instanceof String);
        } else if (vs instanceof LongFieldSource) {
          assertTrue(values.objectVal(i) instanceof Long);
          assertTrue(values.bytesVal(i, bytes));
        } else {
          throw new AssertionError();
        }
        
        Object expected = vals[ids.intVal(i)];
        switch (type) {
          case SORTED:
            values.ordVal(i); // no exception
            assertTrue(values.numOrd() >= 1);
          case BINARY:
            assertEquals(expected, values.objectVal(i));
            assertEquals(expected, values.strVal(i));
            assertEquals(expected, values.objectVal(i));
            assertEquals(expected, values.strVal(i));
            assertTrue(values.bytesVal(i, bytes));
            assertEquals(new BytesRef((String) expected), bytes);
            break;
          case NUMERIC:
            assertEquals(((Number) expected).longValue(), values.longVal(i));
            break;
        }
      }
    }
    rd.close();
    d.close();
  }

  public void test() throws IOException {
    for (DocValuesType type : DocValuesType.values()) {
      if (type != DocValuesType.SORTED_SET) {
        test(type);
      }
    }
  }

}
