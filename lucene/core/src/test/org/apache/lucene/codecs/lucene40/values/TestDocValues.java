package org.apache.lucene.codecs.lucene40.values;

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

import java.io.IOException;
import java.io.Reader;
import java.util.Comparator;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.lucene40.values.Bytes;
import org.apache.lucene.codecs.lucene40.values.Floats;
import org.apache.lucene.codecs.lucene40.values.Ints;
import org.apache.lucene.index.DocValues.SortedSource;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

// TODO: some of this should be under lucene40 codec tests? is talking to codec directly?f
public class TestDocValues extends LuceneTestCase {
  private static final Comparator<BytesRef> COMP = BytesRef.getUTF8SortedAsUnicodeComparator();
  // TODO -- for sorted test, do our own Sort of the
  // values and verify it's identical

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

  public void runTestBytes(final Bytes.Mode mode, final boolean fixedSize)
      throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    valueHolder.comp = COMP;
    final BytesRef bytesRef = new BytesRef();

    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Bytes.getWriter(dir, "test", mode, fixedSize, COMP, trackBytes, newIOContext(random()),
        random().nextBoolean());
    int maxDoc = 220;
    final String[] values = new String[maxDoc];
    final int fixedLength = 1 + atLeast(50);
    for (int i = 0; i < 100; i++) {
      final String s;
      if (i > 0 && random().nextInt(5) <= 2) {
        // use prior value
        s = values[2 * random().nextInt(i)];
      } else {
        s = _TestUtil.randomFixedByteLengthUnicodeString(random(), fixedSize? fixedLength : 1 + random().nextInt(39));
      }
      values[2 * i] = s;

      UnicodeUtil.UTF16toUTF8(s, 0, s.length(), bytesRef);
      valueHolder.bytes = bytesRef;
      w.add(2 * i, valueHolder);
    }
    w.finish(maxDoc);
    assertEquals(0, trackBytes.get());

    DocValues r = Bytes.getValues(dir, "test", mode, fixedSize, maxDoc, COMP, newIOContext(random()));

    // Verify we can load source twice:
    for (int iter = 0; iter < 2; iter++) {
      Source s;
      DocValues.SortedSource ss;
      if (mode == Bytes.Mode.SORTED) {
        // default is unicode so we can simply pass null here
        s = ss = getSortedSource(r);  
      } else {
        s = getSource(r);
        ss = null;
      }
      for (int i = 0; i < 100; i++) {
        final int idx = 2 * i;
        assertNotNull("doc " + idx + "; value=" + values[idx], s.getBytes(idx,
            bytesRef));
        assertEquals("doc " + idx, values[idx], s.getBytes(idx, bytesRef)
            .utf8ToString());
        if (ss != null) {
          assertEquals("doc " + idx, values[idx], ss.getByOrd(ss.ord(idx),
              bytesRef).utf8ToString());
         int ord = ss
              .getOrdByValue(new BytesRef(values[idx]), new BytesRef());
          assertTrue(ord >= 0);
          assertEquals(ss.ord(idx), ord);
        }
      }

      // Lookup random strings:
      if (mode == Bytes.Mode.SORTED) {
        final int valueCount = ss.getValueCount();
        Random random = random();
        for (int i = 0; i < 1000; i++) {
          BytesRef bytesValue = new BytesRef(_TestUtil.randomFixedByteLengthUnicodeString(random, fixedSize? fixedLength : 1 + random.nextInt(39)));
          int ord = ss.getOrdByValue(bytesValue, new BytesRef());
          if (ord >= 0) {
            assertTrue(bytesValue
                .bytesEquals(ss.getByOrd(ord, bytesRef)));
            int count = 0;
            for (int k = 0; k < 100; k++) {
              if (bytesValue.utf8ToString().equals(values[2 * k])) {
                assertEquals(ss.ord(2 * k), ord);
                count++;
              }
            }
            assertTrue(count > 0);
          } else {
            assert ord < 0;
            int insertIndex = (-ord)-1;
            if (insertIndex == 0) {
              final BytesRef firstRef = ss.getByOrd(1, bytesRef);
              // random string was before our first
              assertTrue(firstRef.compareTo(bytesValue) > 0);
            } else if (insertIndex == valueCount) {
              final BytesRef lastRef = ss.getByOrd(valueCount-1, bytesRef);
              // random string was after our last
              assertTrue(lastRef.compareTo(bytesValue) < 0);
            } else {
              // TODO: I don't think this actually needs a deep copy?
              final BytesRef before = BytesRef.deepCopyOf(ss.getByOrd(insertIndex-1, bytesRef));
              BytesRef after = ss.getByOrd(insertIndex, bytesRef);
              assertTrue(COMP.compare(before, bytesValue) < 0);
              assertTrue(COMP.compare(bytesValue, after) < 0);
            }
          }
        }
      }
    }


    r.close();
    dir.close();
  }

  public void testVariableIntsLimits() throws IOException {
    long[][] minMax = new long[][] { { Long.MIN_VALUE, Long.MAX_VALUE },
        { Long.MIN_VALUE + 1, 1 }, { -1, Long.MAX_VALUE },
        { Long.MIN_VALUE, -1 }, { 1, Long.MAX_VALUE },
        { -1, Long.MAX_VALUE - 1 }, { Long.MIN_VALUE + 2, 1 }, };
    Type[] expectedTypes = new Type[] { Type.FIXED_INTS_64,
        Type.FIXED_INTS_64, Type.FIXED_INTS_64,
        Type.FIXED_INTS_64, Type.VAR_INTS, Type.VAR_INTS,
        Type.VAR_INTS, };
    DocValueHolder valueHolder = new DocValueHolder();
    for (int i = 0; i < minMax.length; i++) {
      Directory dir = newDirectory();
      final Counter trackBytes = Counter.newCounter();
      DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, Type.VAR_INTS, newIOContext(random()));
      valueHolder.numberValue = minMax[i][0];
      w.add(0, valueHolder);
      valueHolder.numberValue = minMax[i][1];
      w.add(1, valueHolder);
      w.finish(2);
      assertEquals(0, trackBytes.get());
      DocValues r = Ints.getValues(dir, "test", 2,  Type.VAR_INTS, newIOContext(random()));
      Source source = getSource(r);
      assertEquals(i + " with min: " + minMax[i][0] + " max: " + minMax[i][1],
          expectedTypes[i], source.getType());
      assertEquals(minMax[i][0], source.getInt(0));
      assertEquals(minMax[i][1], source.getInt(1));

      r.close();
      dir.close();
    }
  }
  
  public void testVInts() throws IOException {
    testInts(Type.VAR_INTS, 63);
  }
  
  public void testFixedInts() throws IOException {
    testInts(Type.FIXED_INTS_64, 63);
    testInts(Type.FIXED_INTS_32, 31);
    testInts(Type.FIXED_INTS_16, 15);
    testInts(Type.FIXED_INTS_8, 7);

  }
  
  public void testGetInt8Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    byte[] sourceArray = new byte[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, Type.FIXED_INTS_8, newIOContext(random()));
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = (long) sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Ints.getValues(dir, "test", sourceArray.length, Type.FIXED_INTS_8, newIOContext(random()));
    Source source = r.getSource();
    assertTrue(source.hasArray());
    byte[] loaded = ((byte[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i]);
    }
    r.close();
    dir.close();
  }
  
  public void testGetInt16Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    short[] sourceArray = new short[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, Type.FIXED_INTS_16, newIOContext(random()));
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = (long) sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Ints.getValues(dir, "test", sourceArray.length, Type.FIXED_INTS_16, newIOContext(random()));
    Source source = r.getSource();
    assertTrue(source.hasArray());
    short[] loaded = ((short[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i]);
    }
    r.close();
    dir.close();
  }
  
  public void testGetInt64Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    long[] sourceArray = new long[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, Type.FIXED_INTS_64, newIOContext(random()));
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Ints.getValues(dir, "test", sourceArray.length, Type.FIXED_INTS_64, newIOContext(random()));
    Source source = r.getSource();
    assertTrue(source.hasArray());
    long[] loaded = ((long[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i]);
    }
    r.close();
    dir.close();
  }
  
  public void testGetInt32Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    int[] sourceArray = new int[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, Type.FIXED_INTS_32, newIOContext(random()));
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = (long) sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Ints.getValues(dir, "test", sourceArray.length, Type.FIXED_INTS_32, newIOContext(random()));
    Source source = r.getSource();
    assertTrue(source.hasArray());
    int[] loaded = ((int[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i]);
    }
    r.close();
    dir.close();
  }
  
  public void testGetFloat32Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    float[] sourceArray = new float[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Floats.getWriter(dir, "test", trackBytes, newIOContext(random()), Type.FLOAT_32);
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Floats.getValues(dir, "test", 3, newIOContext(random()), Type.FLOAT_32);
    Source source = r.getSource();
    assertTrue(source.hasArray());
    float[] loaded = ((float[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i], 0.0f);
    }
    r.close();
    dir.close();
  }
  
  public void testGetFloat64Array() throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    double[] sourceArray = new double[] {1,2,3};
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Floats.getWriter(dir, "test", trackBytes, newIOContext(random()), Type.FLOAT_64);
    for (int i = 0; i < sourceArray.length; i++) {
      valueHolder.numberValue = sourceArray[i];
      w.add(i, valueHolder);
    }
    w.finish(sourceArray.length);
    DocValues r = Floats.getValues(dir, "test", 3, newIOContext(random()), Type.FLOAT_64);
    Source source = r.getSource();
    assertTrue(source.hasArray());
    double[] loaded = ((double[])source.getArray());
    assertEquals(loaded.length, sourceArray.length);
    for (int i = 0; i < loaded.length; i++) {
      assertEquals("value didn't match at index " + i, sourceArray[i], loaded[i], 0.0d);
    }
    r.close();
    dir.close();
  }

  private void testInts(Type type, int maxBit) throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    long maxV = 1;
    final int NUM_VALUES = 333 + random().nextInt(333);
    final long[] values = new long[NUM_VALUES];
    for (int rx = 1; rx < maxBit; rx++, maxV *= 2) {
      Directory dir = newDirectory();
      final Counter trackBytes = Counter.newCounter();
      DocValuesConsumer w = Ints.getWriter(dir, "test", trackBytes, type, newIOContext(random()));
      for (int i = 0; i < NUM_VALUES; i++) {
        final long v = random().nextLong() % (1 + maxV);
        valueHolder.numberValue = values[i] = v;
        w.add(i, valueHolder);
      }
      final int additionalDocs = 1 + random().nextInt(9);
      w.finish(NUM_VALUES + additionalDocs);
      assertEquals(0, trackBytes.get());

      DocValues r = Ints.getValues(dir, "test", NUM_VALUES + additionalDocs, type, newIOContext(random()));
      for (int iter = 0; iter < 2; iter++) {
        Source s = getSource(r);
        assertEquals(type, s.getType());
        for (int i = 0; i < NUM_VALUES; i++) {
          final long v = s.getInt(i);
          assertEquals("index " + i, values[i], v);
        }
      }

      r.close();
      dir.close();
    }
  }

  public void testFloats4() throws IOException {
    runTestFloats(Type.FLOAT_32);
  }

  private void runTestFloats(Type type) throws IOException {
    DocValueHolder valueHolder = new DocValueHolder();
    Directory dir = newDirectory();
    final Counter trackBytes = Counter.newCounter();
    DocValuesConsumer w = Floats.getWriter(dir, "test", trackBytes, newIOContext(random()), type);
    final int NUM_VALUES = 777 + random().nextInt(777);
    final double[] values = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      final double v = type == Type.FLOAT_32 ? random().nextFloat() : random()
          .nextDouble();
      valueHolder.numberValue = values[i] = v;
      w.add(i, valueHolder);
    }
    final int additionalValues = 1 + random().nextInt(10);
    w.finish(NUM_VALUES + additionalValues);
    assertEquals(0, trackBytes.get());

    DocValues r = Floats.getValues(dir, "test", NUM_VALUES + additionalValues, newIOContext(random()), type);
    for (int iter = 0; iter < 2; iter++) {
      Source s = getSource(r);
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals("" + i, values[i], s.getFloat(i), 0.0f);
      }
    }
    r.close();
    dir.close();
  }

  public void testFloats8() throws IOException {
    runTestFloats(Type.FLOAT_64);
  }
  

  private Source getSource(DocValues values) throws IOException {
    // getSource uses cache internally
    switch(random().nextInt(5)) {
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
  
  private SortedSource getSortedSource(DocValues values) throws IOException {
    return getSource(values).asSortedSource();
  }
  
  public static class DocValueHolder implements IndexableField {
    BytesRef bytes;
    Number numberValue;
    Comparator<BytesRef> comp;

    @Override
    public TokenStream tokenStream(Analyzer a) {
      return null;
    }

    @Override
    public float boost() {
      return 0.0f;
    }

    @Override
    public String name() {
      return "test";
    }

    @Override
    public BytesRef binaryValue() {
      return bytes;
    }

    @Override
    public Number numericValue() {
      return numberValue;
    }

    @Override
    public String stringValue() {
      return null;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public IndexableFieldType fieldType() {
      return null;
    }
  }
}
