package org.apache.lucene.facet.encoding;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.encoding.DGapIntEncoder;
import org.apache.lucene.facet.encoding.DGapVInt8IntEncoder;
import org.apache.lucene.facet.encoding.EightFlagsIntEncoder;
import org.apache.lucene.facet.encoding.FourFlagsIntEncoder;
import org.apache.lucene.facet.encoding.IntDecoder;
import org.apache.lucene.facet.encoding.IntEncoder;
import org.apache.lucene.facet.encoding.NOnesIntEncoder;
import org.apache.lucene.facet.encoding.SimpleIntEncoder;
import org.apache.lucene.facet.encoding.SortingIntEncoder;
import org.apache.lucene.facet.encoding.UniqueValuesIntEncoder;
import org.apache.lucene.facet.encoding.VInt8IntEncoder;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class EncodingTest extends FacetTestCase {

  private static IntsRef uniqueSortedData, data;
  
  @BeforeClass
  public static void beforeClassEncodingTest() throws Exception {
    int capacity = atLeast(10000);
    data = new IntsRef(capacity);
    for (int i = 0; i < 10; i++) {
      data.ints[i] = i + 1; // small values
    }
    for (int i = 10; i < data.ints.length; i++) {
      data.ints[i] = random().nextInt(Integer.MAX_VALUE - 1) + 1; // some encoders don't allow 0
    }
    data.length = data.ints.length;
    
    uniqueSortedData = IntsRef.deepCopyOf(data);
    Arrays.sort(uniqueSortedData.ints);
    uniqueSortedData.length = 0;
    int prev = -1;
    for (int i = 0; i < uniqueSortedData.ints.length; i++) {
      if (uniqueSortedData.ints[i] != prev) {
        uniqueSortedData.ints[uniqueSortedData.length++] = uniqueSortedData.ints[i];
        prev = uniqueSortedData.ints[i];
      }
    }
  }
  
  private static void encoderTest(IntEncoder encoder, IntsRef data, IntsRef expected) throws IOException {
    // ensure toString is implemented
    String toString = encoder.toString();
    assertFalse(toString.startsWith(encoder.getClass().getName() + "@"));
    IntDecoder decoder = encoder.createMatchingDecoder();
    toString = decoder.toString();
    assertFalse(toString.startsWith(decoder.getClass().getName() + "@"));
    
    BytesRef bytes = new BytesRef(100); // some initial capacity - encoders should grow the byte[]
    IntsRef values = new IntsRef(100); // some initial capacity - decoders should grow the int[]
    for (int i = 0; i < 2; i++) {
      // run 2 iterations to catch encoders/decoders which don't reset properly
      encoding(encoder, data, bytes);
      decoding(bytes, values, encoder.createMatchingDecoder());
      assertTrue(expected.intsEquals(values));
    }
  }

  private static void encoding(IntEncoder encoder, IntsRef data, BytesRef bytes) throws IOException {
    final IntsRef values;
    if (random().nextBoolean()) { // randomly set the offset
      values = new IntsRef(data.length + 1);
      System.arraycopy(data.ints, 0, values.ints, 1, data.length);
      values.offset = 1; // ints start at index 1
      values.length = data.length;
    } else {
      // need to copy the array because it may be modified by encoders (e.g. sorting)
      values = IntsRef.deepCopyOf(data);
    }
    encoder.encode(values, bytes);
  }

  private static void decoding(BytesRef bytes, IntsRef values, IntDecoder decoder) throws IOException {
    int offset = 0;
    if (random().nextBoolean()) { // randomly set the offset and length to other than 0,0
      bytes.grow(bytes.length + 1); // ensure that we have enough capacity to shift values by 1
      bytes.offset = 1; // bytes start at index 1 (must do that after grow)
      System.arraycopy(bytes.bytes, 0, bytes.bytes, 1, bytes.length);
      offset = 1;
    }
    decoder.decode(bytes, values);
    assertEquals(offset, bytes.offset); // decoders should not mess with offsets
  }

  @Test
  public void testVInt8() throws Exception {
    encoderTest(new VInt8IntEncoder(), data, data);
    
    // cover negative numbers;
    BytesRef bytes = new BytesRef(5);
    IntEncoder enc = new VInt8IntEncoder();
    IntsRef values = new IntsRef(1);
    values.ints[values.length++] = -1;
    enc.encode(values, bytes);
    
    IntDecoder dec = enc.createMatchingDecoder();
    values.length = 0;
    dec.decode(bytes, values);
    assertEquals(1, values.length);
    assertEquals(-1, values.ints[0]);
  }
  
  @Test
  public void testSimpleInt() throws Exception {
    encoderTest(new SimpleIntEncoder(), data, data);
  }
  
  @Test
  public void testSortingUniqueValues() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new VInt8IntEncoder())), data, uniqueSortedData);
  }

  @Test
  public void testSortingUniqueDGap() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEncoder()))), data, uniqueSortedData);
  }

  @Test
  public void testSortingUniqueDGapEightFlags() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new EightFlagsIntEncoder()))), data, uniqueSortedData);
  }

  @Test
  public void testSortingUniqueDGapFourFlags() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new FourFlagsIntEncoder()))), data, uniqueSortedData);
  }

  @Test
  public void testSortingUniqueDGapNOnes4() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new NOnesIntEncoder(4)))), data, uniqueSortedData);
  }
  
  @Test
  public void testSortingUniqueDGapNOnes3() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new NOnesIntEncoder(3)))), data, uniqueSortedData);
  }
  
  @Test
  public void testSortingUniqueDGapVInt() throws Exception {
    encoderTest(new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapVInt8IntEncoder())), data, uniqueSortedData);
  }

}
