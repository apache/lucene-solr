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
package org.apache.lucene.codecs.lucene50;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.NormMap;
import org.apache.lucene.index.BaseNormsFormatTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests Lucene50NormsFormat
 */
public class TestLucene50NormsFormat extends BaseNormsFormatTestCase {
  private final Codec codec = new Lucene50RWCodec();
  
  @Override
  protected Codec getCodec() {
    return codec;
  }
  
  // NormMap is rather complicated, doing domain encoding / tracking frequencies etc.
  // test it directly some here...

  public void testNormMapSimple() {
    NormMap map = new NormMap();
    map.add((byte)4);
    map.add((byte) 10);
    map.add((byte) 5);
    map.add((byte)10);
    assertEquals(3, map.size);
    
    // first come, first serve ord assignment
    assertEquals(0, map.ord((byte) 4));
    assertEquals(1, map.ord((byte) 10));
    assertEquals(2, map.ord((byte) 5));
    
    assertEquals(4, map.values[0]);
    assertEquals(10, map.values[1]);
    assertEquals(5, map.values[2]);
    
    assertEquals(1, map.freqs[0]);
    assertEquals(2, map.freqs[1]);
    assertEquals(1, map.freqs[2]);

    // optimizing reorders the ordinals
    map.optimizeOrdinals();
    assertEquals(0, map.ord((byte)10));
    assertEquals(1, map.ord((byte)4));
    assertEquals(2, map.ord((byte)5));

    assertEquals(10, map.values[0]);
    assertEquals(4, map.values[1]);
    assertEquals(5, map.values[2]);

    assertEquals(2, map.freqs[0]);
    assertEquals(1, map.freqs[1]);
    assertEquals(1, map.freqs[2]);
  }
  
  public void testNormMapRandom() {

    Set<Byte> uniqueValuesSet = new HashSet<>();
    int numUniqValues = TestUtil.nextInt(random(), 1, 256);
    for (int i = 0; i < numUniqValues; i++) {
      uniqueValuesSet.add(Byte.valueOf((byte)TestUtil.nextInt(random(), Byte.MIN_VALUE, Byte.MAX_VALUE)));
    }
    Byte uniqueValues[] = uniqueValuesSet.toArray(new Byte[uniqueValuesSet.size()]);

    Map<Byte,Integer> freqs = new HashMap<>();
    NormMap map = new NormMap();
    int numdocs = TestUtil.nextInt(random(), 1, 100000);
    for (int i = 0; i < numdocs; i++) {
      byte value = uniqueValues[random().nextInt(uniqueValues.length)];
      // now add to both expected and actual
      map.add(value);
      if (freqs.containsKey(value)) {
        freqs.put(value, freqs.get(value) + 1);
      } else {
        freqs.put(value, 1);
      }
    }

    assertEquals(freqs.size(), map.size);
    for (Map.Entry<Byte,Integer> kv : freqs.entrySet()) {
      byte value = kv.getKey();
      int freq = kv.getValue();
      int ord = map.ord(value);
      assertEquals(freq, map.freqs[ord]);
      assertEquals(value, map.values[ord]);
    }

    // optimizing should reorder ordinals from greatest to least frequency
    map.optimizeOrdinals();
    // recheck consistency
    assertEquals(freqs.size(), map.size);
    for (Map.Entry<Byte,Integer> kv : freqs.entrySet()) {
      byte value = kv.getKey();
      int freq = kv.getValue();
      int ord = map.ord(value);
      assertEquals(freq, map.freqs[ord]);
      assertEquals(value, map.values[ord]);
    }
    // also check descending freq
    int prevFreq = map.freqs[0];
    for (int i = 1; i < map.size; ++i) {
      assertTrue(prevFreq >= map.freqs[i]);
      prevFreq = map.freqs[i];
    }
  }
}
