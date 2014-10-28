package org.apache.lucene.codecs.lucene50;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50NormsConsumer.NormMap;
import org.apache.lucene.index.BaseNormsFormatTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests Lucene49NormsFormat
 */
public class TestLucene50NormsFormat extends BaseNormsFormatTestCase {
  private final Codec codec = TestUtil.getDefaultCodec();
  
  @Override
  protected Codec getCodec() {
    return codec;
  }
  
  // NormMap is rather complicated, doing domain encoding / tracking frequencies etc.
  // test it directly some here...

  public void testNormMapSimple() {
    NormMap map = new NormMap();
    map.add(10);
    map.add(5);
    map.add(4);
    map.add(10);
    assertEquals(3, map.size);
    
    // first come, first serve ord assignment
    
    // encode
    assertEquals(0, map.getOrd(10));
    assertEquals(1, map.getOrd(5));
    assertEquals(2, map.getOrd(4));
    
    // decode
    long decode[] = map.getDecodeTable();
    assertEquals(10, decode[0]);
    assertEquals(5, decode[1]);
    assertEquals(4, decode[2]);
    
    // freqs
    int freqs[] = map.getFreqs();
    assertEquals(2, freqs[0]);
    assertEquals(1, freqs[1]);
    assertEquals(1, freqs[2]);
    
    assertEquals(2, map.maxFreq());
  }
  
  public void testNormMapRandom() {
    Map<Long,Integer> freqs = new HashMap<>();
    Map<Long,Integer> ords = new HashMap<>();
    
    Set<Long> uniqueValuesSet = new HashSet<>();
    int numUniqValues = TestUtil.nextInt(random(), 1, 256);
    for (int i = 0; i < numUniqValues; i++) {
      if (random().nextBoolean()) {
        uniqueValuesSet.add(TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE));
      } else {
        uniqueValuesSet.add(TestUtil.nextLong(random(), Byte.MIN_VALUE, Byte.MAX_VALUE));
      }
    }
    
    Long uniqueValues[] = uniqueValuesSet.toArray(new Long[uniqueValuesSet.size()]);
    
    NormMap map = new NormMap();
    int numdocs = TestUtil.nextInt(random(), 1, 100000);
    for (int i = 0; i < numdocs; i++) {
      long value = uniqueValues[random().nextInt(uniqueValues.length)];
      // now add to both expected and actual
      map.add(value);
      
      Integer ord = ords.get(value);
      if (ord == null) {
        ord = ords.size();
        ords.put(value, ord);
        freqs.put(value, 1);
      } else {
        freqs.put(value, freqs.get(value)+1);
      }
    }
    
    // value -> ord
    assertEquals(ords.size(), map.size);
    for (Map.Entry<Long,Integer> kv : ords.entrySet()) {
      assertEquals(kv.getValue().intValue(), map.getOrd(kv.getKey()));
    }
    
    // ord -> value
    Map<Long,Integer> reversed = new HashMap<>();
    long table[] = map.getDecodeTable();
    for (int i = 0; i < map.size; i++) {
      reversed.put(table[i], i);
    }
    assertEquals(ords, reversed);
    
    // freqs
    int freqTable[] = map.getFreqs();
    for (int i = 0; i < map.size; i++) {
      assertEquals(freqs.get(table[i]).longValue(), freqTable[i]);
    }
  }
}
