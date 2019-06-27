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
package org.apache.solr.util.hll;

import org.apache.solr.SolrTestCase;
import org.apache.lucene.util.TestUtil;

import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.EnumSet;

import static org.apache.solr.util.hll.HLL.*;

/**
 * Serialization smoke-tests.
 */
public class HLLSerializationTest extends SolrTestCase {
  
  /**
   * A smoke-test that covers serialization/deserialization of an HLL
   * under most possible init parameters.
   */
  @Test
  @Slow
  @Nightly
  public void serializationSmokeTest() throws Exception {
    final Random random = new Random(randomLong());
    final int randomCount = 250;
    final List<Long> randoms = new ArrayList<Long>(randomCount);
    for (int i=0; i<randomCount; i++) {
      randoms.add(random.nextLong());
    }
    
    // NOTE: log2m<=16 was chosen as the max log2m parameter so that the test
    //       completes in a reasonable amount of time. Not much is gained by
    //       testing larger values
    final int maxLog2m = 16;
    for (HLLType type : EnumSet.allOf(HLLType.class)) {
      assertCardinality(type, maxLog2m, randoms);
    }
  }
  
  /**
   * A smoke-test that covers serialization/deserialization of HLLs
   * under the max possible numeric init parameters, iterating over all possible combinations of 
   * the other params.
   *
   * @see #manyValuesHLLSerializationTest
   */
  @Test
  @Slow
  @Monster("needs roughly -Dtests.heapsize=8g because of the (multiple) massive data structs")
  public void monsterHLLSerializationTest() throws Exception {
    final Random random = new Random(randomLong());
    final int randomCount = 250;
    final List<Long> randoms = new ArrayList<Long>(randomCount);
    for (int i=0; i<randomCount; i++) {
      randoms.add(random.nextLong());
    }

    for (HLLType type : EnumSet.allOf(HLLType.class)) {
      for (boolean sparse : new boolean[] {true, false} ) {
        HLL hll = new HLL(MAXIMUM_LOG2M_PARAM, MAXIMUM_REGWIDTH_PARAM, MAXIMUM_EXPTHRESH_PARAM,
                          sparse, type);
        assertCardinality(hll, randoms);
      }
    }
  }
  
  /**
   * A smoke-test that covers serialization/deserialization of a (single) HLL
   * with random init params with an extremely large number of unique values added to it.
   *
   * @see #monsterHLLSerializationTest
   */
  @Test
  @Slow
  @Monster("may require as much as -Dtests.heapsize=4g depending on random values picked")
  public void manyValuesHLLSerializationTest() throws Exception {

    final HLLType[] ALL_TYPES = EnumSet.allOf(HLLType.class).toArray(new HLLType[0]);
    Arrays.sort(ALL_TYPES);
      
    final int log2m = TestUtil.nextInt(random(), MINIMUM_LOG2M_PARAM, MAXIMUM_LOG2M_PARAM);
    final int regwidth = TestUtil.nextInt(random(), MINIMUM_REGWIDTH_PARAM, MAXIMUM_REGWIDTH_PARAM);
    final int expthresh = TestUtil.nextInt(random(), MINIMUM_EXPTHRESH_PARAM, MAXIMUM_EXPTHRESH_PARAM);
    final boolean sparse = random().nextBoolean();
    final HLLType type = ALL_TYPES[TestUtil.nextInt(random(), 0, ALL_TYPES.length-1)];
    
    HLL hll = new HLL(log2m, regwidth, expthresh, sparse, type);

    final long NUM_VALS = TestUtil.nextLong(random(), 150000, 1000000);
    final long MIN_VAL = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE-NUM_VALS);
    final long MAX_VAL = MIN_VAL + NUM_VALS;
    assert MIN_VAL < MAX_VAL;
    
    for (long val = MIN_VAL; val < MAX_VAL; val++) {
      hll.addRaw(val);
    }
    
    final long expectedCardinality = hll.cardinality();
    final HLLType expectedType = hll.getType();

    byte[] serializedData = hll.toBytes();
    hll = null; // allow some GC
    
    HLL copy = HLL.fromBytes(serializedData);
    serializedData = null; // allow some GC
    
    assertEquals(expectedCardinality, copy.cardinality());
    assertEquals(expectedType, copy.getType());
    
  }
  
  /**
   * A smoke-test that covers serialization/deserialization of a (single) HLL
   * with random the max possible numeric init parameters, with randomized values for the other params.
   *
   * @see #monsterHLLSerializationTest
   */
  @Test
  @Slow
  @Monster("can require as much as -Dtests.heapsize=4g because of the massive data structs")
  public void manyValuesMonsterHLLSerializationTest() throws Exception {

    final HLLType[] ALL_TYPES = EnumSet.allOf(HLLType.class).toArray(new HLLType[0]);
    Arrays.sort(ALL_TYPES);
      
    final boolean sparse = random().nextBoolean();
    final HLLType type = ALL_TYPES[TestUtil.nextInt(random(), 0, ALL_TYPES.length-1)];
    
    HLL hll = new HLL(MAXIMUM_LOG2M_PARAM, MAXIMUM_REGWIDTH_PARAM, MAXIMUM_EXPTHRESH_PARAM, sparse, type);

    final long NUM_VALS = TestUtil.nextLong(random(), 150000, 1000000);
    final long MIN_VAL = TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE-NUM_VALS);
    final long MAX_VAL = MIN_VAL + NUM_VALS;
    assert MIN_VAL < MAX_VAL;
    
    for (long val = MIN_VAL; val < MAX_VAL; val++) {
      hll.addRaw(val);
    }
    
    final long expectedCardinality = hll.cardinality();
    final HLLType expectedType = hll.getType();

    byte[] serializedData = hll.toBytes();
    hll = null; // allow some GC
    
    HLL copy = HLL.fromBytes(serializedData);
    serializedData = null; // allow some GC
    
    assertEquals(expectedCardinality, copy.cardinality());
    assertEquals(expectedType, copy.getType());
    
  }

  /**
   * Iterates over all possible constructor args, with the exception of log2m, 
   * which is only iterated up to the specified max so the test runs in a 
   * "reasonable" amount of time and ram.
   */
  private static void assertCardinality(final HLLType hllType,
                                        final int maxLog2m,
                                        final Collection<Long> items) throws CloneNotSupportedException {
    for(int regw=MINIMUM_REGWIDTH_PARAM; regw<=MAXIMUM_REGWIDTH_PARAM; regw++) {
      for(int expthr=MINIMUM_EXPTHRESH_PARAM; expthr<=MAXIMUM_EXPTHRESH_PARAM; expthr++ ) {
        for(final boolean sparse: new boolean[]{true, false}) {
          for(int log2m=MINIMUM_LOG2M_PARAM; log2m<=maxLog2m; log2m++) {
            assertCardinality(new HLL(log2m, regw, expthr, sparse, hllType), items);
          }
        }
      }
    }
  }

  /**
   * Adds all of the items to the specified hll, then does a round trip serialize/deserialize and confirms
   * equality of several properties (including the byte serialization).  Repeats process with a clone.
   */
  private static void assertCardinality(HLL hll, final Collection<Long> items)
    throws CloneNotSupportedException {
    
    for (final Long item: items) {
      hll.addRaw(item);
    }
    
    final long hllCardinality = hll.cardinality();
    final HLLType hllType = hll.getType();
    final byte[] hllBytes = hll.toBytes();
    hll = null; // allow some GC
    
    HLL copy = HLL.fromBytes(hllBytes);
    assertEquals(copy.cardinality(), hllCardinality);
    assertEquals(copy.getType(), hllType);
    assertTrue(Arrays.equals(copy.toBytes(), hllBytes));
    
    HLL clone = copy.clone();
    copy = null; // allow some GC
    
    assertEquals(clone.cardinality(), hllCardinality);
    assertEquals(clone.getType(), hllType);
    assertTrue(Arrays.equals(clone.toBytes(), hllBytes));
  }
}
