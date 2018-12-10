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
package org.apache.lucene.util;

import java.util.Locale;
import java.util.Random;

public class TestRankBitSet extends LuceneTestCase {

  public void testSingleWord() {
    RankBitSet rank = new RankBitSet(60);
    rank.set(20);
    rank.buildRankCache();

    assertEquals("The rank at index 20 should be correct", 0, rank.rank(20));
    assertEquals("The rank at index 21 should be correct", 1, rank.rank(21));
  }

  public void testSecondWord() {
    RankBitSet rank = new RankBitSet(100);
    rank.set(70);
    rank.buildRankCache();

    assertEquals("The rank at index 70 should be correct", 0, rank.rank(70));
    assertEquals("The rank at index 71 should be correct", 1, rank.rank(71));
  }

  public void testThirdWord() {
    RankBitSet rank = new RankBitSet(200);
    rank.set(130);
    rank.buildRankCache();

    assertEquals("The rank at index 130 should be correct", 0, rank.rank(130));
    assertEquals("The rank at index 131 should be correct", 1, rank.rank(131));
  }

  public void testSecondLower() {
    RankBitSet rank = new RankBitSet(3000);
    rank.set(2500);
    rank.buildRankCache();

    assertEquals("The rank at index 2500 should be correct", 0, rank.rank(2500));
    assertEquals("The rank at index 2500 should be correct", 1, rank.rank(2501));
  }

  public void testSpecific282() {
    RankBitSet rank = new RankBitSet(448);
    rank.set(282);
    rank.buildRankCache();

    assertEquals("The rank at index 288 should be correct", 1, rank.rank(288));
  }

  public void testSpecific1031() {
    RankBitSet rank = new RankBitSet(1446);
    rank.set(1031);
    rank.buildRankCache();

    assertEquals("The rank at index 1057 should be correct", 1, rank.rank(1057));
  }

  public void testMonkeys() {
    monkey(20, 8000, 40);
  }

  @Slow
  public void testManyMonkeys() {
    monkey(20, 100000, 400);
  }

  public void monkey(int runs, int sizeMax, int setMax) {
    Random random = random();
    //Random random = new Random(87);
    for (int run = 0 ; run < runs ; run++) {
      final int size = random.nextInt(sizeMax-1)+1;
      RankBitSet rank = new RankBitSet(size);
      int doSet = random.nextInt(setMax);
      for (int s = 0 ; s < doSet ; s++) {
        int index = random.nextInt(size);
        rank.set(index);
      }
      rank.buildRankCache();
      int setbits = 0;
      for (int i = 0 ; i < size ; i++) {
        assertEquals(String.format(Locale.ENGLISH, "run=%d, index=%d/%d, setbits=%d", run, i, size, setbits),
            setbits, rank.rank(i));
        if (rank.get(i)) {
          setbits++;
        }
      }
    }
  }
}
