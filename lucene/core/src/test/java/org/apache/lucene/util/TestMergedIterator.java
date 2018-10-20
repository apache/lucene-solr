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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

public class TestMergedIterator extends LuceneTestCase {
  private static final int REPEATS = 2;
  private static final int VALS_TO_MERGE = 15000;

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testMergeEmpty() {
    Iterator<Integer> merged = new MergedIterator<>();
    assertFalse(merged.hasNext());

    merged = new MergedIterator<>(new ArrayList<Integer>().iterator());
    assertFalse(merged.hasNext());

    Iterator<Integer>[] itrs = new Iterator[random().nextInt(100)];
    for (int i = 0; i < itrs.length; i++) {
      itrs[i] = new ArrayList<Integer>().iterator();
    }
    merged = new MergedIterator<>( itrs );
    assertFalse(merged.hasNext());
  }

  @Repeat(iterations = REPEATS)
  public void testNoDupsRemoveDups() {
    testCase(1, 1, true);
  }

  @Repeat(iterations = REPEATS)
  public void testOffItrDupsRemoveDups() {
    testCase(3, 1, true);
  }

  @Repeat(iterations = REPEATS)
  public void testOnItrDupsRemoveDups() {
    testCase(1, 3, true);
  }

  @Repeat(iterations = REPEATS)
  public void testOnItrRandomDupsRemoveDups() {
    testCase(1, -3, true);
  }

  @Repeat(iterations = REPEATS)
  public void testBothDupsRemoveDups() {
    testCase(3, 3, true);
  }

  @Repeat(iterations = REPEATS)
  public void testBothDupsWithRandomDupsRemoveDups() {
    testCase(3, -3, true);
  }

  @Repeat(iterations = REPEATS)
  public void testNoDupsKeepDups() {
    testCase(1, 1, false);
  }

  @Repeat(iterations = REPEATS)
  public void testOffItrDupsKeepDups() {
    testCase(3, 1, false);
  }

  @Repeat(iterations = REPEATS)
  public void testOnItrDupsKeepDups() {
    testCase(1, 3, false);
  }

  @Repeat(iterations = REPEATS)
  public void testOnItrRandomDupsKeepDups() {
    testCase(1, -3, false);
  }

  @Repeat(iterations = REPEATS)
  public void testBothDupsKeepDups() {
    testCase(3, 3, false);
  }

  @Repeat(iterations = REPEATS)
  public void testBothDupsWithRandomDupsKeepDups() {
    testCase(3, -3, false);
  }

  private void testCase(int itrsWithVal, int specifiedValsOnItr, boolean removeDups) {
    // Build a random number of lists
    List<Integer> expected = new ArrayList<>();
    Random random = new Random(random().nextLong());
    int numLists = itrsWithVal + random.nextInt(1000 - itrsWithVal);
    @SuppressWarnings({"rawtypes", "unchecked"})
    List<Integer>[] lists = new List[numLists];
    for (int i = 0; i < numLists; i++) {
      lists[i] = new ArrayList<>();
    }
    int start = random.nextInt(1000000);
    int end = start + VALS_TO_MERGE / itrsWithVal / Math.abs(specifiedValsOnItr);
    for (int i = start; i < end; i++) {
      int maxList = lists.length;
      int maxValsOnItr = 0;
      int sumValsOnItr = 0;
      for (int itrWithVal = 0; itrWithVal < itrsWithVal; itrWithVal++) {
        int list = random.nextInt(maxList);
        int valsOnItr = specifiedValsOnItr < 0 ? (1 + random.nextInt(-specifiedValsOnItr)) : specifiedValsOnItr;
        maxValsOnItr = Math.max(maxValsOnItr, valsOnItr);
        sumValsOnItr += valsOnItr;
        for (int valOnItr = 0; valOnItr < valsOnItr; valOnItr++) {
          lists[list].add(i);
        }
        maxList = maxList - 1;
        ArrayUtil.swap(lists, list, maxList);
      }
      int maxCount = removeDups ? maxValsOnItr : sumValsOnItr;
      for (int count = 0; count < maxCount; count++) {
        expected.add(i);
      }
    }
    // Now check that they get merged cleanly
    @SuppressWarnings({"rawtypes", "unchecked"})
    Iterator<Integer>[] itrs = new Iterator[numLists];
    for (int i = 0; i < numLists; i++) {
      itrs[i] = lists[i].iterator();
    }
    
    MergedIterator<Integer> mergedItr = new MergedIterator<>(removeDups, itrs);
    Iterator<Integer> expectedItr = expected.iterator();
    while (expectedItr.hasNext()) {
      assertTrue(mergedItr.hasNext());
      assertEquals(expectedItr.next(), mergedItr.next()); 
    }
    assertFalse(mergedItr.hasNext());
  }
}
