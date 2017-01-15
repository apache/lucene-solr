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


import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

@Slow
public class TestTimSorterWorstCase extends LuceneTestCase {

  public void testWorstCaseStackSize() {
    // we need large arrays to be able to reproduce this bug
    // but not so big we blow up available heap.
    final int length;
    if (TEST_NIGHTLY) {
      length = RandomNumbers.randomIntBetween(random(), 140000000, 400000000);
    } else {
      length = RandomNumbers.randomIntBetween(random(), 140000000, 200000000);
    }
    final PackedInts.Mutable arr = generateWorstCaseArray(length);
    new TimSorter(0) {

      @Override
      protected void swap(int i, int j) {
        final long tmp = arr.get(i);
        arr.set(i, arr.get(j));
        arr.set(j, tmp);
      }

      @Override
      protected int compare(int i, int j) {
        return Long.compare(arr.get(i), arr.get(j));
      }

      @Override
      protected void save(int i, int len) {
        throw new UnsupportedOperationException();
      }

      @Override
      protected void restore(int i, int j) {
        throw new UnsupportedOperationException();
      }

      @Override
      protected void copy(int src, int dest) {
        arr.set(dest, arr.get(src));
      }

      @Override
      protected int compareSaved(int i, int j) {
        throw new UnsupportedOperationException();
      }
    }.sort(0, length);
  }

  /** Create an array for the given list of runs. */
  private static PackedInts.Mutable createArray(int length, List<Integer> runs) {
    PackedInts.Mutable array = PackedInts.getMutable(length, 1, 0);
    int endRun = -1;
    for (long len : runs) {
      array.set(endRun += len, 1);
    }
    array.set(length - 1, 0);
    return array;
  }

  /** Create an array that triggers a worst-case sequence of run lens. */
  public static PackedInts.Mutable generateWorstCaseArray(int length) {
    final int minRun = TimSorter.minRun(length);
    final List<Integer> runs = runsWorstCase(length, minRun);
    return createArray(length, runs);
  }

  //
  // Code below is borrowed from https://github.com/abstools/java-timsort-bug/blob/master/TestTimSort.java
  //

  /**
   * Fills <code>runs</code> with a sequence of run lengths of the form<br>
   * Y_n     x_{n,1}   x_{n,2}   ... x_{n,l_n} <br>
   * Y_{n-1} x_{n-1,1} x_{n-1,2} ... x_{n-1,l_{n-1}} <br>
   * ... <br>
   * Y_1     x_{1,1}   x_{1,2}   ... x_{1,l_1}<br>
   * The Y_i's are chosen to satisfy the invariant throughout execution,
   * but the x_{i,j}'s are merged (by <code>TimSort.mergeCollapse</code>)
   * into an X_i that violates the invariant.
   */
  private static List<Integer> runsWorstCase(int length, int minRun) {
    List<Integer> runs = new LinkedList<>();

    int runningTotal = 0, Y=minRun+4, X=minRun;

    while((long) runningTotal+Y+X <= length) {
      runningTotal += X + Y;
      generateWrongElem(X, minRun, runs);
      runs.add(0,Y);

      // X_{i+1} = Y_i + x_{i,1} + 1, since runs.get(1) = x_{i,1}
      X = Y + runs.get(1) + 1;

      // Y_{i+1} = X_{i+1} + Y_i + 1
      Y += X + 1;
    }

    if((long) runningTotal + X <= length) {
      runningTotal += X;
      generateWrongElem(X, minRun, runs);
    }

    runs.add(length-runningTotal);
    return runs;
  }

  /**
   * Adds a sequence x_1, ..., x_n of run lengths to <code>runs</code> such that:<br>
   * 1. X = x_1 + ... + x_n <br>
   * 2. x_j >= minRun for all j <br>
   * 3. x_1 + ... + x_{j-2}  <  x_j  <  x_1 + ... + x_{j-1} for all j <br>
   * These conditions guarantee that TimSort merges all x_j's one by one
   * (resulting in X) using only merges on the second-to-last element.
   * @param X  The sum of the sequence that should be added to runs.
   */
  private static void generateWrongElem(int X, int minRun, List<Integer> runs) {
    for(int newTotal; X >= 2*minRun+1; X = newTotal) {
      //Default strategy
      newTotal = X/2 + 1;

      //Specialized strategies
      if(3*minRun+3 <= X && X <= 4*minRun+1) {
        // add x_1=MIN+1, x_2=MIN, x_3=X-newTotal  to runs
        newTotal = 2*minRun+1;
      } else if(5*minRun+5 <= X && X <= 6*minRun+5) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=X-newTotal  to runs
        newTotal = 3*minRun+3;
      } else if(8*minRun+9 <= X && X <= 10*minRun+9) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=2MIN+2, x_5=X-newTotal  to runs
        newTotal = 5*minRun+5;
      } else if(13*minRun+15 <= X && X <= 16*minRun+17) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=2MIN+2, x_5=3MIN+4, x_6=X-newTotal  to runs
        newTotal = 8*minRun+9;
      }
      runs.add(0, X-newTotal);
    }
    runs.add(0, X);
  }

}
