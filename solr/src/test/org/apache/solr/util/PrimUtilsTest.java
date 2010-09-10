package org.apache.solr.util;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.LuceneTestCase;

import java.util.Arrays;

public class PrimUtilsTest extends LuceneTestCase {

  public void testSort() {
    int maxSize = 100;
    int maxVal = 100;
    int[] a = new int[maxSize];
    int[] b = new int[maxSize];

    PrimUtils.IntComparator comparator = new PrimUtils.IntComparator() {
      @Override
      public int compare(int a, int b) {
        return b - a;     // sort in reverse
      }
    };

    for (int iter=0; iter<100; iter++) {
      int start = random.nextInt(maxSize+1);
      int end = start==maxSize ? maxSize : start + random.nextInt(maxSize-start);
      for (int i=start; i<end; i++) {
        a[i] = b[i] = random.nextInt(maxVal);
      }
      PrimUtils.sort(start, end, a, comparator);
      Arrays.sort(b, start, end);
      for (int i=start; i<end; i++) {
        assertEquals(a[i], b[end-(i-start+1)]);
      }
    }
  }
}