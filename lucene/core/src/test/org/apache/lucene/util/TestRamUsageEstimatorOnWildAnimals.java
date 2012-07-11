package org.apache.lucene.util;

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

import org.junit.Assert;

/**
 * Check large and special graphs. 
 */
public class TestRamUsageEstimatorOnWildAnimals extends LuceneTestCase {
  public static class ListElement {
    ListElement next;
  }

  public void testOverflowMaxChainLength() {
    int UPPERLIMIT = 100000;
    int lower = 0;
    int upper = UPPERLIMIT;
    
    while (lower + 1 < upper) {
      int mid = (lower + upper) / 2;
      try {
        ListElement first = new ListElement();
        ListElement last = first;
        for (int i = 0; i < mid; i++) {
          last = (last.next = new ListElement());
        }
        RamUsageEstimator.sizeOf(first); // cause SOE or pass.
        lower = mid;
      } catch (StackOverflowError e) {
        upper = mid;
      }
    }

    if (lower + 1 < UPPERLIMIT) {
      Assert.fail("Max object chain length till stack overflow: " + lower);
    }
  }  
}
