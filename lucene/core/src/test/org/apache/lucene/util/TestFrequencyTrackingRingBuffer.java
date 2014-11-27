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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFrequencyTrackingRingBuffer extends LuceneTestCase {

  private static <T> void assertBuffer(FrequencyTrackingRingBuffer<T> buffer, int maxSize, List<T> items) {
    final List<T> recentItems;
    if (items.size() <= maxSize) {
      recentItems = items;
    } else {
      recentItems = items.subList(items.size() - maxSize, items.size());
    }
    final Map<T, Integer> expectedFrequencies = new HashMap<T, Integer>();
    for (T item : recentItems) {
      final Integer freq = expectedFrequencies.get(item);
      if (freq == null) {
        expectedFrequencies.put(item, 1);
      } else {
        expectedFrequencies.put(item, freq + 1);
      }
    }
    assertEquals(expectedFrequencies, buffer.asFrequencyMap());
  }

  public void test() {
    final int iterations = atLeast(100);
    for (int i = 0; i < iterations; ++i) {
      final int maxSize = 1 + random().nextInt(100);
      final int numitems = random().nextInt(500);
      final int maxitem = 1 + random().nextInt(100);
      List<Integer> items = new ArrayList<>();
      FrequencyTrackingRingBuffer<Integer> buffer = new FrequencyTrackingRingBuffer<>(maxSize);
      for (int j = 0; j < numitems; ++j) {
        final Integer item = random().nextInt(maxitem);
        items.add(item);
        buffer.add(item);
      }
      assertBuffer(buffer, maxSize, items);
    }
  }

}
