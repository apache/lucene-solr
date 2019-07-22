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

import org.apache.lucene.misc.CollectorMemoryTracker;

public class TestCollectorMemoryTracker extends LuceneTestCase {
  public void testAdditionsAndDeletions() {
    long perCollectorMemoryLimit = 100; //100 Bytes
    CollectorMemoryTracker collectorMemoryTracker = new CollectorMemoryTracker("testMemoryTracker",
        perCollectorMemoryLimit);

    collectorMemoryTracker.updateBytes(50);
    assertEquals(collectorMemoryTracker.getBytes(), 50);
    collectorMemoryTracker.updateBytes(-30);
    assertEquals(collectorMemoryTracker.getBytes(), 20);
    expectThrows(IllegalStateException.class, () -> {
      collectorMemoryTracker.updateBytes(130);
    });
    collectorMemoryTracker.updateBytes(-110);
    assertEquals(collectorMemoryTracker.getBytes(), 40);

    expectThrows(IllegalStateException.class, () -> {
      collectorMemoryTracker.updateBytes(-90);
    });
  }
}
