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
package org.apache.lucene.store;


import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.util.LuceneTestCase;

public class TestBufferedChecksum extends LuceneTestCase {

  public void testSimple() {
    Checksum c = new BufferedChecksum(new CRC32());
    c.update(1);
    c.update(2);
    c.update(3);
    assertEquals(1438416925L, c.getValue());
  }
  
  public void testRandom() {
    Checksum c1 = new CRC32();
    Checksum c2 = new BufferedChecksum(new CRC32());
    int iterations = atLeast(10000);
    for (int i = 0; i < iterations; i++) {
      switch(random().nextInt(4)) {
        case 0:
          // update(byte[], int, int)
          int length = random().nextInt(1024);
          byte bytes[] = new byte[length];
          random().nextBytes(bytes);
          c1.update(bytes, 0, bytes.length);
          c2.update(bytes, 0, bytes.length);
          break;
        case 1:
          // update(int)
          int b = random().nextInt(256);
          c1.update(b);
          c2.update(b);
          break;
        case 2:
          // reset()
          c1.reset();
          c2.reset();
          break;
        case 3:
          // getValue()
          assertEquals(c1.getValue(), c2.getValue());
          break;
      }
    }
    assertEquals(c1.getValue(), c2.getValue());
  }
}
