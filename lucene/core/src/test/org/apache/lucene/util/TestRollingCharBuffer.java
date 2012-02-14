package org.apache.lucene.util;

/**
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

import java.io.StringReader;

public class TestRollingCharBuffer extends LuceneTestCase {

  public void test() throws Exception {
    final int ITERS = atLeast(1000);
    
    RollingCharBuffer buffer = new RollingCharBuffer();

    for(int iter=0;iter<ITERS;iter++) {
      String s = _TestUtil.randomUnicodeString(random, 10000);
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s.length()=" + s.length());
      }
      buffer.reset(new StringReader(s));
      int nextRead = 0;
      int availCount = 0;
      while(nextRead < s.length()) {
        if (VERBOSE) {
          System.out.println("  cycle nextRead=" + nextRead + " avail=" + availCount);
        }
        if (availCount == 0 || random.nextBoolean()) {
          if (VERBOSE) {
            System.out.println("    new char");
          }
          assertEquals(s.charAt(nextRead), buffer.get(nextRead));
          nextRead++;
          availCount++;
        } else {
          int pos = _TestUtil.nextInt(random, nextRead-availCount, nextRead-1);
          if (VERBOSE) {
            System.out.println("    old char pos=" + pos);
          }
          assertEquals(s.charAt(pos), buffer.get(pos));
        }

        if (availCount > 0 && random.nextInt(20) == 17) {
          final int toFree = random.nextInt(availCount);
          if (VERBOSE) {
            System.out.println("    free " + toFree + " (avail=" + (availCount-toFree) + ")");
          }
          buffer.freeBefore(nextRead-(availCount-toFree));
          availCount -= toFree;
        }
      }
    }
  }
}
