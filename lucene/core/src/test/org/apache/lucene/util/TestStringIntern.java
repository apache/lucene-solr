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

package org.apache.lucene.util;
import java.util.Random;

public class TestStringIntern extends LuceneTestCase {
  String[] testStrings;
  String[] internedStrings;

  private String randStr(int len) {
    char[] arr = new char[len];
    for (int i=0; i<len; i++) {
      arr[i] = (char)('a' + random.nextInt(26));
    }
    return new String(arr);
  }

  private void makeStrings(int sz) {
    testStrings = new String[sz];
    internedStrings = new String[sz];
    for (int i=0; i<sz; i++) {
      testStrings[i] = randStr(random.nextInt(8)+3);
    }
  }

  public void testStringIntern() throws InterruptedException {
    makeStrings(1024*10);  // something greater than the capacity of the default cache size
    // makeStrings(100);  // realistic for perf testing
    int nThreads = 20;
    // final int iter=100000;
    final int iter = atLeast(100000);
    
    // try native intern
    // StringHelper.interner = new StringInterner();

    Thread[] threads = new Thread[nThreads];
    for (int i=0; i<nThreads; i++) {
      final int seed = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          Random rand = new Random(seed);
          String[] myInterned = new String[testStrings.length];
          for (int j=0; j<iter; j++) {
            int idx = rand.nextInt(testStrings.length);
            String s = testStrings[idx];
            if (rand.nextBoolean()) s = new String(s); // make a copy half of the time
            String interned = StringHelper.intern(s);
            String prevInterned = myInterned[idx];
            String otherInterned = internedStrings[idx];

            // test against other threads
            if (otherInterned != null && otherInterned != interned) {
              fail();
            }
            internedStrings[idx] = interned;

            // test against local copy
            if (prevInterned != null && prevInterned != interned) {
              fail();
            }
            myInterned[idx] = interned;
          }
        }
      };

      threads[i].start();
    }

    for (int i=0; i<nThreads; i++) {
      threads[i].join();
    }
  }
}
