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

import org.junit.Ignore;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Timeout;

public class TestWorstCaseTestBehavior extends LuceneTestCase {
  @Ignore
  public void testThreadLeak() {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          // Ignore.
        }
      }
    };
    t.start();

    while (!t.isAlive()) {
      Thread.yield();
    }

    // once alive, leave it to run outside of the test scope.
  }

  @Ignore
  public void testLaaaaaargeOutput() throws Exception {
    String message = "I will not OOM on large output";
    int howMuch = 250 * 1024 * 1024;
    for (int i = 0; i < howMuch; i++) {
      if (i > 0) System.out.print(",\n");
      System.out.print(message);
      howMuch -= message.length(); // approximately.
    }
    System.out.println(".");
  }

  @Ignore
  public void testProgressiveOutput() throws Exception {
    for (int i = 0; i < 20; i++) {
      System.out.println("Emitting sysout line: " + i);
      System.err.println("Emitting syserr line: " + i);
      System.out.flush();
      System.err.flush();
      RandomizedTest.sleep(1000);
    }
  }

  @Ignore
  public void testUncaughtException() throws Exception {
    Thread t = new Thread() {
      @Override
      public void run() {
        throw new RuntimeException("foobar");
      }
    };
    t.start();
    t.join();
  }
  
  @Ignore
  @Timeout(millis = 500)
  public void testTimeout() throws Exception {
    Thread.sleep(5000);
  }
  
  @Ignore
  @Timeout(millis = 1000)
  public void testZombie() throws Exception {
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }
}
