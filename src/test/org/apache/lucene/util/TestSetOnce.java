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

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.lucene.util.SetOnce.AlreadySetException;
import org.junit.Test;

public class TestSetOnce extends LuceneTestCaseJ4 {

  private static final class SetOnceThread extends Thread {
    SetOnce<Integer> set;
    boolean success = false;
    
    @Override
    public void run() {
      try {
        sleep(RAND.nextInt(10)); // sleep for a short time
        set.set(new Integer(Integer.parseInt(getName().substring(2))));
        success = true;
      } catch (InterruptedException e) {
        // ignore
      } catch (RuntimeException e) {
        // TODO: change exception type
        // expected.
        success = false;
      }
    }
  }
  
  private static final Random RAND = new Random();
  
  @Test
  public void testEmptyCtor() throws Exception {
    SetOnce<Integer> set = new SetOnce<Integer>();
    assertNull(set.get());
  }
  
  @Test(expected=AlreadySetException.class)
  public void testSettingCtor() throws Exception {
    SetOnce<Integer> set = new SetOnce<Integer>(new Integer(5));
    assertEquals(5, set.get().intValue());
    set.set(new Integer(7));
  }
  
  @Test(expected=AlreadySetException.class)
  public void testSetOnce() throws Exception {
    SetOnce<Integer> set = new SetOnce<Integer>();
    set.set(new Integer(5));
    assertEquals(5, set.get().intValue());
    set.set(new Integer(7));
  }
  
  @Test
  public void testSetMultiThreaded() throws Exception {
    long seed = RAND.nextLong();
    RAND.setSeed(seed);
    if (VERBOSE) {
      System.out.println("testSetMultiThreaded: seed=" + seed);
    }
    final SetOnce<Integer> set = new SetOnce<Integer>();
    SetOnceThread[] threads = new SetOnceThread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new SetOnceThread();
      threads[i].setName("t-" + (i+1));
      threads[i].set = set;
    }
    
    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }
    
    for (SetOnceThread t : threads) {
      if (t.success) {
        int expectedVal = Integer.parseInt(t.getName().substring(2));
        assertEquals("thread " + t.getName(), expectedVal, t.set.get().intValue());
      }
    }
  }
  
}
