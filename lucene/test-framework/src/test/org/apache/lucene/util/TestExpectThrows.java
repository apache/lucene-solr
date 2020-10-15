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

import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;

import org.junit.internal.AssumptionViolatedException;
  
public class TestExpectThrows extends LuceneTestCase {

  private static class HuperDuperException extends IOException {
    public HuperDuperException() {
      /* No-Op */
    }
  }
  
  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable throws (an 
   * instance of a subclass of) the expected Exception type: by returning that Exception.
   */
  public void testPass() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    final IOException returned = expectThrows(IOException.class, () -> {
        ran.getAndSet(true);
        throw new HuperDuperException();
      });
    assertTrue(ran.get());
    assertNotNull(returned);
    assertEquals(HuperDuperException.class, returned.getClass());
  }
  
  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable does not throw (an 
   * instance of a subclass of) the expected Exception type: by throwing an assertion to 
   * <code>FAIL</code> the test.
   */
  public void testFail() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    AssertionError caught = null;
    try {
      final IOException returned = expectThrows(IOException.class, () -> {
          ran.getAndSet(true);
        });
      fail("must not complete"); // NOTE: we don't use expectThrows to test expectThrows
    } catch (AssertionError ae) {
      caught = ae;
    }
    assertTrue(ran.get());
    assertNotNull(caught);
    assertEquals("Expected exception IOException but no exception was thrown", caught.getMessage());
                 
  }

  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable contains an  
   * assertion that does not pass: by allowing that assertion to propogate and 
   * <code>FAIL</code> the test.
   */
  public void testNestedFail() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    AssertionError caught = null;
    try {
      final IOException returned = expectThrows(IOException.class, () -> {
          ran.getAndSet(true);
          fail("this failure should propogate");
        });
      fail("must not complete"); // NOTE: we don't use expectThrows to test expectThrows
    } catch (AssertionError ae) {
      caught = ae;
    }
    assertTrue(ran.get());
    assertNotNull(caught);
    assertEquals("this failure should propogate", caught.getMessage());
  }
  
  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable contains an 
   * assumption that does not pass: by allowing that assumption to propogate and cause 
   * the test to <code>SKIP</code>.
   */
  public void testNestedAssume() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    AssumptionViolatedException caught = null;
    try {
      final IOException returned = expectThrows(IOException.class, () -> {
          ran.getAndSet(true);
          assumeTrue("this assumption should propogate", false);
        });
      fail("must not complete"); // NOTE: we don't use expectThrows to test expectThrows
    } catch (AssumptionViolatedException ave) {
      caught = ave;
    }
    assertTrue(ran.get());
    assertNotNull(caught);
    assertEquals("this assumption should propogate", caught.getMessage());
  }

  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable contains an  
   * assertion that does not pass but the caller has explicitly said they expect an Exception of that type:
   * by returning that assertion failure Exception.
   */
  public void testExpectingNestedFail() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    AssertionError returned = null;
    try {
      returned = expectThrows(AssertionError.class, () -> {
          ran.getAndSet(true);
          fail("this failure should be returned, not propogated");
        });
    } catch (AssertionError caught) { // NOTE: we don't use expectThrows to test expectThrows
      assertNull("An exception should not have been thrown", caught);
    }
    assertTrue(ran.get());
    assertNotNull(returned);
    assertEquals("this failure should be returned, not propogated", returned.getMessage());
  }
  
  /** 
   * Tests that {@link #expectThrows} behaves correctly when the Runnable contains an 
   * assumption that does not pass but the caller has explicitly said they expect an Exception of that type: 
   * by returning that assumption failure Exception.
   */
  public void testExpectingNestedAssume() {
    final AtomicBoolean ran = new AtomicBoolean(false);
    AssumptionViolatedException returned = null;
    try {
      returned = expectThrows(AssumptionViolatedException.class, () -> {
          ran.getAndSet(true);
          assumeTrue("this assumption should be returned, not propogated", false);
        });
    } catch (AssumptionViolatedException caught) { // NOTE: we don't use expectThrows to test expectThrows
      assertNull("An exception should not have been thrown", caught);
    }
    assertTrue(ran.get());
    assertNotNull(returned);
    assertEquals("this assumption should be returned, not propogated", returned.getMessage());
  }
  
}
