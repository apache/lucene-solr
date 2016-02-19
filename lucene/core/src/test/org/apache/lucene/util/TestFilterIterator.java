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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.NoSuchElementException;

public class TestFilterIterator extends LuceneTestCase {

  private static final Set<String> set = new TreeSet<>(Arrays.asList("a", "b", "c"));

  private static void assertNoMore(Iterator<?> it) {
    assertFalse(it.hasNext());
    expectThrows(NoSuchElementException.class, () -> {
      it.next();
    });
    assertFalse(it.hasNext());
  }

  public void testEmpty() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return false;
      }
    };
    assertNoMore(it);
  }
    
  public void testA1() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return "a".equals(s);
      }
    };
    assertTrue(it.hasNext());
    assertEquals("a", it.next());
    assertNoMore(it);
  }
    
  public void testA2() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return "a".equals(s);
      }
    };
    // this time without check: assertTrue(it.hasNext());
    assertEquals("a", it.next());
    assertNoMore(it);
  }
    
  public void testB1() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return "b".equals(s);
      }
    };
    assertTrue(it.hasNext());
    assertEquals("b", it.next());
    assertNoMore(it);
  }
    
  public void testB2() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return "b".equals(s);
      }
    };
    // this time without check: assertTrue(it.hasNext());
    assertEquals("b", it.next());
    assertNoMore(it);
  }
    
  public void testAll1() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return true;
      }
    };
    assertTrue(it.hasNext());
    assertEquals("a", it.next());
    assertTrue(it.hasNext());
    assertEquals("b", it.next());
    assertTrue(it.hasNext());
    assertEquals("c", it.next());
    assertNoMore(it);
  }
    
  public void testAll2() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return true;
      }
    };
    assertEquals("a", it.next());
    assertEquals("b", it.next());
    assertEquals("c", it.next());
    assertNoMore(it);
  }

  public void testUnmodifiable() {
    final Iterator<String> it = new FilterIterator<String, String>(set.iterator()) {
      @Override
      protected boolean predicateFunction(String s) {
        return true;
      }
    };
    assertEquals("a", it.next());
    expectThrows(UnsupportedOperationException.class, () -> {
      it.remove();
    });
  }

}
