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

package org.apache.solr.common.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.IteratorChain;

public class IteratorChainTest extends LuceneTestCase {
  
  private Iterator<String> makeIterator(String marker,int howMany) {
    final List<String> c = new ArrayList<String>();
    for(int i = 1; i <= howMany; i++) {
      c.add(marker + i);
    }
    return c.iterator();
  }
  
  public void testNoIterator() {
    final IteratorChain<String> c = new IteratorChain<String>();
    assertFalse("Empty IteratorChain.hastNext() is false",c.hasNext());
    assertEquals("",getString(c));
  }
  
  public void testCallNextTooEarly() {
    final IteratorChain<String> c = new IteratorChain<String>();
    c.addIterator(makeIterator("a",3));
    try {
      c.next();
      fail("Calling next() before hasNext() should throw RuntimeException");
    } catch(RuntimeException asExpected) {
      // we're fine
    }
  }
  
  public void testCallAddTooLate() {
    final IteratorChain<String> c = new IteratorChain<String>();
    c.hasNext();
    try {
      c.addIterator(makeIterator("a",3));
      fail("Calling addIterator after hasNext() should throw RuntimeException");
    } catch(RuntimeException asExpected) {
      // we're fine
    }
  }
  
  public void testRemove() {
    final IteratorChain<String> c = new IteratorChain<String>();
    try {
      c.remove();
      fail("Calling remove should throw UnsupportedOperationException");
    } catch(UnsupportedOperationException asExpected) {
      // we're fine
    }
  }
  
  public void testOneIterator() {
    final IteratorChain<String> c = new IteratorChain<String>();
    c.addIterator(makeIterator("a",3));
    assertEquals("a1a2a3",getString(c));
  }
  
  public void testTwoIterators() {
    final IteratorChain<String> c = new IteratorChain<String>();
    c.addIterator(makeIterator("a",3));
    c.addIterator(makeIterator("b",2));
    assertEquals("a1a2a3b1b2",getString(c));
  }
  
  public void testEmptyIteratorsInTheMiddle() {
    final IteratorChain<String> c = new IteratorChain<String>();
    c.addIterator(makeIterator("a",3));
    c.addIterator(makeIterator("b",0));
    c.addIterator(makeIterator("c",1));
    assertEquals("a1a2a3c1",getString(c));
  }
  
  /** dump the contents of it to a String */
  private String getString(Iterator<String> it) {
    final StringBuilder sb = new StringBuilder();
    sb.append("");
    while(it.hasNext()) {
      sb.append(it.next());
    }
    return sb.toString();
  }
}
