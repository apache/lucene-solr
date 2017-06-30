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
package org.apache.lucene.index;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestPrefixCodedTerms extends LuceneTestCase {
  
  public void testEmpty() {
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    PrefixCodedTerms pb = b.finish();
    TermIterator iter = pb.iterator();
    assertNull(iter.next());
  }
  
  public void testOne() {
    Term term = new Term("foo", "bogus");
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    b.add(term);
    PrefixCodedTerms pb = b.finish();
    TermIterator iter = pb.iterator();
    assertNotNull(iter.next());
    assertEquals("foo", iter.field());
    assertEquals("bogus", iter.bytes.utf8ToString());
    assertNull(iter.next());
  }
  
  public void testRandom() {
    Set<Term> terms = new TreeSet<>();
    int nterms = atLeast(10000);
    for (int i = 0; i < nterms; i++) {
      Term term = new Term(TestUtil.randomUnicodeString(random(), 2), TestUtil.randomUnicodeString(random()));
      terms.add(term);
    }    
    
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    for (Term ref: terms) {
      b.add(ref);
    }
    PrefixCodedTerms pb = b.finish();
    
    TermIterator iter = pb.iterator();
    Iterator<Term> expected = terms.iterator();
    assertEquals(terms.size(), pb.size());
    //System.out.println("TEST: now iter");
    while (iter.next() != null) {
      assertTrue(expected.hasNext());
      assertEquals(expected.next(), new Term(iter.field(), iter.bytes));
    }

    assertFalse(expected.hasNext());
  }
}
