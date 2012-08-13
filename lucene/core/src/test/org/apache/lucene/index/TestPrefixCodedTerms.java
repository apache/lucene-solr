package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestPrefixCodedTerms extends LuceneTestCase {
  
  public void testEmpty() {
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    PrefixCodedTerms pb = b.finish();
    assertFalse(pb.iterator().hasNext());
  }
  
  public void testOne() {
    Term term = new Term("foo", "bogus");
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    b.add(term);
    PrefixCodedTerms pb = b.finish();
    Iterator<Term> iterator = pb.iterator();
    assertTrue(iterator.hasNext());
    assertEquals(term, iterator.next());
  }
  
  public void testRandom() {
    Set<Term> terms = new TreeSet<Term>();
    int nterms = atLeast(10000);
    for (int i = 0; i < nterms; i++) {
      Term term = new Term(_TestUtil.randomUnicodeString(random(), 2), _TestUtil.randomUnicodeString(random()));
      terms.add(term);
    }    
    
    PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
    for (Term ref: terms) {
      b.add(ref);
    }
    PrefixCodedTerms pb = b.finish();
    
    Iterator<Term> expected = terms.iterator();
    for (Term t : pb) {
      assertTrue(expected.hasNext());
      assertEquals(expected.next(), t);
    }
    assertFalse(expected.hasNext());
  }
  
  @SuppressWarnings("unchecked")
  public void testMergeEmpty() {
    Iterator<Term> merged = new MergedIterator<Term>();
    assertFalse(merged.hasNext());

    merged = new MergedIterator<Term>(new PrefixCodedTerms.Builder().finish().iterator(), new PrefixCodedTerms.Builder().finish().iterator());
    assertFalse(merged.hasNext());
  }

  @SuppressWarnings("unchecked")
  public void testMergeOne() {
    Term t1 = new Term("foo", "a");
    PrefixCodedTerms.Builder b1 = new PrefixCodedTerms.Builder();
    b1.add(t1);
    PrefixCodedTerms pb1 = b1.finish();
    
    Term t2 = new Term("foo", "b");
    PrefixCodedTerms.Builder b2 = new PrefixCodedTerms.Builder();
    b2.add(t2);
    PrefixCodedTerms pb2 = b2.finish();
    
    Iterator<Term> merged = new MergedIterator<Term>(pb1.iterator(), pb2.iterator());
    assertTrue(merged.hasNext());
    assertEquals(t1, merged.next());
    assertTrue(merged.hasNext());
    assertEquals(t2, merged.next());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testMergeRandom() {
    PrefixCodedTerms pb[] = new PrefixCodedTerms[_TestUtil.nextInt(random(), 2, 10)];
    Set<Term> superSet = new TreeSet<Term>();
    
    for (int i = 0; i < pb.length; i++) {
      Set<Term> terms = new TreeSet<Term>();
      int nterms = _TestUtil.nextInt(random(), 0, 10000);
      for (int j = 0; j < nterms; j++) {
        Term term = new Term(_TestUtil.randomUnicodeString(random(), 2), _TestUtil.randomUnicodeString(random(), 4));
        terms.add(term);
      }
      superSet.addAll(terms);
    
      PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
      for (Term ref: terms) {
        b.add(ref);
      }
      pb[i] = b.finish();
    }
    
    List<Iterator<Term>> subs = new ArrayList<Iterator<Term>>();
    for (int i = 0; i < pb.length; i++) {
      subs.add(pb[i].iterator());
    }
    
    Iterator<Term> expected = superSet.iterator();
    Iterator<Term> actual = new MergedIterator<Term>(subs.toArray(new Iterator[0]));
    while (actual.hasNext()) {
      assertTrue(expected.hasNext());
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(expected.hasNext());
  }
}
