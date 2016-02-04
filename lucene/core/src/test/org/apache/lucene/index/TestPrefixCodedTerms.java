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


import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.util.BytesRef;
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

    MergedPrefixCodedTermsIterator merged = new MergedPrefixCodedTermsIterator(Arrays.asList(new PrefixCodedTerms[] {pb1, pb2}));
    BytesRef term = merged.next();
    assertNotNull(term);
    assertEquals("foo", merged.field());
    assertEquals("a", term.utf8ToString());
    term = merged.next();
    assertNotNull(term);
    assertEquals("b", term.utf8ToString());
    assertNull(merged.next());
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testMergeRandom() {
    PrefixCodedTerms pb[] = new PrefixCodedTerms[TestUtil.nextInt(random(), 2, 10)];
    Set<Term> superSet = new TreeSet<>();
    
    for (int i = 0; i < pb.length; i++) {
      Set<Term> terms = new TreeSet<>();
      int nterms = TestUtil.nextInt(random(), 0, 10000);
      for (int j = 0; j < nterms; j++) {
        String field = TestUtil.randomUnicodeString(random(), 2);
        //String field = TestUtil.randomSimpleString(random(), 2);
        Term term = new Term(field, TestUtil.randomUnicodeString(random(), 4));
        terms.add(term);
      }
      superSet.addAll(terms);
    
      PrefixCodedTerms.Builder b = new PrefixCodedTerms.Builder();
      //System.out.println("TEST: sub " + i + " has " + terms.size() + " terms");
      for (Term ref: terms) {
        //System.out.println("  add " + ref.field() + " " + ref.bytes());
        b.add(ref);
      }
      pb[i] = b.finish();
    }
    
    Iterator<Term> expected = superSet.iterator();

    MergedPrefixCodedTermsIterator actual = new MergedPrefixCodedTermsIterator(Arrays.asList(pb));
    String field = "";

    BytesRef lastTerm = null;
    BytesRef term;
    while ((term = actual.next()) != null) {
      if (field != actual.field()) {
        field = actual.field();
        lastTerm = null;
      }
      if (lastTerm != null && lastTerm.equals(term)) {
        continue;
      }
      lastTerm = BytesRef.deepCopyOf(term);
      assertTrue(expected.hasNext());

      Term expectedTerm = expected.next();
      assertEquals(expectedTerm, new Term(field, term));
    }

    assertFalse(expected.hasNext());
  }
}
