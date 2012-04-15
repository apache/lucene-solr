package org.apache.lucene.search;

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

import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Simple base class for checking search equivalence.
 * Extend it, and write tests that create {@link #randomTerm()}s
 * (all terms are single characters a-z), and use 
 * {@link #assertSameSet(Query, Query)} and 
 * {@link #assertSubsetOf(Query, Query)}
 */
public abstract class SearchEquivalenceTestBase extends LuceneTestCase {
  protected static IndexSearcher s1, s2;
  protected static Directory directory;
  protected static IndexReader reader;
  protected static Analyzer analyzer;
  protected static String stopword; // we always pick a character as a stopword
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Random random = random();
    directory = newDirectory();
    stopword = "" + randomChar();
    CharacterRunAutomaton stopset = new CharacterRunAutomaton(BasicAutomata.makeString(stopword));
    analyzer = new MockAnalyzer(random, MockTokenizer.WHITESPACE, false, stopset, true);
    RandomIndexWriter iw = new RandomIndexWriter(random, directory, analyzer);
    Document doc = new Document();
    Field id = new StringField("id", "");
    Field field = new TextField("field", "");
    doc.add(id);
    doc.add(field);
    
    // index some docs
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      id.setStringValue(Integer.toString(i));
      field.setStringValue(randomFieldContents());
      iw.addDocument(doc);
    }
    
    // delete some docs
    int numDeletes = numDocs/20;
    for (int i = 0; i < numDeletes; i++) {
      Term toDelete = new Term("id", Integer.toString(random.nextInt(numDocs)));
      if (random.nextBoolean()) {
        iw.deleteDocuments(toDelete);
      } else {
        iw.deleteDocuments(new TermQuery(toDelete));
      }
    }
    
    reader = iw.getReader();
    s1 = newSearcher(reader);
    s2 = newSearcher(reader);
    iw.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    analyzer.close();
    reader = null;
    directory = null;
    analyzer = null;
    s1 = s2 = null;
  }
  
  /**
   * populate a field with random contents.
   * terms should be single characters in lowercase (a-z)
   * tokenization can be assumed to be on whitespace.
   */
  static String randomFieldContents() {
    // TODO: zipf-like distribution
    StringBuilder sb = new StringBuilder();
    int numTerms = random().nextInt(15);
    for (int i = 0; i < numTerms; i++) {
      if (sb.length() > 0) {
        sb.append(' '); // whitespace
      }
      sb.append(randomChar());
    }
    return sb.toString();
  }

  /**
   * returns random character (a-z)
   */
  static char randomChar() {
    return (char) _TestUtil.nextInt(random(), 'a', 'z');
  }

  /**
   * returns a term suitable for searching.
   * terms are single characters in lowercase (a-z)
   */
  protected Term randomTerm() {
    return new Term("field", "" + randomChar());
  }
  
  /**
   * Returns a random filter over the document set
   */
  protected Filter randomFilter() {
    return new QueryWrapperFilter(TermRangeQuery.newStringRange("field", "a", "" + randomChar(), true, true));
  }

  /**
   * Asserts that the documents returned by <code>q1</code>
   * are the same as of those returned by <code>q2</code>
   */
  public void assertSameSet(Query q1, Query q2) throws Exception {
    assertSubsetOf(q1, q2);
    assertSubsetOf(q2, q1);
  }
  
  /**
   * Asserts that the documents returned by <code>q1</code>
   * are a subset of those returned by <code>q2</code>
   */
  public void assertSubsetOf(Query q1, Query q2) throws Exception {   
    // test without a filter
    assertSubsetOf(q1, q2, null);
    
    // test with a filter (this will sometimes cause advance'ing enough to test it)
    assertSubsetOf(q1, q2, randomFilter());
  }
  
  /**
   * Asserts that the documents returned by <code>q1</code>
   * are a subset of those returned by <code>q2</code>.
   * 
   * Both queries will be filtered by <code>filter</code>
   */
  protected void assertSubsetOf(Query q1, Query q2, Filter filter) throws Exception {
    // TRUNK ONLY: test both filter code paths
    if (filter != null && random().nextBoolean()) {
      final boolean q1RandomAccess = random().nextBoolean();
      final boolean q2RandomAccess = random().nextBoolean();
      q1 = new FilteredQuery(q1, filter) {
        @Override
        protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
          return q1RandomAccess;
        }
      };
      q2 = new FilteredQuery(q2, filter) {
        @Override
        protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
          return q2RandomAccess;
        }
      };
      filter = null;
    }
    
    // not efficient, but simple!
    TopDocs td1 = s1.search(q1, filter, reader.maxDoc());
    TopDocs td2 = s2.search(q2, filter, reader.maxDoc());
    assertTrue(td1.totalHits <= td2.totalHits);
    
    // fill the superset into a bitset
    BitSet bitset = new BitSet();
    for (int i = 0; i < td2.scoreDocs.length; i++) {
      bitset.set(td2.scoreDocs[i].doc);
    }
    
    // check in the subset, that every bit was set by the super
    for (int i = 0; i < td1.scoreDocs.length; i++) {
      assertTrue(bitset.get(td1.scoreDocs[i].doc));
    }
  }
}
