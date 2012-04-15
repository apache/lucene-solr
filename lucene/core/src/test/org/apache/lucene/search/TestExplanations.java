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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests primitive queries (ie: that rewrite to themselves) to
 * insure they match the expected set of docs, and that the score of each
 * match is equal to the value of the scores explanation.
 *
 * <p>
 * The assumption is that if all of the "primitive" queries work well,
 * then anything that rewrites to a primitive will work well also.
 * </p>
 *
 * @see "Subclasses for actual tests"
 */
public class TestExplanations extends LuceneTestCase {
  protected static IndexSearcher searcher;
  protected static IndexReader reader;
  protected static Directory directory;
  
  public static final String KEY = "KEY";
  // boost on this field is the same as the iterator for the doc
  public static final String FIELD = "field";
  // same contents, but no field boost
  public static final String ALTFIELD = "alt";
  
  @AfterClass
  public static void afterClassTestExplanations() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  @BeforeClass
  public static void beforeClassTestExplanations() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(KEY, ""+i, StringField.TYPE_UNSTORED));
      Field f = newField(FIELD, docFields[i], TextField.TYPE_UNSTORED);
      f.setBoost(i);
      doc.add(f);
      doc.add(newField(ALTFIELD, docFields[i], TextField.TYPE_UNSTORED));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  protected static final String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3 zz",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3 zz"
  };
  
  /** check the expDocNrs first, then check the query (and the explanations) */
  public void qtest(Query q, int[] expDocNrs) throws Exception {
    CheckHits.checkHitCollector(random(), q, FIELD, searcher, expDocNrs);
  }

  /**
   * Tests a query using qtest after wrapping it with both optB and reqB
   * @see #qtest
   * @see #reqB
   * @see #optB
   */
  public void bqtest(Query q, int[] expDocNrs) throws Exception {
    qtest(reqB(q), expDocNrs);
    qtest(optB(q), expDocNrs);
  }
  
  /** 
   * Convenience subclass of FieldCacheTermsFilter
   */
  public static class ItemizedFilter extends FieldCacheTermsFilter {
    private static String[] int2str(int [] terms) {
      String [] out = new String[terms.length];
      for (int i = 0; i < terms.length; i++) {
        out[i] = ""+terms[i];
      }
      return out;
    }
    public ItemizedFilter(String keyField, int [] keys) {
      super(keyField, int2str(keys));
    }
    public ItemizedFilter(int [] keys) {
      super(KEY, int2str(keys));
    }
  }

  /** helper for generating MultiPhraseQueries */
  public static Term[] ta(String[] s) {
    Term[] t = new Term[s.length];
    for (int i = 0; i < s.length; i++) {
      t[i] = new Term(FIELD, s[i]);
    }
    return t;
  }

  /** MACRO for SpanTermQuery */
  public SpanTermQuery st(String s) {
    return new SpanTermQuery(new Term(FIELD,s));
  }
  
  /** MACRO for SpanNotQuery */
  public SpanNotQuery snot(SpanQuery i, SpanQuery e) {
    return new SpanNotQuery(i,e);
  }

  /** MACRO for SpanOrQuery containing two SpanTerm queries */
  public SpanOrQuery sor(String s, String e) {
    return sor(st(s), st(e));
  }
  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanOrQuery sor(SpanQuery s, SpanQuery e) {
    return new SpanOrQuery(s, e);
  }
  
  /** MACRO for SpanOrQuery containing three SpanTerm queries */
  public SpanOrQuery sor(String s, String m, String e) {
    return sor(st(s), st(m), st(e));
  }
  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanOrQuery sor(SpanQuery s, SpanQuery m, SpanQuery e) {
    return new SpanOrQuery(s, m, e);
  }
  
  /** MACRO for SpanNearQuery containing two SpanTerm queries */
  public SpanNearQuery snear(String s, String e, int slop, boolean inOrder) {
    return snear(st(s), st(e), slop, inOrder);
  }
  /** MACRO for SpanNearQuery containing two SpanQueries */
  public SpanNearQuery snear(SpanQuery s, SpanQuery e,
                             int slop, boolean inOrder) {
    return new SpanNearQuery(new SpanQuery[] { s, e }, slop, inOrder);
  }
  
  
  /** MACRO for SpanNearQuery containing three SpanTerm queries */
  public SpanNearQuery snear(String s, String m, String e,
                             int slop, boolean inOrder) {
    return snear(st(s), st(m), st(e), slop, inOrder);
  }
  /** MACRO for SpanNearQuery containing three SpanQueries */
  public SpanNearQuery snear(SpanQuery s, SpanQuery m, SpanQuery e,
                             int slop, boolean inOrder) {
    return new SpanNearQuery(new SpanQuery[] { s, m, e }, slop, inOrder);
  }
  
  /** MACRO for SpanFirst(SpanTermQuery) */
  public SpanFirstQuery sf(String s, int b) {
    return new SpanFirstQuery(st(s), b);
  }

  /**
   * MACRO: Wraps a Query in a BooleanQuery so that it is optional, along
   * with a second prohibited clause which will never match anything
   */
  public Query optB(Query q) throws Exception {
    BooleanQuery bq = new BooleanQuery(true);
    bq.add(q, BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("NEVER","MATCH")), BooleanClause.Occur.MUST_NOT);
    return bq;
  }

  /**
   * MACRO: Wraps a Query in a BooleanQuery so that it is required, along
   * with a second optional clause which will match everything
   */
  public Query reqB(Query q) throws Exception {
    BooleanQuery bq = new BooleanQuery(true);
    bq.add(q, BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(FIELD,"w1")), BooleanClause.Occur.SHOULD);
    return bq;
  }
  
  /**
   * Placeholder: JUnit freaks if you don't have one test ... making
   * class abstract doesn't help
   */
  public void testNoop() {
    /* NOOP */
  }
}
