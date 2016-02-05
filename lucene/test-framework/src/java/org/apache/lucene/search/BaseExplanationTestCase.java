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
package org.apache.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.search.spans.SpanTestUtil.*;

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
 */
public abstract class BaseExplanationTestCase extends LuceneTestCase {
  protected static IndexSearcher searcher;
  protected static IndexReader reader;
  protected static Directory directory;
  protected static Analyzer analyzer;
  
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
    analyzer.close();
    analyzer = null;
  }
  
  @BeforeClass
  public static void beforeClassTestExplanations() throws Exception {
    directory = newDirectory();
    analyzer = new MockAnalyzer(random());
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newStringField(KEY, ""+i, Field.Store.NO));
      doc.add(new SortedDocValuesField(KEY, new BytesRef(""+i)));
      Field f = newTextField(FIELD, docFields[i], Field.Store.NO);
      f.setBoost(i);
      doc.add(f);
      doc.add(newTextField(ALTFIELD, docFields[i], Field.Store.NO));
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
   * Convenience subclass of TermsQuery
   */
  protected Query matchTheseItems(int[] terms) {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    for(int term : terms) {
      query.add(new BooleanClause(new TermQuery(new Term(KEY, ""+term)), BooleanClause.Occur.SHOULD));
    }
    return query.build();
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
  public SpanQuery st(String s) {
    return spanTermQuery(FIELD, s);
  }
  
  /** MACRO for SpanNotQuery */
  public SpanQuery snot(SpanQuery i, SpanQuery e) {
    return spanNotQuery(i, e);
  }

  /** MACRO for SpanOrQuery containing two SpanTerm queries */
  public SpanQuery sor(String s, String e) {
    return spanOrQuery(FIELD, s, e);
  }
  
  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery e) {
    return spanOrQuery(s, e);
  }
  
  /** MACRO for SpanOrQuery containing three SpanTerm queries */
  public SpanQuery sor(String s, String m, String e) {
    return spanOrQuery(FIELD, s, m, e);
  }
  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery m, SpanQuery e) {
    return spanOrQuery(s, m, e);
  }
  
  /** MACRO for SpanNearQuery containing two SpanTerm queries */
  public SpanQuery snear(String s, String e, int slop, boolean inOrder) {
    return snear(st(s), st(e), slop, inOrder);
  }
  
  /** MACRO for SpanNearQuery containing two SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, e);
    } else {
      return spanNearUnorderedQuery(slop, s, e);
    }
  }
  
  
  /** MACRO for SpanNearQuery containing three SpanTerm queries */
  public SpanQuery snear(String s, String m, String e,
                             int slop, boolean inOrder) {
    return snear(st(s), st(m), st(e), slop, inOrder);
  }
  /** MACRO for SpanNearQuery containing three SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery m, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, m, e);
    } else {
      return spanNearUnorderedQuery(slop, s, m, e);
    }
  }
  
  /** MACRO for SpanFirst(SpanTermQuery) */
  public SpanQuery sf(String s, int b) {
    return spanFirstQuery(st(s), b);
  }

  /**
   * MACRO: Wraps a Query in a BooleanQuery so that it is optional, along
   * with a second prohibited clause which will never match anything
   */
  public Query optB(Query q) throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(q, BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("NEVER","MATCH")), BooleanClause.Occur.MUST_NOT);
    return bq.build();
  }

  /**
   * MACRO: Wraps a Query in a BooleanQuery so that it is required, along
   * with a second optional clause which will match everything
   */
  public Query reqB(Query q) throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(q, BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(FIELD,"w1")), BooleanClause.Occur.SHOULD);
    return bq.build();
  }
}
