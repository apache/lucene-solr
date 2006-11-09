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

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexReader;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class CheckHits {
  
  /**
   * Tests that all documents up to maxDoc which are *not* in the
   * expected result set, have an explanation which indicates no match
   * (ie: Explanation value of 0.0f)
   */
  public static void checkNoMatchExplanations(Query q, String defaultFieldName,
                                              Searcher searcher, int[] results)
    throws IOException {

    String d = q.toString(defaultFieldName);
    Set ignore = new TreeSet();
    for (int i = 0; i < results.length; i++) {
      ignore.add(new Integer(results[i]));
    }
    
    int maxDoc = searcher.maxDoc();
    for (int doc = 0; doc < maxDoc; doc++) {
      if (ignore.contains(new Integer(doc))) continue;

      Explanation exp = searcher.explain(q, doc);
      TestCase.assertNotNull("Explanation of [["+d+"]] for #"+doc+" is null",
                             exp);
      TestCase.assertEquals("Explanation of [["+d+"]] for #"+doc+
                            " doesn't indicate non-match: " + exp.toString(),
                            0.0f, exp.getValue(), 0.0f);
    }
    
  }
  
  /**
   * Tests that a query matches the an expected set of documents using a
   * HitCollector.
   *
   * <p>
   * Note that when using the HitCollector API, documents will be collected
   * if they "match" regardless of what their score is.
   * </p>
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   * @param results a list of documentIds that must match the query
   * @see Searcher#search(Query,HitCollector)
   * @see #checkHits
   */
  public static void checkHitCollector(Query query, String defaultFieldName,
                                       Searcher searcher, int[] results)
    throws IOException {
    
    Set correct = new TreeSet();
    for (int i = 0; i < results.length; i++) {
      correct.add(new Integer(results[i]));
    }
    
    final Set actual = new TreeSet();
    searcher.search(query, new HitCollector() {
        public void collect(int doc, float score) {
          actual.add(new Integer(doc));
        }
      });
    TestCase.assertEquals(query.toString(defaultFieldName), correct, actual);

    QueryUtils.check(query,searcher);
  }
  
  /**
   * Tests that a query matches the an expected set of documents using Hits.
   *
   * <p>
   * Note that when using the Hits API, documents will only be returned
   * if they have a positive normalized score.
   * </p>
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   * @param results a list of documentIds that must match the query
   * @see Searcher#search(Query)
   * @see #checkHitCollector
   */
  public static void checkHits(
        Query query,
        String defaultFieldName,
        Searcher searcher,
        int[] results)
          throws IOException {
    if (searcher instanceof IndexSearcher) {
      QueryUtils.check(query,(IndexSearcher)searcher);
    }

    Hits hits = searcher.search(query);

    Set correct = new TreeSet();
    for (int i = 0; i < results.length; i++) {
      correct.add(new Integer(results[i]));
    }

    Set actual = new TreeSet();
    for (int i = 0; i < hits.length(); i++) {
      actual.add(new Integer(hits.id(i)));
    }

    TestCase.assertEquals(query.toString(defaultFieldName), correct, actual);

    QueryUtils.check(query,searcher);
  }

  /** Tests that a Hits has an expected order of documents */
  public static void checkDocIds(String mes, int[] results, Hits hits)
  throws IOException {
    TestCase.assertEquals(mes + " nr of hits", results.length, hits.length());
    for (int i = 0; i < results.length; i++) {
      TestCase.assertEquals(mes + " doc nrs for hit " + i, results[i], hits.id(i));
    }
  }

  /** Tests that two queries have an expected order of documents,
   * and that the two queries have the same score values.
   */
  public static void checkHitsQuery(
        Query query,
        Hits hits1,
        Hits hits2,
        int[] results)
          throws IOException {

    checkDocIds("hits1", results, hits1);
    checkDocIds("hits2", results, hits2);
    checkEqual(query, hits1, hits2);
  }

  public static void checkEqual(Query query, Hits hits1, Hits hits2) throws IOException {
     final float scoreTolerance = 1.0e-6f;
     if (hits1.length() != hits2.length()) {
       TestCase.fail("Unequal lengths: hits1="+hits1.length()+",hits2="+hits2.length());
     }
    for (int i = 0; i < hits1.length(); i++) {
      if (hits1.id(i) != hits2.id(i)) {
        TestCase.fail("Hit " + i + " docnumbers don't match\n"
                + hits2str(hits1, hits2,0,0)
                + "for query:" + query.toString());
      }

      if ((hits1.id(i) != hits2.id(i))
          || Math.abs(hits1.score(i) -  hits2.score(i)) > scoreTolerance)
      {
        TestCase.fail("Hit " + i + ", doc nrs " + hits1.id(i) + " and " + hits2.id(i)
                      + "\nunequal       : " + hits1.score(i)
                      + "\n           and: " + hits2.score(i)
                      + "\nfor query:" + query.toString());
      }
    }
  }

  public static String hits2str(Hits hits1, Hits hits2, int start, int end) throws IOException {
    StringBuffer sb = new StringBuffer();
    int len1=hits1==null ? 0 : hits1.length();
    int len2=hits2==null ? 0 : hits2.length();
    if (end<=0) {
      end = Math.max(len1,len2);
    }

    sb.append("Hits length1=" + len1 + "\tlength2="+len2);

    sb.append("\n");
    for (int i=start; i<end; i++) {
      sb.append("hit=" + i + ":");
      if (i<len1) {
        sb.append(" doc"+hits1.id(i) + "=" + hits1.score(i));
      } else {
        sb.append("               ");
      }
      sb.append(",\t");
      if (i<len2) {
        sb.append(" doc"+hits2.id(i) + "=" + hits2.score(i));
      }
      sb.append("\n");
    }
    return sb.toString();
  }


  public static String topdocsString(TopDocs docs, int start, int end) {
    StringBuffer sb = new StringBuffer();
    sb.append("TopDocs totalHits="+docs.totalHits + " top="+docs.scoreDocs.length+"\n");
    if (end<=0) end=docs.scoreDocs.length;
    else end=Math.min(end,docs.scoreDocs.length);
    for (int i=start; i<end; i++) {
      sb.append("\t");
      sb.append(i);
      sb.append(") doc=");
      sb.append(docs.scoreDocs[i].doc);
      sb.append("\tscore=");
      sb.append(docs.scoreDocs[i].score);
      sb.append("\n");
    }
    return sb.toString();
  }

  /**
   * Asserts that the score explanation for every document matching a
   * query corrisponds with the true score.
   *
   * @see ExplanationAsserter
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   */
  public static void checkExplanations(Query query,
                                       String defaultFieldName,
                                       Searcher searcher) throws IOException {

    searcher.search(query,
                    new ExplanationAsserter
                    (query, defaultFieldName, searcher));

  }

  /**
   * an IndexSearcher that implicitly checks hte explanation of every match
   * whenever it executes a search
   */
  public static class ExplanationAssertingSearcher extends IndexSearcher {
    public ExplanationAssertingSearcher(Directory d) throws IOException {
      super(d);
    }
    public ExplanationAssertingSearcher(IndexReader r) throws IOException {
      super(r);
    }
    protected void checkExplanations(Query q) throws IOException {
      super.search(q, null,
                   new ExplanationAsserter
                   (q, null, this));
    }
    public Hits search(Query query, Filter filter) throws IOException {
      checkExplanations(query);
      return super.search(query,filter);
    }
    public Hits search(Query query, Sort sort) throws IOException {
      checkExplanations(query);
      return super.search(query,sort);
    }
    public Hits search(Query query, Filter filter,
                       Sort sort) throws IOException {
      checkExplanations(query);
      return super.search(query,filter,sort);
    }
    public TopFieldDocs search(Query query,
                               Filter filter,
                               int n,
                               Sort sort) throws IOException {
      
      checkExplanations(query);
      return super.search(query,filter,n,sort);
    }
    public void search(Query query, HitCollector results) throws IOException {
      checkExplanations(query);
      super.search(query,results);
    }
    public void search(Query query, Filter filter,
                       HitCollector results) throws IOException {
      checkExplanations(query);
      super.search(query,filter, results);
    }
    public TopDocs search(Query query, Filter filter,
                          int n) throws IOException {

      checkExplanations(query);
      return super.search(query,filter, n);
    }
  }
    
  /**
   * Asserts that the score explanation for every document matching a
   * query corrisponds with the true score.
   *
   * NOTE: this HitCollector should only be used with the Query and Searcher
   * specified at when it is constructed.
   */
  public static class ExplanationAsserter extends HitCollector {

    /**
     * Some explains methods calculate their vlaues though a slightly
     * differnet  order of operations from the acctaul scoring method ...
     * this allows for a small amount of variation
     */
    public static float SCORE_TOLERANCE_DELTA = 0.00005f;
    
    Query q;
    Searcher s;
    String d;
    public ExplanationAsserter(Query q, String defaultFieldName, Searcher s) {
      this.q=q;
      this.s=s;
      this.d = q.toString(defaultFieldName);
    }      
    public void collect(int doc, float score) {
      Explanation exp = null;
      
      try {
        exp = s.explain(q, doc);
      } catch (IOException e) {
        throw new RuntimeException
          ("exception in hitcollector of [["+d+"]] for #"+doc, e);
      }
      
      TestCase.assertNotNull("Explanation of [["+d+"]] for #"+doc+" is null",
                             exp);
      TestCase.assertEquals("Score of [["+d+"]] for #"+doc+
                            " does not match explanation: " + exp.toString(),
                            score, exp.getValue(), SCORE_TOLERANCE_DELTA);
    }
    
  }

}

