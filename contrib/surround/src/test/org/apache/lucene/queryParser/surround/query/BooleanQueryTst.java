package org.apache.lucene.queryParser.surround.query;

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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.HitCollector;

import org.apache.lucene.queryParser.surround.parser.QueryParser;

import junit.framework.TestCase;

public class BooleanQueryTst {
  String queryText;
  final int[] expectedDocNrs;
  SingleFieldTestDb dBase;
  String fieldName;
  TestCase testCase;
  BasicQueryFactory qf;
  boolean verbose = true;

  public BooleanQueryTst(
      String queryText,
      int[] expectedDocNrs,
      SingleFieldTestDb dBase,
      String fieldName,
      TestCase testCase,
      BasicQueryFactory qf) {
    this.queryText = queryText;
    this.expectedDocNrs = expectedDocNrs;
    this.dBase = dBase;
    this.fieldName = fieldName;
    this.testCase = testCase;
    this.qf = qf;
  }
  
  public void setVerbose(boolean verbose) {this.verbose = verbose;}

  class TestCollector extends HitCollector { // FIXME: use check hits from Lucene tests
    int totalMatched;
    boolean[] encountered;

    TestCollector() {
      totalMatched = 0;
      encountered = new boolean[expectedDocNrs.length];
    }

    public void collect(int docNr, float score) {
      /* System.out.println(docNr + " '" + dBase.getDocs()[docNr] + "': " + score); */
      TestCase.assertTrue(queryText + ": positive score", score > 0.0);
      TestCase.assertTrue(queryText + ": too many hits", totalMatched < expectedDocNrs.length);
      int i;
      for (i = 0; i < expectedDocNrs.length; i++) {
        if ((! encountered[i]) && (expectedDocNrs[i] == docNr)) {
          encountered[i] = true;
          break;
        }
      }
      if (i == expectedDocNrs.length) {
        TestCase.assertTrue(queryText + ": doc nr for hit not expected: " + docNr, false);
      }
      totalMatched++;
    }

    void checkNrHits() {
      TestCase.assertEquals(queryText + ": nr of hits", expectedDocNrs.length, totalMatched);
    }
  }

  public void doTest() throws Exception {

    if (verbose) {    
        System.out.println("");
        System.out.println("Query: " + queryText);
    }
    
    SrndQuery lq = QueryParser.parse(queryText);
    
    /* if (verbose) System.out.println("Srnd: " + lq.toString()); */
    
    Query query = lq.makeLuceneQueryField(fieldName, qf);
    /* if (verbose) System.out.println("Lucene: " + query.toString()); */

    TestCollector tc = new TestCollector();
    Searcher searcher = new IndexSearcher(dBase.getDb());
    try {
      searcher.search(query, tc);
    } finally {
      searcher.close();
    }
    tc.checkNrHits();
  }
}


