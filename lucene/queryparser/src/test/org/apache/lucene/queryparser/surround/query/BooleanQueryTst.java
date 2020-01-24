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
package org.apache.lucene.queryparser.surround.query;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.surround.parser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.junit.Assert;

public class BooleanQueryTst {
  String queryText;
  final int[] expectedDocNrs;
  SingleFieldTestDb dBase;
  String fieldName;
  Assert testCase;
  BasicQueryFactory qf;
  boolean verbose = true;

  public BooleanQueryTst(
      String queryText,
      int[] expectedDocNrs,
      SingleFieldTestDb dBase,
      String fieldName,
      Assert testCase,
      BasicQueryFactory qf) {
    this.queryText = queryText;
    this.expectedDocNrs = expectedDocNrs;
    this.dBase = dBase;
    this.fieldName = fieldName;
    this.testCase = testCase;
    this.qf = qf;
  }
  
  public void setVerbose(boolean verbose) {this.verbose = verbose;}

  class TestCollector extends SimpleCollector { // FIXME: use check hits from Lucene tests
    int totalMatched;
    boolean[] encountered;
    private Scorable scorer = null;
    private int docBase = 0;

    TestCollector() {
      totalMatched = 0;
      encountered = new boolean[expectedDocNrs.length];
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
    }
    
    @Override
    public void collect(int docNr) throws IOException {
      float score = scorer.score();
      docNr += docBase;
      /* System.out.println(docNr + " '" + dBase.getDocs()[docNr] + "': " + score); */
      Assert.assertTrue(queryText + ": positive score", score > 0.0);
      Assert.assertTrue(queryText + ": too many hits", totalMatched < expectedDocNrs.length);
      int i;
      for (i = 0; i < expectedDocNrs.length; i++) {
        if ((! encountered[i]) && (expectedDocNrs[i] == docNr)) {
          encountered[i] = true;
          break;
        }
      }
      if (i == expectedDocNrs.length) {
        Assert.assertTrue(queryText + ": doc nr for hit not expected: " + docNr, false);
      }
      totalMatched++;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    void checkNrHits() {
      Assert.assertEquals(queryText + ": nr of hits", expectedDocNrs.length, totalMatched);
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
    IndexReader reader = DirectoryReader.open(dBase.getDb());
    IndexSearcher searcher = new IndexSearcher(reader);
    try {
      searcher.search(query, tc);
    } finally {
      reader.close();
    }
    tc.checkNrHits();
  }
}


