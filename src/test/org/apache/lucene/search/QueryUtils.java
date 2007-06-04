package org.apache.lucene.search;

import junit.framework.TestCase;

import java.io.IOException;

/**
 * Copyright 2005 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * @author yonik
 */
public class QueryUtils {

  /** Check the types of things query objects should be able to do. */
  public static void check(Query q) {
    checkHashEquals(q);
  }

  /** check very basic hashCode and equals */
  public static void checkHashEquals(Query q) {
    Query q2 = (Query)q.clone();
    checkEqual(q,q2);

    Query q3 = (Query)q.clone();
    q3.setBoost(7.21792348f);
    checkUnequal(q,q3);

    // test that a class check is done so that no exception is thrown
    // in the implementation of equals()
    Query whacky = new Query() {
      public String toString(String field) {
        return "My Whacky Query";
      }
    };
    whacky.setBoost(q.getBoost());
    checkUnequal(q, whacky);
  }

  public static void checkEqual(Query q1, Query q2) {
    TestCase.assertEquals(q1, q2);
    TestCase.assertEquals(q1.hashCode(), q2.hashCode());
  }

  public static void checkUnequal(Query q1, Query q2) {
    TestCase.assertTrue(!q1.equals(q2));
    TestCase.assertTrue(!q2.equals(q1));

    // possible this test can fail on a hash collision... if that
    // happens, please change test to use a different example.
    TestCase.assertTrue(q1.hashCode() != q2.hashCode());
  }
  
  /** deep check that explanations of a query 'score' correctly */
  public static void checkExplanations (final Query q, final Searcher s) throws IOException {
    CheckHits.checkExplanations(q, null, s, true);
  }
  
  /** 
   * various query sanity checks on a searcher, including explanation checks.
   * @see #checkExplanations
   * @see #checkSkipTo
   * @see #check(Query)
   */
  public static void check(Query q1, Searcher s) {
    try {
      check(q1);
      if (s!=null) {
        if (s instanceof IndexSearcher) {
          IndexSearcher is = (IndexSearcher)s;
          checkSkipTo(q1,is);
        }
        checkExplanations(q1,s);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** alternate scorer skipTo(),skipTo(),next(),next(),skipTo(),skipTo(), etc
   * and ensure a hitcollector receives same docs and scores
   */
  public static void checkSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("Checking "+q);
   
    if (BooleanQuery.getAllowDocsOutOfOrder()) return;  // 1.4 doesn't support skipTo

    final int skip_op = 0;
    final int next_op = 1;
    final int orders [][] = {
        {skip_op},
        {next_op},
        {skip_op, next_op},
        {next_op, skip_op},
        {skip_op, skip_op, next_op, next_op},
        {next_op, next_op, skip_op, skip_op},
        {skip_op, skip_op, skip_op, next_op, next_op},
    };
    for (int k = 0; k < orders.length; k++) {
      final int order[] = orders[k];
      //System.out.print("Order:");for (int i = 0; i < order.length; i++) System.out.print(order[i]==skip_op ? " skip()":" next()"); System.out.println();
      final int opidx[] = {0};

      final Weight w = q.weight(s);
      final Scorer scorer = w.scorer(s.getIndexReader());
      
      // FUTURE: ensure scorer.doc()==-1

      final int[] sdoc = new int[] {-1};
      final float maxDiff = 1e-5f;
      s.search(q,new HitCollector() {
        public void collect(int doc, float score) {
          try {
            int op = order[(opidx[0]++)%order.length];
            //System.out.println(op==skip_op ? "skip("+(sdoc[0]+1)+")":"next()");
            boolean more = op==skip_op ? scorer.skipTo(sdoc[0]+1) : scorer.next();
            sdoc[0] = scorer.doc();
            float scorerScore = scorer.score();
            float scoreDiff = Math.abs(score-scorerScore);
            if (more==false || doc != sdoc[0] || scoreDiff>maxDiff) {
              StringBuffer sbord = new StringBuffer();
              for (int i = 0; i < order.length; i++) 
                sbord.append(order[i]==skip_op ? " skip()":" next()");
              throw new RuntimeException("ERROR matching docs:"
                  +"\n\tscorer.more=" + more + " doc="+sdoc[0] + " scorerScore="+scorerScore
                  +" scoreDiff="+scoreDiff + " maxDiff="+maxDiff
                  +"\n\thitCollector.doc=" + doc + " score="+score
                  +"\n\t Scorer=" + scorer
                  +"\n\t Query=" + q
                  +"\n\t Searcher=" + s
                  +"\n\t Order=" + sbord
              );
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      
      // make sure next call to scorer is false.
      int op = order[(opidx[0]++)%order.length];
      //System.out.println(op==skip_op ? "last: skip()":"last: next()");
      boolean more = op==skip_op ? scorer.skipTo(sdoc[0]+1) : scorer.next();
      TestCase.assertFalse(more);
    }
  }

}
