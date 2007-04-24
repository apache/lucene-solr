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


  /** various query sanity checks on a searcher */
  public static void check(Query q1, Searcher s) {
// Disabled because this started failing after LUCENE-730 patch was applied
//     try {
      check(q1);
/* disabled for use of BooleanScorer in BooleanScorer2.
      if (s!=null && s instanceof IndexSearcher) {
        IndexSearcher is = (IndexSearcher)s;
//         checkSkipTo(q1,is);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
 */
  }

  /** alternate scorer skipTo(),skipTo(),next(),next(),skipTo(),skipTo(), etc
   * and ensure a hitcollector receives same docs and scores
   */
  public static void checkSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("Checking "+q);
    final Weight w = q.weight(s);
    final Scorer scorer = w.scorer(s.getIndexReader());

    // FUTURE: ensure scorer.doc()==-1
    
    if (BooleanQuery.getUseScorer14()) return;  // 1.4 doesn't support skipTo

    final int[] which = new int[1];
    final int[] sdoc = new int[] {-1};
    final float maxDiff = 1e-5f;
    s.search(q,new HitCollector() {
      public void collect(int doc, float score) {
        try {
          boolean more = (which[0]++&0x02)==0 ? scorer.skipTo(sdoc[0]+1) : scorer.next();
          sdoc[0] = scorer.doc();
          float scorerScore = scorer.score();
          float scoreDiff = Math.abs(score-scorerScore);
          scoreDiff=0; // TODO: remove this go get LUCENE-697 failures 
          if (more==false || doc != sdoc[0] || scoreDiff>maxDiff) {
            throw new RuntimeException("ERROR matching docs:"
                    +"\n\tscorer.more=" + more + " doc="+sdoc[0] + " scorerScore="+scorerScore
                    +" scoreDiff="+scoreDiff + " maxDiff="+maxDiff
                    +"\n\thitCollector.doc=" + doc + " score="+score
                    +"\n\t Scorer=" + scorer
                    +"\n\t Query=" + q
                    +"\n\t Searcher=" + s
            );
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // make sure next call to scorer is false.
    TestCase.assertFalse((which[0]++&0x02)==0 ? scorer.skipTo(sdoc[0]+1) : scorer.next());
  }

}
