package org.apache.lucene.search;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
          checkFirstSkipTo(q1,is);
          checkSkipTo(q1,is);
        }
        checkExplanations(q1,s);
        checkSerialization(q1,s);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** check that the query weight is serializable. 
   * @throws IOException if serialization check fail. 
   */
  private static void checkSerialization(Query q, Searcher s) throws IOException {
    Weight w = q.weight(s);
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(w);
      oos.close();
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
      ois.readObject();
      ois.close();
      
      //skip rquals() test for now - most weights don't overide equals() and we won't add this just for the tests.
      //TestCase.assertEquals("writeObject(w) != w.  ("+w+")",w2,w);   
      
    } catch (Exception e) {
      IOException e2 = new IOException("Serialization failed for "+w);
      e2.initCause(e);
      throw e2;
    }
  }


  /** alternate scorer skipTo(),skipTo(),next(),next(),skipTo(),skipTo(), etc
   * and ensure a hitcollector receives same docs and scores
   */
  public static void checkSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("Checking "+q);
    
    if (BooleanQuery.getAllowDocsOutOfOrder()) return;  // in this case order of skipTo() might differ from that of next().

    final int skip_op = 0;
    final int next_op = 1;
    final int orders [][] = {
        {next_op},
        {skip_op},
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
            float scorerScore2 = scorer.score();
            float scoreDiff = Math.abs(score-scorerScore);
            float scorerDiff = Math.abs(scorerScore2-scorerScore);
            if (!more || doc != sdoc[0] || scoreDiff>maxDiff || scorerDiff>maxDiff) {
              StringBuffer sbord = new StringBuffer();
              for (int i = 0; i < order.length; i++) 
                sbord.append(order[i]==skip_op ? " skip()":" next()");
              throw new RuntimeException("ERROR matching docs:"
                  +"\n\t"+(doc!=sdoc[0]?"--> ":"")+"doc="+sdoc[0]
                  +"\n\t"+(!more?"--> ":"")+"tscorer.more=" + more 
                  +"\n\t"+(scoreDiff>maxDiff?"--> ":"")+"scorerScore="+scorerScore+" scoreDiff="+scoreDiff + " maxDiff="+maxDiff
                  +"\n\t"+(scorerDiff>maxDiff?"--> ":"")+"scorerScore2="+scorerScore2+" scorerDiff="+scorerDiff
                  +"\n\thitCollector.doc=" + doc + " score="+score
                  +"\n\t Scorer=" + scorer
                  +"\n\t Query=" + q + "  "+q.getClass().getName()
                  +"\n\t Searcher=" + s
                  +"\n\t Order=" + sbord
                  +"\n\t Op=" + (op==skip_op ? " skip()":" next()")
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
    
  // check that first skip on just created scorers always goes to the right doc
  private static void checkFirstSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("checkFirstSkipTo: "+q);
    final float maxDiff = 1e-5f;
    final int lastDoc[] = {-1};
    s.search(q,new HitCollector() {
      public void collect(int doc, float score) {
        //System.out.println("doc="+doc);
        try {
          for (int i=lastDoc[0]+1; i<=doc; i++) {
            Weight w = q.weight(s);
            Scorer scorer = w.scorer(s.getIndexReader());
            TestCase.assertTrue("query collected "+doc+" but skipTo("+i+") says no more docs!",scorer.skipTo(i));
            TestCase.assertEquals("query collected "+doc+" but skipTo("+i+") got to "+scorer.doc(),doc,scorer.doc());
            float skipToScore = scorer.score();
            TestCase.assertEquals("unstable skipTo("+i+") score!",skipToScore,scorer.score(),maxDiff); 
            TestCase.assertEquals("query assigned doc "+doc+" a score of <"+score+"> but skipTo("+i+") has <"+skipToScore+">!",score,skipToScore,maxDiff);
          }
          lastDoc[0] = doc;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    Weight w = q.weight(s);
    Scorer scorer = w.scorer(s.getIndexReader());
    boolean more = scorer.skipTo(lastDoc[0]+1);
    if (more) 
      TestCase.assertFalse("query's last doc was "+lastDoc[0]+" but skipTo("+(lastDoc[0]+1)+") got to "+scorer.doc(),more);
  }
}
