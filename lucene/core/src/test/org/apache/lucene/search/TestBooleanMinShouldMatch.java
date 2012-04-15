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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.text.DecimalFormat;
import java.util.Random;

/** Test that BooleanQuery.setMinimumNumberShouldMatch works.
 */
public class TestBooleanMinShouldMatch extends LuceneTestCase {

    private static Directory index;
    private static IndexReader r;
    private static IndexSearcher s;

    @BeforeClass
    public static void beforeClass() throws Exception {
        String[] data = new String [] {
            "A 1 2 3 4 5 6",
            "Z       4 5 6",
            null,
            "B   2   4 5 6",
            "Y     3   5 6",
            null,
            "C     3     6",
            "X       4 5 6"
        };

        index = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), index);

        for (int i = 0; i < data.length; i++) {
            Document doc = new Document();
            doc.add(newField("id", String.valueOf(i), StringField.TYPE_STORED));//Field.Keyword("id",String.valueOf(i)));
            doc.add(newField("all", "all", StringField.TYPE_STORED));//Field.Keyword("all","all"));
            if (null != data[i]) {
                doc.add(newField("data", data[i], TextField.TYPE_STORED));//Field.Text("data",data[i]));
            }
            w.addDocument(doc);
        }

        r = w.getReader();
        s = newSearcher(r);
        w.close();
//System.out.println("Set up " + getName());
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
      s = null;
      r.close();
      r = null;
      index.close();
      index = null;
    }


    public void verifyNrHits(Query q, int expected) throws Exception {
        ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
        if (expected != h.length) {
            printHits(getName(), h, s);
        }
        assertEquals("result count", expected, h.length);
        //System.out.println("TEST: now check");
        QueryUtils.check(random(), q,s);
    }

    public void testAllOptional() throws Exception {

        BooleanQuery q = new BooleanQuery();
        for (int i = 1; i <=4; i++) {
            q.add(new TermQuery(new Term("data",""+i)), BooleanClause.Occur.SHOULD);//false, false);
        }
        q.setMinimumNumberShouldMatch(2); // match at least two of 4
        verifyNrHits(q, 2);
    }

    public void testOneReqAndSomeOptional() throws Exception {

        /* one required, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(2); // 2 of 3 optional 

        verifyNrHits(q, 5);
    }

    public void testSomeReqAndSomeOptional() throws Exception {

        /* two required, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(2); // 2 of 3 optional 

        verifyNrHits(q, 5);
    }

    public void testOneProhibAndSomeOptional() throws Exception {

        /* one prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(2); // 2 of 3 optional 

        verifyNrHits(q, 1);
    }

    public void testSomeProhibAndSomeOptional() throws Exception {

        /* two prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "C"  )), BooleanClause.Occur.MUST_NOT);//false, true );

        q.setMinimumNumberShouldMatch(2); // 2 of 3 optional 

        verifyNrHits(q, 1);
    }

    public void testOneReqOneProhibAndSomeOptional() throws Exception {

        /* one required, one prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);// true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(3); // 3 of 4 optional 

        verifyNrHits(q, 1);
    }

    public void testSomeReqOneProhibAndSomeOptional() throws Exception {

        /* two required, one prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all",  "all")), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(3); // 3 of 4 optional 

        verifyNrHits(q, 1);
    }

    public void testOneReqSomeProhibAndSomeOptional() throws Exception {

        /* one required, two prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "C"  )), BooleanClause.Occur.MUST_NOT);//false, true );

        q.setMinimumNumberShouldMatch(3); // 3 of 4 optional 

        verifyNrHits(q, 1);
    }

    public void testSomeReqSomeProhibAndSomeOptional() throws Exception {

        /* two required, two prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all",  "all")), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "C"  )), BooleanClause.Occur.MUST_NOT);//false, true );

        q.setMinimumNumberShouldMatch(3); // 3 of 4 optional 

        verifyNrHits(q, 1);
    }

    public void testMinHigherThenNumOptional() throws Exception {

        /* two required, two prohibited, some optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all",  "all")), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "5"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "4"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST_NOT);//false, true );
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "1"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "C"  )), BooleanClause.Occur.MUST_NOT);//false, true );

        q.setMinimumNumberShouldMatch(90); // 90 of 4 optional ?!?!?!

        verifyNrHits(q, 0);
    }

    public void testMinEqualToNumOptional() throws Exception {

        /* two required, two optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "6"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.SHOULD);//false, false);

        q.setMinimumNumberShouldMatch(2); // 2 of 2 optional 

        verifyNrHits(q, 1);
    }

    public void testOneOptionalEqualToMin() throws Exception {

        /* two required, one optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "3"  )), BooleanClause.Occur.SHOULD);//false, false);
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.MUST);//true,  false);

        q.setMinimumNumberShouldMatch(1); // 1 of 1 optional 

        verifyNrHits(q, 1);
    }

    public void testNoOptionalButMin() throws Exception {

        /* two required, no optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.MUST);//true,  false);
        q.add(new TermQuery(new Term("data", "2"  )), BooleanClause.Occur.MUST);//true,  false);

        q.setMinimumNumberShouldMatch(1); // 1 of 0 optional 

        verifyNrHits(q, 0);
    }

    public void testNoOptionalButMin2() throws Exception {

        /* one required, no optional */
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term("all", "all" )), BooleanClause.Occur.MUST);//true,  false);

        q.setMinimumNumberShouldMatch(1); // 1 of 0 optional 

        verifyNrHits(q, 0);
    }

    public void testRandomQueries() throws Exception {
      String field="data";
      String[] vals = {"1","2","3","4","5","6","A","Z","B","Y","Z","X","foo"};
      int maxLev=4;

      // callback object to set a random setMinimumNumberShouldMatch
      TestBoolean2.Callback minNrCB = new TestBoolean2.Callback() {
        public void postCreate(BooleanQuery q) {
          BooleanClause[] c =q.getClauses();
          int opt=0;
          for (int i=0; i<c.length;i++) {
            if (c[i].getOccur() == BooleanClause.Occur.SHOULD) opt++;
          }
          q.setMinimumNumberShouldMatch(random().nextInt(opt+2));
        }
      };



      // increase number of iterations for more complete testing      
      int num = atLeast(10);
      for (int i=0; i<num; i++) {
        int lev = random().nextInt(maxLev);
        final long seed = random().nextLong();
        BooleanQuery q1 = TestBoolean2.randBoolQuery(new Random(seed), true, lev, field, vals, null);
        // BooleanQuery q2 = TestBoolean2.randBoolQuery(new Random(seed), lev, field, vals, minNrCB);
        BooleanQuery q2 = TestBoolean2.randBoolQuery(new Random(seed), true, lev, field, vals, null);
        // only set minimumNumberShouldMatch on the top level query since setting
        // at a lower level can change the score.
        minNrCB.postCreate(q2);

        // Can't use Hits because normalized scores will mess things
        // up.  The non-sorting version of search() that returns TopDocs
        // will not normalize scores.
        TopDocs top1 = s.search(q1,null,100);
        TopDocs top2 = s.search(q2,null,100);
        if (i < 100) {
          QueryUtils.check(random(), q1,s);
          QueryUtils.check(random(), q2,s);
        }
        // The constrained query
        // should be a superset to the unconstrained query.
        if (top2.totalHits > top1.totalHits) {
          fail("Constrained results not a subset:\n"
                        + CheckHits.topdocsString(top1,0,0)
                        + CheckHits.topdocsString(top2,0,0)
                        + "for query:" + q2.toString());
        }

        for (int hit=0; hit<top2.totalHits; hit++) {
          int id = top2.scoreDocs[hit].doc;
          float score = top2.scoreDocs[hit].score;
          boolean found=false;
          // find this doc in other hits
          for (int other=0; other<top1.totalHits; other++) {
            if (top1.scoreDocs[other].doc == id) {
              found=true;
              float otherScore = top1.scoreDocs[other].score;
              // check if scores match
              assertEquals("Doc " + id + " scores don't match\n"
                  + CheckHits.topdocsString(top1,0,0)
                  + CheckHits.topdocsString(top2,0,0)
                  + "for query:" + q2.toString(),
                  score, otherScore, CheckHits.explainToleranceDelta(score, otherScore));
            }
          }

          // check if subset
          if (!found) fail("Doc " + id + " not found\n"
                + CheckHits.topdocsString(top1,0,0)
                + CheckHits.topdocsString(top2,0,0)
                + "for query:" + q2.toString());
        }
      }
      // System.out.println("Total hits:"+tot);
    }



    protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher) throws Exception {

        System.err.println("------- " + test + " -------");

        DecimalFormat f = new DecimalFormat("0.000000");

        for (int i = 0; i < h.length; i++) {
            Document d = searcher.doc(h[i].doc);
            float score = h[i].score;
            System.err.println("#" + i + ": " + f.format(score) + " - " +
                               d.get("id") + " - " + d.get("data"));
        }
    }
}
