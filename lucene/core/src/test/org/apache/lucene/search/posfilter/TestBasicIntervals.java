package org.apache.lucene.search.posfilter;

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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

public class TestBasicIntervals extends IntervalTestBase {

  public static final String field = "field";

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    for (String content : docFields) {
      Document doc = new Document();
      doc.add(newField(field, content, TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }
  }
  
  private String[] docFields = {
      "w1 w2 w3 w4 w5", //0
      "w1 w3 w2 w3",//1
      "w1 xx w2 yy w3",//2
      "w1 w3 xx w2 yy w3",//3
      "u2 u2 u1", //4
      "u2 xx u2 u1",//5
      "u2 u2 xx u1", //6
      "u2 xx u2 yy u1", //7
      "u2 xx u1 u2",//8
      "u1 u2 xx u2",//9
      "u2 u1 xx u2",//10
      "t1 t2 t1 t3 t2 t3",//11
      "v1 v2 v3",//12
      "v1 v3 v2 v3 v4",//13
      "v4 v2 v2 v4",//14
      "v3 v4 v3"};//15

  public void testSimpleConjunction() throws IOException {
    Query q = makeAndQuery(makeTermQuery("v2"), makeTermQuery("v4"));
    checkIntervals(q, searcher, new int[][]{
        { 13, 2, 2, 4, 4 },
        { 14, 0, 0, 1, 1, 2, 2, 3, 3 }
    });
  }

  public void testExclusion() throws IOException {
    Query q = makeBooleanQuery(makeBooleanClause("v2", BooleanClause.Occur.MUST),
                               makeBooleanClause("v3", BooleanClause.Occur.MUST_NOT));
    checkIntervals(q, searcher, new int[][]{
        { 14, 1, 1, 2, 2 }
    });
  }

  public void testOptExclusion() throws IOException {
    Query q = makeBooleanQuery(makeBooleanClause("w2", BooleanClause.Occur.SHOULD),
                               makeBooleanClause("w3", BooleanClause.Occur.SHOULD),
                               makeBooleanClause("xx", BooleanClause.Occur.MUST_NOT));
    checkIntervals(q, searcher, new int[][]{
        { 0, 1, 1, 2, 2 },
        { 1, 1, 1, 2, 2, 3, 3 }
    });
  }

  public void testNestedConjunctions() throws IOException {
    Query q = makeAndQuery(makeTermQuery("v2"), makeOrQuery(makeTermQuery("v3"), makeTermQuery("v4")));
    checkIntervals(q, searcher, new int[][]{
        { 12, 1, 1, 2, 2 },
        { 13, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 14, 0, 0, 1, 1, 2, 2, 3, 3 }
    });
  }

  public void testSingleRequiredManyOptional() throws IOException {
    Query q = makeBooleanQuery(makeBooleanClause("v2", BooleanClause.Occur.MUST),
                               makeBooleanClause("v3", BooleanClause.Occur.SHOULD),
                               makeBooleanClause("v4", BooleanClause.Occur.SHOULD));
    checkIntervals(q, searcher, new int[][]{
        { 12, 1, 1, 2, 2 },
        { 13, 1, 1, 2, 2, 3, 3, 4, 4 },
        { 14, 0, 0, 1, 1, 2, 2, 3, 3 }
    });
  }

  public void testSimpleTerm() throws IOException {
    Query q = makeTermQuery("u2");
    checkIntervals(q, searcher, new int[][]{
        { 4, 0, 0, 1, 1 },
        { 5, 0, 0, 2, 2 },
        { 6, 0, 0, 1, 1 },
        { 7, 0, 0, 2, 2 },
        { 8, 0, 0, 3, 3 },
        { 9, 1, 1, 3, 3 },
        { 10, 0, 0, 3, 3 }
    });
  }

  public void testBasicDisjunction() throws IOException {
    Query q = makeOrQuery(makeTermQuery("v3"), makeTermQuery("v2"));
    checkIntervals(q, searcher, new int[][]{
        { 12, 1, 1, 2, 2 },
        { 13, 1, 1, 2, 2, 3, 3 },
        { 14, 1, 1, 2, 2 },
        { 15, 0, 0, 2, 2 }
    });
  }


  
  public void testOrSingle() throws Exception {
    Query q = makeOrQuery(makeTermQuery("w5"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 4, 4 }
    });
  }

  public void testOrPartialMatch() throws Exception {
    Query q = makeOrQuery(makeTermQuery("w5"), makeTermQuery("xx"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 4, 4 },
        { 2, 1, 1 },
        { 3, 2, 2 },
        { 5, 1, 1 },
        { 6, 2, 2 },
        { 7, 1, 1 },
        { 8, 1, 1 },
        { 9, 2, 2 },
        { 10, 2, 2 },
    });
  }

  public void testOrDisjunctionMatch() throws Exception {
    Query q = makeOrQuery(makeTermQuery("w5"), makeTermQuery("yy"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 4, 4 },
        { 2, 3, 3 },
        { 3, 4, 4 },
        { 7, 3, 3 }
    });
  }

  // "t1 t2 t1 t3 t2 t3"
  //  -----------
  //     --------
  //        --------
  public void testOrSingleDocument() throws Exception {
    Query q = makeOrQuery(makeTermQuery("t1"), makeTermQuery("t2"), makeTermQuery("t3"));
    checkIntervals(q, searcher, new int[][]{
        { 11, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 }
    });
  }

  // andnot(andnot(w1, or(w2, flurble)), or(foo, bar))
  public void testConjunctionExclusionQuery() throws IOException {
    BooleanQuery andnotinner = new BooleanQuery();
    andnotinner.add(makeTermQuery("w1"), BooleanClause.Occur.MUST);
    BooleanQuery andnotinneror = new BooleanQuery();
    andnotinneror.add(makeTermQuery("w2"), BooleanClause.Occur.SHOULD);
    andnotinneror.add(makeTermQuery("flurble"), BooleanClause.Occur.SHOULD);
    andnotinner.add(andnotinneror, BooleanClause.Occur.MUST_NOT);
    BooleanQuery outer = new BooleanQuery();
    outer.add(andnotinner, BooleanClause.Occur.MUST);
    BooleanQuery andnotouteror = new BooleanQuery();
    andnotouteror.add(makeTermQuery("foo"), BooleanClause.Occur.SHOULD);
    andnotouteror.add(makeTermQuery("bar"), BooleanClause.Occur.SHOULD);
    outer.add(andnotouteror, BooleanClause.Occur.MUST_NOT);
    checkIntervals(outer, searcher, new int[][]{});
  }
  
}
