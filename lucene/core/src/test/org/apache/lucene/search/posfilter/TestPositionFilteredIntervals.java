package org.apache.lucene.search.posfilter;

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

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

@Seed("5DA7DDB9F83D92AC:D927BA1FCE8EC808")
public class TestPositionFilteredIntervals extends IntervalTestBase {

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

  public void testNearOrdered01() throws Exception {
    Query q = new OrderedNearQuery(0, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 }
    });
  }

  public void testNearOrdered02() throws Exception {
    Query q = new OrderedNearQuery(1, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 },
        { 1, 0, 3 }
    });
  }

  public void testNearOrdered03() throws Exception {
    Query q = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 },
        { 1, 0, 3 },
        { 2, 0, 4 }
    });
  }

  public void testNearOrdered04() throws Exception {
    Query q = new OrderedNearQuery(3, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 },
        { 1, 0, 3 },
        { 2, 0, 4 },
        { 3, 0, 5 }
    });
  }

  public void testNearOrdered05() throws Exception {
    Query q = new OrderedNearQuery(4, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 },
        { 1, 0, 3 },
        { 2, 0, 4 },
        { 3, 0, 5 }
    });
  }

  public void testNearOrderedEqual01() throws Exception {
    Query q = new OrderedNearQuery(0, makeTermQuery("w1"), makeTermQuery("w3"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{});
  }

  public void testNearOrderedEqual02() throws Exception {
    Query q = new OrderedNearQuery(1, makeTermQuery("w1"), makeTermQuery("w3"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 1, 0, 3 }
    });
  }

  public void testNearOrderedEqual03() throws Exception {
    Query q = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w3"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 1, 0, 3 }
    });
  }

  public void testNearOrderedEqual04() throws Exception {
    Query q = new OrderedNearQuery(3, makeTermQuery("w1"), makeTermQuery("w3"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 1, 0, 3 },
        { 3, 0, 5 }
    });
  }

  public void testNearOrderedEqual11() throws Exception {
    Query q = new OrderedNearQuery(0, makeTermQuery("u2"), makeTermQuery("u2"), makeTermQuery("u1"));
    checkIntervals(q, searcher, new int[][]{
        { 4, 0, 2 }
    });
  }

  public void testNearOrderedEqual13() throws Exception {
    Query q = new OrderedNearQuery(1, makeTermQuery("u2"), makeTermQuery("u2"), makeTermQuery("u1"));
    checkIntervals(q, searcher, new int[][]{
        { 4, 0, 2 },
        { 5, 0, 3 },
        { 6, 0, 3 }
    });
  }

  public void testNearOrderedEqual14() throws Exception {
    Query q = new OrderedNearQuery(2, makeTermQuery("u2"), makeTermQuery("u2"), makeTermQuery("u1"));
    checkIntervals(q, searcher, new int[][]{
        { 4, 0, 2 },
        { 5, 0, 3 },
        { 6, 0, 3 },
        { 7, 0, 4 }
    });
  }

  public void testNearOrderedEqual15() throws Exception {
    Query q = new OrderedNearQuery(3, makeTermQuery("u2"), makeTermQuery("u2"), makeTermQuery("u1"));
    checkIntervals(q, searcher, new int[][]{
        { 4, 0, 2 },
        { 5, 0, 3 },
        { 6, 0, 3 },
        { 7, 0, 4 }
    });
  }

  public void testNearOrderedOverlap() throws Exception {
    Query q = new OrderedNearQuery(3, makeTermQuery("t1"), makeTermQuery("t2"), makeTermQuery("t3"));
    checkIntervals(q, searcher, new int[][]{
        { 11, 0, 3, 2, 5 }
    });
  }

  public void testNearUnordered() throws Exception {
    Query q = new UnorderedNearQuery(0, makeTermQuery("u1"), makeTermQuery("u2"));
    checkIntervals(q, searcher, new int[][]{
        { 4, 1, 2 },
        { 5, 2, 3 },
        { 8, 2, 3 },
        { 9, 0, 1 },
        { 10, 0, 1 }
    });
  }

  public void testMultipleNearUnordered() throws Exception {
    Query q = new UnorderedNearQuery(1, makeTermQuery("w1"), makeTermQuery("w2"), makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 0, 2 },
        { 1, 0, 2 },
        { 3, 0, 3 }
    });
  }
  /*
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
      "t1 t2 t1 t3 t2 t3"};//11
   */

  // ((u1 near u2) and xx)
  public void testNestedNear() throws Exception {

    Query q = new UnorderedNearQuery(0, makeTermQuery("u1"), makeTermQuery("u2"));
    BooleanQuery topq = new BooleanQuery();
    topq.add(q, BooleanClause.Occur.MUST);
    topq.add(makeTermQuery("xx"), BooleanClause.Occur.MUST);

    checkIntervals(topq, searcher, new int[][]{
        { 5, 1, 1, 2, 3 },
        { 8, 1, 1, 2, 3 },
        { 9, 0, 1, 2, 2 },
        { 10, 0, 1, 2, 2 }
    });

  }
}
