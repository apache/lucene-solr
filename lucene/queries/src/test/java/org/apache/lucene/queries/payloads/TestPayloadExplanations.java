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
package org.apache.lucene.queries.payloads;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BaseExplanationTestCase;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

/**
 * TestExplanations subclass focusing on payload queries
 */
public class TestPayloadExplanations extends BaseExplanationTestCase {

  private static PayloadFunction functions[] = new PayloadFunction[] {
      new AveragePayloadFunction(),
      new MinPayloadFunction(),
      new MaxPayloadFunction(),
  };

  /** macro for payloadscorequery */
  private SpanQuery pt(String s, PayloadFunction fn) {
    return new PayloadScoreQuery(new SpanTermQuery(new Term(FIELD,s)), fn, PayloadDecoder.FLOAT_DECODER, random().nextBoolean());
  }
  
  /* simple PayloadTermQueries */
  
  public void testPT1() throws Exception {
    for (PayloadFunction fn : functions) {
      qtest(pt("w1", fn), new int[] {0,1,2,3});
    }
  }

  public void testPT2() throws Exception {
    for (PayloadFunction fn : functions) {
      SpanQuery q = pt("w1", fn);
      qtest(new SpanBoostQuery(q, 1000), new int[] {0,1,2,3});
    }
  }

  public void testPT4() throws Exception {
    for (PayloadFunction fn : functions) {
      qtest(pt("xx", fn), new int[] {2,3});
    }
  }

  public void testPT5() throws Exception {
    for (PayloadFunction fn : functions) {
      SpanQuery q = pt("xx", fn);
      qtest(new SpanBoostQuery(q, 1000), new int[] {2,3});
    }
  }

  /*
    protected static final String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3 zz",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3 zz"
  };
   */

  public void testAllFunctions(SpanQuery query, int[] expected) throws Exception {
    for (PayloadFunction fn : functions) {
      qtest(new PayloadScoreQuery(query, fn, PayloadDecoder.FLOAT_DECODER, random().nextBoolean()), expected);
    }
  }

  public void testSimpleTerm() throws Exception {
    SpanTermQuery q = new SpanTermQuery(new Term(FIELD, "w2"));
    testAllFunctions(q, new int[]{ 0, 1, 2, 3});
  }

  public void testOrTerm() throws Exception {
    SpanOrQuery q = new SpanOrQuery(
        new SpanTermQuery(new Term(FIELD, "xx")), new SpanTermQuery(new Term(FIELD, "yy"))
    );
    testAllFunctions(q, new int[]{ 2, 3 });
  }

  public void testOrderedNearQuery() throws Exception {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "w3")), new SpanTermQuery(new Term(FIELD, "w2"))
        }, 1, true);
    testAllFunctions(q, new int[]{ 1, 3 });
  }

  public void testUnorderedNearQuery() throws Exception {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanTermQuery(new Term(FIELD, "w2")), new SpanTermQuery(new Term(FIELD, "w3"))
    }, 1, false);
    testAllFunctions(q, new int[]{ 0, 1, 2, 3 });
  }
}
