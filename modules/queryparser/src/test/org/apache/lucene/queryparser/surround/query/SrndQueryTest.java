package org.apache.lucene.queryparser.surround.query;

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

import org.apache.lucene.queryparser.surround.parser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 *
 *
 **/
public class SrndQueryTest extends LuceneTestCase {
  
  void checkEqualParsings(String s1, String s2) throws Exception {
    String fieldName = "foo";
    BasicQueryFactory qf = new BasicQueryFactory(16);
    Query lq1, lq2;
    lq1 = QueryParser.parse(s1).makeLuceneQueryField(fieldName, qf);
    lq2 = QueryParser.parse(s2).makeLuceneQueryField(fieldName, qf);
    QueryUtils.checkEqual(lq1, lq2);
  }

  @Test
  public void testHashEquals() throws Exception {
    //grab some sample queries from Test02Boolean and Test03Distance and
    //check there hashes and equals
    checkEqualParsings("word1 w word2", " word1  w  word2 ");
    checkEqualParsings("2N(w1,w2,w3)", " 2N(w1, w2 , w3)");
    checkEqualParsings("abc?", " abc? ");
    checkEqualParsings("w*rd?", " w*rd?");
  }
}
