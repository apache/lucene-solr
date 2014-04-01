package org.apache.lucene.queryparser.spans;

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

import org.apache.lucene.queryparser.complexPhrase.TestComplexPhraseQuery;
import org.apache.lucene.search.Query;

/**
 * Copied and pasted from TestComplexPhraseQuery r1569314.
 * Had to make small changes in syntax.
 */
public class TestComplexPhraseSpanQuery extends TestComplexPhraseQuery {

  @Override
  public Query getQuery(String qString) throws Exception {
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, 
        defaultFieldName, analyzer);
    return p.parse(qString);
  }
  
  @Override  
  public void testParserSpecificSyntax() throws Exception {
    //can't have boolean operators within a SpanNear
    //must rewrite as SpanNot !~ or ( OR ) clauses without the "OR"
    checkMatches("\"[jo* john]!~  smith\"", "2");
    checkMatches("\"(john johathon)  smith\"", "1,2");
    checkMatches("\"[jo* john]!~ smyth~\"", "2");
    checkMatches("\"john percival\"!~2,2", "1");
    
    //check multiterms with no hits
    checkMatches("\"john  nosuchword*\"", "");
    checkMatches("\"john  nosuchw?rd\"!~2,3", "1,3");
    checkMatches("\"nosuchw?rd john\"!~2,3", "");
    checkMatches("\"nosuchw?rd john\"", "");
    
    //WAS:
    //checkBadQuery("\"jo* \"smith\" \"");
    //IS: ignore test.  SpanQueryParser will parse this as 
    //1) "jo* "
    //2) smith 
    //3) " "
    checkBadQuery("\"(jo* -john)  smith\""); // can't have boolean operators in phrase

  }

}
