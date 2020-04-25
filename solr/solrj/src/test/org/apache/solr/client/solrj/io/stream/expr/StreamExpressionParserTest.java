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
package org.apache.solr.client.solrj.io.stream.expr;

import org.apache.solr.SolrTestCase;

import org.junit.Test;

/**
 **/

public class StreamExpressionParserTest extends SolrTestCase {

  public StreamExpressionParserTest() {
    super();
  }
  
  
  @Test
  public void testParsing() throws Exception{
    StreamExpression actual, expected;
        
    actual = StreamExpressionParser.parse("aliases(a_i=alias.a_i)");
    expected = new StreamExpression("aliases")
                    .withParameter(new StreamExpressionNamedParameter("a_i", "alias.a_i"));
    assertEquals(expected,actual);
    
    actual = StreamExpressionParser.parse("search(a,b)");
    expected = new StreamExpression("search").withParameter("a").withParameter("b");
    assertEquals(expected, actual);

    actual = StreamExpressionParser.parse("search(collection1, q=*:*, sort=\"fieldA desc, fieldB asc, fieldC asc\")");
    expected = new StreamExpression("search")
                    .withParameter(new StreamExpressionValue("collection1"))
                    .withParameter(new StreamExpressionNamedParameter("q").withParameter("*:*"))
                    .withParameter(new StreamExpressionNamedParameter("sort").withParameter("fieldA desc, fieldB asc, fieldC asc"));
    assertEquals(expected,actual);
    
    actual = StreamExpressionParser.parse("unique(search(collection1, q=*:*, sort=\"fieldA desc, fieldB asc, fieldC asc\"))");
    expected = new StreamExpression("unique")
                .withParameter(new StreamExpression("search")
                    .withParameter(new StreamExpressionValue("collection1"))
                    .withParameter(new StreamExpressionNamedParameter("q").withParameter("*:*"))
                    .withParameter(new StreamExpressionNamedParameter("sort").withParameter("fieldA desc, fieldB asc, fieldC asc"))
                );
    assertEquals(expected,actual);
    
    actual = StreamExpressionParser.parse("unique(search(collection1, q=*:*, sort=\"fieldA desc, fieldB asc, fieldC asc\"), alt=search(collection1, foo=bar))");
    expected = new StreamExpression("unique")
                .withParameter(new StreamExpression("search")
                    .withParameter(new StreamExpressionValue("collection1"))
                    .withParameter(new StreamExpressionNamedParameter("q").withParameter("*:*"))
                    .withParameter(new StreamExpressionNamedParameter("sort").withParameter("fieldA desc, fieldB asc, fieldC asc")))
                .withParameter(new StreamExpressionNamedParameter("alt")
                    .withParameter(new StreamExpression("search")
                      .withParameter("collection1")
                      .withParameter(new StreamExpressionNamedParameter("foo")
                        .withParameter("bar"))));
    assertEquals(expected,actual);
    
    actual = StreamExpressionParser.parse("innerJoin("
                                + "left=search(collection1, q=*:*, fl=\"fieldA,fieldB,fieldC\", sort=\"fieldA asc, fieldB asc\"),"
                                + "right=search(collection2, q=*:*, fl=\"fieldA,fieldD\", sort=fieldA asc),"
                                + "on(equals(fieldA), notEquals(fieldC,fieldD))"
                                + ")");
    expected = new StreamExpression("innerJoin")
                .withParameter(new StreamExpressionNamedParameter("left")
                  .withParameter(new StreamExpression("search")
                    .withParameter("collection1")
                    .withParameter(new StreamExpressionNamedParameter("q","*:*"))
                    .withParameter(new StreamExpressionNamedParameter("fl","fieldA,fieldB,fieldC"))
                    .withParameter(new StreamExpressionNamedParameter("sort","fieldA asc, fieldB asc"))))
                .withParameter(new StreamExpressionNamedParameter("right")
                  .withParameter(new StreamExpression("search")
                    .withParameter("collection2")
                    .withParameter(new StreamExpressionNamedParameter("q","*:*"))
                    .withParameter(new StreamExpressionNamedParameter("fl","fieldA,fieldD"))
                    .withParameter(new StreamExpressionNamedParameter("sort","fieldA asc"))))
                .withParameter(new StreamExpression("on")
                  .withParameter(new StreamExpression("equals").withParameter("fieldA"))
                  .withParameter(new StreamExpression("notEquals").withParameter("fieldC").withParameter("fieldD")));
    assertEquals(expected,actual);    
  }
  
}
