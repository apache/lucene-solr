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
package org.apache.solr.client.solrj.io.stream;

import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

/**
 **/

public class StreamExpressionToExpessionTest extends LuceneTestCase {

  private StreamFactory factory;
  
  public StreamExpressionToExpessionTest() {
    super();
    
    factory = new StreamFactory()
                    .withCollectionZkHost("collection1", "testhost:1234")
                    .withStreamFunction("search", CloudSolrStream.class)
                    .withStreamFunction("merge", MergeStream.class)
                    .withStreamFunction("unique", UniqueStream.class)
                    .withStreamFunction("top", RankStream.class)
                    .withStreamFunction("group", ReducerStream.class)
                    ;
  }
  
  
  @Test
  public void testCloudSolrStream() throws Exception {

    CloudSolrStream stream;
    String expressionString;
    
    // Basic test
    stream = new CloudSolrStream(StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("search(collection1,"));
    assertTrue(expressionString.contains("q=\"*:*\""));
    assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
    assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
    
    // Basic w/aliases
    stream = new CloudSolrStream(StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"id=izzy,a_s=kayden\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("id=izzy"));
    assertTrue(expressionString.contains("a_s=kayden"));

  }

  @Test
  public void testUniqueStream() throws Exception {

    UniqueStream stream;
    String expressionString;
    
    // Basic test
    stream = new UniqueStream(StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f asc\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("unique(search(collection1"));
    assertTrue(expressionString.contains("over=\"a_f asc\""));
  }
  
  @Test
  public void testMergeStream() throws Exception {

    MergeStream stream;
    String expressionString;
    
    // Basic test
    stream = new MergeStream(StreamExpressionParser.parse("merge("
                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "on=\"a_f asc, a_s asc\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("q=\"id:(0 3 4)\""));
    assertTrue(expressionString.contains("q=\"id:(1 2)\""));
    assertTrue(expressionString.contains("on=\"a_f asc,a_s asc\""));
  }
  
  @Test
  public void testRankStream() throws Exception {

    RankStream stream;
    String expressionString;
    
    // Basic test
    stream = new RankStream(StreamExpressionParser.parse("top("
                                              + "n=3,"
                                              + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc,a_i asc\"),"
                                              + "sort=\"a_f asc, a_i asc\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("top(n=3,search(collection1"));
    assertTrue(expressionString.contains("sort=\"a_f asc,a_i asc\""));
    // find 2nd instance of sort
    assertTrue(expressionString.substring(expressionString.indexOf("sort=\"a_f asc,a_i asc\"") + 1).contains("sort=\"a_f asc,a_i asc\""));
  }

  @Test
  public void testReducerStream() throws Exception {

    ReducerStream stream;
    String expressionString;
    
    // Basic test
    stream = new ReducerStream(StreamExpressionParser.parse("group("
                                                  + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc, a_f asc\"),"
                                                  + "by=\"a_s desc\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("group(search(collection1"));
    assertTrue(expressionString.contains("by=\"a_s desc\""));
  }

}
