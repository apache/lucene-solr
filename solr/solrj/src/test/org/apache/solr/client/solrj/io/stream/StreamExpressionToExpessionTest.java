package org.apache.solr.client.solrj.io.stream;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.junit.Test;

/**
 **/

public class StreamExpressionToExpessionTest extends LuceneTestCase {

  private StreamFactory factory;
  
  public StreamExpressionToExpessionTest() {
    super();
    
    factory = new StreamFactory()
                    .withCollectionZkHost("collection1", "testhost:1234")
                    .withCollectionZkHost("collection2", "testhost:1234")
                    .withFunctionName("search", CloudSolrStream.class)
                    .withFunctionName("select", SelectStream.class)
                    .withFunctionName("merge", MergeStream.class)
                    .withFunctionName("unique", UniqueStream.class)
                    .withFunctionName("top", RankStream.class)
                    .withFunctionName("reduce", ReducerStream.class)
                    .withFunctionName("group", GroupOperation.class)
                    .withFunctionName("update", UpdateStream.class)
                    .withFunctionName("stats", StatsStream.class)
                    .withFunctionName("facet", FacetStream.class)
                    .withFunctionName("count", CountMetric.class)
                    .withFunctionName("sum", SumMetric.class)
                    .withFunctionName("min", MinMetric.class)
                    .withFunctionName("max", MaxMetric.class)
                    .withFunctionName("avg", MeanMetric.class)
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
  public void testSelectStream() throws Exception {

    SelectStream stream;
    String expressionString;
    
    // Basic test
    stream = new SelectStream(StreamExpressionParser.parse("select(\"a_s as fieldA\", search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"))"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("select(search(collection1,"));
    assertTrue(expressionString.contains("q=\"*:*\""));
    assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
    assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
    assertTrue(expressionString.contains("a_s as fieldA"));
    
  }
  
  @Test
  public void testStatsStream() throws Exception {

    StatsStream stream;
    String expressionString;
    
    // Basic test
    stream = new StatsStream(StreamExpressionParser.parse("stats(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", sum(a_i), avg(a_i), count(*), min(a_i), max(a_i))"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("stats(collection1,"));
    assertTrue(expressionString.contains("q=\"*:*\""));
    assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
    assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
    assertTrue(expressionString.contains("min(a_i)"));
    assertTrue(expressionString.contains("max(a_i)"));
    assertTrue(expressionString.contains("avg(a_i)"));
    assertTrue(expressionString.contains("count(*)"));
    assertTrue(expressionString.contains("sum(a_i)"));
    
  }

  @Test
  public void testUniqueStream() throws Exception {

    UniqueStream stream;
    String expressionString;
    
    // Basic test
    stream = new UniqueStream(StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("unique(search(collection1"));
    assertTrue(expressionString.contains("over=a_f"));
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
    stream = new ReducerStream(StreamExpressionParser.parse("reduce("
                                                  + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc, a_f asc\"),"
                                                  + "by=\"a_s\", group(sort=\"a_i desc\", n=\"5\"))"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("reduce(search(collection1"));
    assertTrue(expressionString.contains("by=a_s"));
  }
  
  @Test
  public void testUpdateStream() throws Exception {
    StreamExpression expression = StreamExpressionParser.parse("update("
                                                               + "collection2, "
                                                               + "batchSize=5, "
                                                               + "search("
                                                                 + "collection1, "
                                                                 + "q=*:*, "
                                                                 + "fl=\"id,a_s,a_i,a_f\", "
                                                                 + "sort=\"a_f asc, a_i asc\"))");
    
    UpdateStream updateStream = new UpdateStream(expression, factory);
    String expressionString = updateStream.toExpression(factory).toString();
    
    assertTrue(expressionString.contains("update(collection2"));
    assertTrue(expressionString.contains("batchSize=5"));
    assertTrue(expressionString.contains("search(collection1"));
  }
  
  @Test
  public void testFacetStream() throws Exception {

    FacetStream stream;
    String expressionString;
    
    // Basic test
    stream = new FacetStream(StreamExpressionParser.parse("facet("
                                                        +   "collection1, "
                                                        +   "q=\"*:*\", "
                                                        +   "buckets=\"a_s\", "
                                                        +   "bucketSorts=\"sum(a_i) asc\", "
                                                        +   "bucketSizeLimit=100, "
                                                        +   "sum(a_i), sum(a_f), "
                                                        +   "min(a_i), min(a_f), "
                                                        +   "max(a_i), max(a_f), "
                                                        +   "avg(a_i), avg(a_f), "
                                                        +   "count(*)"
                                                        + ")"), factory);
    expressionString = stream.toExpression(factory).toString();
    assertTrue(expressionString.contains("facet(collection1"));
    assertTrue(expressionString.contains("q=\"*:*\""));
    assertTrue(expressionString.contains("buckets=a_s"));
    assertTrue(expressionString.contains("bucketSorts=\"sum(a_i) asc\""));
    assertTrue(expressionString.contains("bucketSizeLimit=100"));
    assertTrue(expressionString.contains("sum(a_i)"));
    assertTrue(expressionString.contains("sum(a_f)"));
    assertTrue(expressionString.contains("min(a_i)"));
    assertTrue(expressionString.contains("min(a_f)"));
    assertTrue(expressionString.contains("max(a_i)"));
    assertTrue(expressionString.contains("max(a_f)"));
    assertTrue(expressionString.contains("avg(a_i)"));
    assertTrue(expressionString.contains("avg(a_f)"));
    assertTrue(expressionString.contains("count(*)"));
  }
  
  @Test
  public void testCountMetric() throws Exception {

    Metric metric;
    String expressionString;
    
    // Basic test
    metric = new CountMetric(StreamExpressionParser.parse("count(*)"), factory);
    expressionString = metric.toExpression(factory).toString();
    
    assertEquals("count(*)", expressionString);
  }
  
  @Test
  public void testMaxMetric() throws Exception {

    Metric metric;
    String expressionString;
    
    // Basic test
    metric = new MaxMetric(StreamExpressionParser.parse("max(foo)"), factory);
    expressionString = metric.toExpression(factory).toString();
    
    assertEquals("max(foo)", expressionString);
  }
  
  @Test
  public void testMinMetric() throws Exception {

    Metric metric;
    String expressionString;
    
    // Basic test
    metric = new MinMetric(StreamExpressionParser.parse("min(foo)"), factory);
    expressionString = metric.toExpression(factory).toString();
    
    assertEquals("min(foo)", expressionString);
  }

  @Test
  public void testMeanMetric() throws Exception {

    Metric metric;
    String expressionString;
    
    // Basic test
    metric = new MeanMetric(StreamExpressionParser.parse("avg(foo)"), factory);
    expressionString = metric.toExpression(factory).toString();
    
    assertEquals("avg(foo)", expressionString);
  }
  
  @Test
  public void testSumMetric() throws Exception {

    Metric metric;
    String expressionString;
    
    // Basic test
    metric = new SumMetric(StreamExpressionParser.parse("sum(foo)"), factory);
    expressionString = metric.toExpression(factory).toString();
    
    assertEquals("sum(foo)", expressionString);
  }
}
