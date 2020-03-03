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

import org.apache.solr.SolrTestCase;
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

public class StreamExpressionToExpessionTest extends SolrTestCase {

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
                    .withFunctionName("jdbc", JDBCStream.class)
                    .withFunctionName("intersect", IntersectStream.class)
                    .withFunctionName("complement", ComplementStream.class)
                    .withFunctionName("count", CountMetric.class)
                    .withFunctionName("sum", SumMetric.class)
                    .withFunctionName("min", MinMetric.class)
                    .withFunctionName("max", MaxMetric.class)
                    .withFunctionName("avg", MeanMetric.class)
                    .withFunctionName("daemon", DaemonStream.class)
                    .withFunctionName("topic", TopicStream.class)
                    .withFunctionName("tlogit", TextLogitStream.class)
                    .withFunctionName("featuresSelection", FeaturesSelectionStream.class)
                    ;
  }
    
  @Test
  public void testCloudSolrStream() throws Exception {

    String expressionString;
    
    // Basic test
    try (CloudSolrStream stream = new CloudSolrStream(StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", fq=\"a_s:one\", fq=\"a_s:two\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      System.out.println("ExpressionString: " + expressionString.toString());
      assertTrue(expressionString.contains("search(collection1,"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
      assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
      assertTrue(expressionString.contains("fq=\"a_s:one\""));
      assertTrue(expressionString.contains("fq=\"a_s:two\""));
    }
    // Basic w/aliases
    try (CloudSolrStream stream = new CloudSolrStream(StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"id=izzy,a_s=kayden\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("id=izzy"));
      assertTrue(expressionString.contains("a_s=kayden"));
    }
  }
  
  @Test
  public void testSelectStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (SelectStream stream = new SelectStream(StreamExpressionParser.parse("select(\"a_s as fieldA\", search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("select(search(collection1,"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
      assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
      assertTrue(expressionString.contains("a_s as fieldA"));
    }
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testDaemonStream() throws Exception {
    String expressionString;

    // Basic test
    try (DaemonStream stream = new DaemonStream(StreamExpressionParser.parse("daemon(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), id=\"blah\", runInterval=\"1000\", queueSize=\"100\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("daemon(search(collection1,"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
      assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
      assertTrue(expressionString.contains("id=blah"));
      assertTrue(expressionString.contains("queueSize=100"));
      assertTrue(expressionString.contains("runInterval=1000"));
    }
  }

  @Test
  public void testTopicStream() throws Exception {

    String expressionString;

    // Basic test
    try (TopicStream stream = new TopicStream(StreamExpressionParser.parse("topic(collection2, collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", id=\"blah\", checkpointEvery=1000)"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("topic(collection2,collection1"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
      assertTrue(expressionString.contains("id=blah"));
      assertTrue(expressionString.contains("checkpointEvery=1000"));
    }
  }

  @Test
  public void testStatsStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (StatsStream stream = new StatsStream(StreamExpressionParser.parse("stats(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", sum(a_i), avg(a_i), count(*), min(a_i), max(a_i))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("stats(collection1,"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("fl=\"id,a_s,a_i,a_f\""));
      assertTrue(expressionString.contains("sort=\"a_f asc, a_i asc\""));
      assertTrue(expressionString.contains("min(a_i)"));
      assertTrue(expressionString.contains("max(a_i)"));
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertTrue(expressionString.contains("sum(a_i)"));
    }
  }

  @Test
  public void testUniqueStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (UniqueStream stream = new UniqueStream(StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("unique(search(collection1"));
      assertTrue(expressionString.contains("over=a_f"));
    }
  }
  
  @Test
  public void testMergeStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (MergeStream stream = new MergeStream(StreamExpressionParser.parse("merge("
                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "on=\"a_f asc, a_s asc\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("q=\"id:(0 3 4)\""));
      assertTrue(expressionString.contains("q=\"id:(1 2)\""));
      assertTrue(expressionString.contains("on=\"a_f asc,a_s asc\""));
    }
  }
  
  @Test
  public void testRankStream() throws Exception {

    String expressionString;
    
    // Basic test
    try (RankStream stream = new RankStream(StreamExpressionParser.parse("top("
                                              + "n=3,"
                                              + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc,a_i asc\"),"
                                              + "sort=\"a_f asc, a_i asc\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("top(n=3,search(collection1"));
      assertTrue(expressionString.contains("sort=\"a_f asc,a_i asc\""));
      // find 2nd instance of sort
      assertTrue(expressionString.substring(expressionString.indexOf("sort=\"a_f asc,a_i asc\"") + 1).contains("sort=\"a_f asc,a_i asc\""));
    }
  }

  @Test
  public void testReducerStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (ReducerStream stream = new ReducerStream(StreamExpressionParser.parse("reduce("
                                                  + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc, a_f asc\"),"
                                                  + "by=\"a_s\", group(sort=\"a_i desc\", n=\"5\"))"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("reduce(search(collection1"));
      assertTrue(expressionString.contains("by=a_s"));
    }
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testUpdateStream() throws Exception {
    StreamExpression expression = StreamExpressionParser.parse("update("
                                                               + "collection2, "
                                                               + "batchSize=5, "
                                                               + "search("
                                                                 + "collection1, "
                                                                 + "q=*:*, "
                                                                 + "fl=\"id,a_s,a_i,a_f\", "
                                                                 + "sort=\"a_f asc, a_i asc\"))");
    
    try (UpdateStream updateStream = new UpdateStream(expression, factory)) {
      String expressionString = updateStream.toExpression(factory).toString();

      assertTrue(expressionString.contains("update(collection2"));
      assertTrue(expressionString.contains("batchSize=5"));
      assertTrue(expressionString.contains("search(collection1"));
    }
  }
  
  @Test
  public void testFacetStream() throws Exception {

    String expressionString;
    
    // Basic test
    try (FacetStream stream = new FacetStream(StreamExpressionParser.parse("facet("
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
        + ")"), factory)){
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
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("avg(a_f,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertEquals(stream.getBucketSizeLimit(), 100);
      assertEquals(stream.getRows(), 100);
      assertEquals(stream.getOffset(), 0);
    }

    try (FacetStream stream = new FacetStream(StreamExpressionParser.parse("facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) asc\", "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")"), factory)){
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("facet(collection1"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("buckets=a_s"));
      assertTrue(expressionString.contains("bucketSorts=\"sum(a_i) asc\""));
      assertTrue(expressionString.contains("rows=10"));
      assertTrue(expressionString.contains("offset=0"));
      assertTrue(expressionString.contains("overfetch=250"));
      assertTrue(expressionString.contains("sum(a_i)"));
      assertTrue(expressionString.contains("sum(a_f)"));
      assertTrue(expressionString.contains("min(a_i)"));
      assertTrue(expressionString.contains("min(a_f)"));
      assertTrue(expressionString.contains("max(a_i)"));
      assertTrue(expressionString.contains("max(a_f)"));
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("avg(a_f,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertEquals(stream.getOverfetch(), 250);
      assertEquals(stream.getBucketSizeLimit(), 260);
      assertEquals(stream.getRows(), 10);
      assertEquals(stream.getOffset(), 0);
    }

    try (FacetStream stream = new FacetStream(StreamExpressionParser.parse("facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) asc\", "
        +   "rows=10, method=dvhash, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")"), factory)){
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("facet(collection1"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("buckets=a_s"));
      assertTrue(expressionString.contains("bucketSorts=\"sum(a_i) asc\""));
      assertTrue(!expressionString.contains("bucketSizeLimit"));
      assertTrue(expressionString.contains("rows=10"));
      assertTrue(expressionString.contains("offset=0"));
      assertTrue(expressionString.contains("overfetch=250"));
      assertTrue(expressionString.contains("method=dvhash"));
      assertTrue(expressionString.contains("sum(a_i)"));
      assertTrue(expressionString.contains("sum(a_f)"));
      assertTrue(expressionString.contains("min(a_i)"));
      assertTrue(expressionString.contains("min(a_f)"));
      assertTrue(expressionString.contains("max(a_i)"));
      assertTrue(expressionString.contains("max(a_f)"));
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("avg(a_f,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertEquals(stream.getBucketSizeLimit(), 260);
      assertEquals(stream.getRows(), 10);
      assertEquals(stream.getOffset(), 0);
      assertEquals(stream.getOverfetch(), 250);

    }

    try (FacetStream stream = new FacetStream(StreamExpressionParser.parse("facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) asc\", "
        +   "rows=10, offset=100, overfetch=30, method=dvhash, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")"), factory)){
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("facet(collection1"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("buckets=a_s"));
      assertTrue(expressionString.contains("bucketSorts=\"sum(a_i) asc\""));
      assertTrue(!expressionString.contains("bucketSizeLimit"));
      assertTrue(expressionString.contains("rows=10"));
      assertTrue(expressionString.contains("offset=100"));
      assertTrue(expressionString.contains("overfetch=30"));
      assertTrue(expressionString.contains("method=dvhash"));
      assertTrue(expressionString.contains("sum(a_i)"));
      assertTrue(expressionString.contains("sum(a_f)"));
      assertTrue(expressionString.contains("min(a_i)"));
      assertTrue(expressionString.contains("min(a_f)"));
      assertTrue(expressionString.contains("max(a_i)"));
      assertTrue(expressionString.contains("max(a_f)"));
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("avg(a_f,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertEquals(stream.getBucketSizeLimit(), 140);
      assertEquals(stream.getRows(), 10);
      assertEquals(stream.getOffset(), 100);
      assertEquals(stream.getOverfetch(), 30);

    }

    try (FacetStream stream = new FacetStream(StreamExpressionParser.parse("facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) asc\", "
        +   "rows=-1, offset=100, overfetch=-1, method=dvhash, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")"), factory)){
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("facet(collection1"));
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("buckets=a_s"));
      assertTrue(expressionString.contains("bucketSorts=\"sum(a_i) asc\""));
      assertTrue(!expressionString.contains("bucketSizeLimit"));
      assertTrue(expressionString.contains("rows=-1"));
      assertTrue(expressionString.contains("offset=100"));
      assertTrue(expressionString.contains("overfetch=-1"));
      assertTrue(expressionString.contains("method=dvhash"));
      assertTrue(expressionString.contains("sum(a_i)"));
      assertTrue(expressionString.contains("sum(a_f)"));
      assertTrue(expressionString.contains("min(a_i)"));
      assertTrue(expressionString.contains("min(a_f)"));
      assertTrue(expressionString.contains("max(a_i)"));
      assertTrue(expressionString.contains("max(a_f)"));
      assertTrue(expressionString.contains("avg(a_i,false)"));
      assertTrue(expressionString.contains("avg(a_f,false)"));
      assertTrue(expressionString.contains("count(*)"));
      assertEquals(stream.getBucketSizeLimit(), Integer.MAX_VALUE);
      assertEquals(stream.getRows(), Integer.MAX_VALUE);
      assertEquals(stream.getOffset(), 100);
      assertEquals(stream.getOverfetch(), -1);
    }

  }
  
  @Test
  public void testJDBCStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (JDBCStream stream = new JDBCStream(StreamExpressionParser.parse("jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\", sort=\"ID asc\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("jdbc(connection=\"jdbc:hsqldb:mem:.\","));
      assertTrue(expressionString.contains("sql=\"select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\""));
      assertTrue(expressionString.contains("sort=\"ID asc\""));
    }
  }

  @Test 
  public void testIntersectStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (IntersectStream stream = new IntersectStream(StreamExpressionParser.parse("intersect("
                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "on=\"a_f, a_s\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("q=\"id:(0 3 4)\""));
      assertTrue(expressionString.contains("q=\"id:(1 2)\""));
      assertTrue(expressionString.contains("on=\"a_f,a_s\""));
    }
  }

  @Test 
  public void testComplementStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (ComplementStream stream = new ComplementStream(StreamExpressionParser.parse("complement("
                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "on=\"a_f, a_s\")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("q=\"id:(0 3 4)\""));
      assertTrue(expressionString.contains("q=\"id:(1 2)\""));
      assertTrue(expressionString.contains("on=\"a_f,a_s\""));
    }
  }
  
  @Test
  public void testCloudSolrStreamWithEscapedQuote() throws Exception {

    // The purpose of this test is to ensure that a parameter with a contained " character is properly
    // escaped when it is turned back into an expression. This is important when an expression is passed
    // to a worker (parallel stream) or even for other reasons when an expression is string-ified.
    
    // Basic test
    String originalExpressionString = "search(collection1,fl=\"id,first\",sort=\"first asc\",q=\"presentTitles:\\\"chief, executive officer\\\" AND age:[36 TO *]\")";
    try (CloudSolrStream firstStream = new CloudSolrStream(StreamExpressionParser.parse(originalExpressionString), factory)) {
      String firstExpressionString = firstStream.toExpression(factory).toString();

      try (CloudSolrStream secondStream = new CloudSolrStream(StreamExpressionParser.parse(firstExpressionString), factory)) {
        String secondExpressionString = secondStream.toExpression(factory).toString();

        assertTrue(firstExpressionString.contains("q=\"presentTitles:\\\"chief, executive officer\\\" AND age:[36 TO *]\""));
        assertTrue(secondExpressionString.contains("q=\"presentTitles:\\\"chief, executive officer\\\" AND age:[36 TO *]\""));
      }
    }
  }

  @Test
  public void testFeaturesSelectionStream() throws Exception {
    String expr = "featuresSelection(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4, positiveLabel=2)";
    try (FeaturesSelectionStream stream = new FeaturesSelectionStream(StreamExpressionParser.parse(expr), factory)) {
      String expressionString = stream.toExpression(factory).toString();
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("featureSet=first"));
      assertTrue(expressionString.contains("field=tv_text"));
      assertTrue(expressionString.contains("outcome=out_i"));
      assertTrue(expressionString.contains("numTerms=4"));
      assertTrue(expressionString.contains("positiveLabel=2"));
    }
  }

  @Test
  public void testTextLogitStreamWithFeaturesSelection() throws Exception {
    String expr = "tlogit(" +
        "collection1, " +
        "q=\"*:*\", " +
        "name=\"model\", " +
        "featuresSelection(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4), " +
        "field=\"tv_text\", " +
        "outcome=\"out_i\", " +
        "maxIterations=100)";
    try (TextLogitStream logitStream = new TextLogitStream(StreamExpressionParser.parse(expr), factory)) {
      String expressionString = logitStream.toExpression(factory).toString();
      assertTrue(expressionString.contains("q=\"*:*\""));
      assertTrue(expressionString.contains("name=model"));
      assertFalse(expressionString.contains("terms="));
      assertTrue(expressionString.contains("featuresSelection("));
      assertTrue(expressionString.contains("field=tv_text"));
      assertTrue(expressionString.contains("outcome=out_i"));
      assertTrue(expressionString.contains("maxIterations=100"));
    }
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
    
    assertEquals("avg(foo,false)", expressionString);
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
