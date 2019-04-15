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

import junit.framework.Assert;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.junit.Test;

/**
 **/

public class StreamExpressionToExplanationTest extends SolrTestCase {

  private StreamFactory factory;
  
  public StreamExpressionToExplanationTest() {
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
                    ;
  }
    
  @Test
  public void testCloudSolrStream() throws Exception {
    // Basic test
    try (CloudSolrStream stream = new CloudSolrStream(StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("search", explanation.getFunctionName());
      Assert.assertEquals(CloudSolrStream.class.getName(), explanation.getImplementingClass());
    }
  }
  
  @Test
  public void testSelectStream() throws Exception {
    // Basic test
    try (SelectStream stream = new SelectStream(StreamExpressionParser.parse("select(\"a_s as fieldA\", search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"))"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("select", explanation.getFunctionName());
      Assert.assertEquals(SelectStream.class.getName(), explanation.getImplementingClass());
    }
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testDaemonStream() throws Exception {
    // Basic test
    try (DaemonStream stream = new DaemonStream(StreamExpressionParser.parse("daemon(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), id=\"blah\", runInterval=\"1000\", queueSize=\"100\")"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("daemon", explanation.getFunctionName());
      Assert.assertEquals(DaemonStream.class.getName(), explanation.getImplementingClass());
    }
  }

  @Test
  public void testTopicStream() throws Exception {
    // Basic test
    try (TopicStream stream = new TopicStream(StreamExpressionParser.parse("topic(collection2, collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", id=\"blah\", checkpointEvery=1000)"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("topic", explanation.getFunctionName());
      Assert.assertEquals(TopicStream.class.getName(), explanation.getImplementingClass());
    }
  }


  @Test
  public void testStatsStream() throws Exception {
    // Basic test
    try (StatsStream stream = new StatsStream(StreamExpressionParser.parse("stats(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", sum(a_i), avg(a_i), count(*), min(a_i), max(a_i))"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("stats", explanation.getFunctionName());
      Assert.assertEquals(StatsStream.class.getName(), explanation.getImplementingClass());
    }
  }

  @Test
  public void testUniqueStream() throws Exception {
    // Basic test
    try (UniqueStream stream = new UniqueStream(StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("unique", explanation.getFunctionName());
      Assert.assertEquals(UniqueStream.class.getName(), explanation.getImplementingClass());
    }
  }
  
  @Test
  public void testMergeStream() throws Exception {
    // Basic test
    try (MergeStream stream = new MergeStream(StreamExpressionParser.parse("merge("
                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                              + "on=\"a_f asc, a_s asc\")"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("merge", explanation.getFunctionName());
      Assert.assertEquals(MergeStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(2, ((StreamExplanation) explanation).getChildren().size());
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
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("top", explanation.getFunctionName());
      Assert.assertEquals(RankStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(1, ((StreamExplanation) explanation).getChildren().size());
    }
  }

  @Test
  public void testReducerStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (ReducerStream stream = new ReducerStream(StreamExpressionParser.parse("reduce("
                                                  + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc, a_f asc\"),"
                                                  + "by=\"a_s\", group(sort=\"a_i desc\", n=\"5\"))"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("reduce", explanation.getFunctionName());
      Assert.assertEquals(ReducerStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(1, ((StreamExplanation) explanation).getChildren().size());
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
      Explanation explanation = updateStream.toExplanation(factory);
      Assert.assertEquals("solr (collection2)", explanation.getFunctionName());
      Assert.assertEquals("Solr/Lucene", explanation.getImplementingClass());

      StreamExplanation updateExplanation = (StreamExplanation) explanation;
      Assert.assertEquals(1, updateExplanation.getChildren().size());
      Assert.assertEquals("update", updateExplanation.getChildren().get(0).getFunctionName());
      Assert.assertEquals(UpdateStream.class.getName(), updateExplanation.getChildren().get(0).getImplementingClass());
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
                                                        + ")"), factory)) {
      expressionString = stream.toExpression(factory).toString();
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("facet", explanation.getFunctionName());
      Assert.assertEquals(FacetStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(1, ((StreamExplanation) explanation).getChildren().size());
    }
  }
  
  @Test
  public void testJDBCStream() throws Exception {
    String expressionString;
    
    // Basic test
    try (JDBCStream stream = new JDBCStream(StreamExpressionParser.parse("jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\", sort=\"ID asc\")"), factory)) {
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("jdbc", explanation.getFunctionName());
      Assert.assertEquals(JDBCStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(1, ((StreamExplanation) explanation).getChildren().size());
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
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("intersect", explanation.getFunctionName());
      Assert.assertEquals(IntersectStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(2, ((StreamExplanation) explanation).getChildren().size());
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
      Explanation explanation = stream.toExplanation(factory);
      Assert.assertEquals("complement", explanation.getFunctionName());
      Assert.assertEquals(ComplementStream.class.getName(), explanation.getImplementingClass());
      Assert.assertEquals(2, ((StreamExplanation) explanation).getChildren().size());
    }
  }
}
