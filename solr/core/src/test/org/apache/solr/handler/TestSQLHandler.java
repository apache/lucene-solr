package org.apache.solr.handler;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StatsStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.params.CommonParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSQLHandler extends AbstractFullDistribZkTestBase {


  static {
    schemaString = "schema-sql.xml";
  }

  public TestSQLHandler() {
    sliceCount = 2;
  }

  //@BeforeClass
  //public static void beforeSuperClass() {
    //AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
 // }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig-sql.xml";
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");

    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  private void delete() throws Exception {
    deleteCore();
  }

  @Test
  public void doTest() throws Exception {
    testPredicate();
    testBasicSelect();
    testBasicGrouping();
    testBasicGroupingFacets();
    testAggregatesWithoutGrouping();
    testSQLException();
    testTimeSeriesGrouping();
    testTimeSeriesGroupingFacet();
    testParallelBasicGrouping();
    testParallelTimeSeriesGrouping();
  }

  private void testPredicate() throws Exception {

    SqlParser parser = new SqlParser();
    String sql = "select a from b where c = 'd'";
    Statement statement = parser.createStatement(sql);
    SQLHandler.SQLVisitor sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("(c:\"d\")"));

    //Add parens
    parser = new SqlParser();
    sql = "select a from b where (c = 'd')";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("(c:\"d\")"));

    //Phrase
    parser = new SqlParser();
    sql = "select a from b where (c = 'd d')";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("(c:\"d d\")"));

    // AND
    parser = new SqlParser();
    sql = "select a from b where ((c = 'd') AND (l = 'z'))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:\"d\") AND (l:\"z\"))"));

    // OR

    parser = new SqlParser();
    sql = "select a from b where ((c = 'd') OR (l = 'z'))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:\"d\") OR (l:\"z\"))"));

    // AND NOT

    parser = new SqlParser();
    sql = "select a from b where ((c = 'd') AND NOT (l = 'z'))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:\"d\") AND -(l:\"z\"))"));

    // NESTED
    parser = new SqlParser();
    sql = "select a from b where ((c = 'd') OR ((l = 'z') AND (m = 'j')))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:\"d\") OR ((l:\"z\") AND (m:\"j\")))"));

    // NESTED NOT
    parser = new SqlParser();
    sql = "select a from b where ((c = 'd') OR ((l = 'z') AND NOT (m = 'j')))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:\"d\") OR ((l:\"z\") AND -(m:\"j\")))"));

    // RANGE - Will have to do until SQL BETWEEN is supported.
    // NESTED
    parser = new SqlParser();
    sql = "select a from b where ((c = '[0 TO 100]') OR ((l = '(z)') AND (m = 'j')))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));

    assert(sqlVistor.query.equals("((c:[0 TO 100]) OR ((l:(z)) AND (m:\"j\")))"));

    // Wildcard
    parser = new SqlParser();
    sql = "select a from b where ((c = '[0 TO 100]') OR ((l = '(z*)') AND (m = 'j')))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));
    assert(sqlVistor.query.equals("((c:[0 TO 100]) OR ((l:(z*)) AND (m:\"j\")))"));

    // Complex Lucene/Solr Query
    parser = new SqlParser();
    sql = "select a from b where ((c = '[0 TO 100]') OR ((l = '(z*)') AND (m = '(j OR (k NOT s))')))";
    statement = parser.createStatement(sql);
    sqlVistor = new SQLHandler.SQLVisitor(new StringBuilder());
    sqlVistor.process(statement, new Integer(0));
    assert(sqlVistor.query.equals("((c:[0 TO 100]) OR ((l:(z*)) AND (m:(j OR (k NOT s)))))"));
  }

  private void testBasicSelect() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
      indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
      indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
      indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
      indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
      indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
      indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
      indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select id, field_i, str_s from collection1 where text='XXXX' order by field_i desc");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      assert(tuples.size() == 8);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("id") == 8);
      assert(tuple.getLong("field_i") == 60);
      assert(tuple.get("str_s").equals("c"));

      tuple = tuples.get(1);
      assert(tuple.getLong("id") == 7);
      assert(tuple.getLong("field_i") == 50);
      assert(tuple.get("str_s").equals("c"));

      tuple = tuples.get(2);
      assert(tuple.getLong("id") == 6);
      assert(tuple.getLong("field_i") == 40);
      assert(tuple.get("str_s").equals("c"));

      tuple = tuples.get(3);
      assert(tuple.getLong("id") == 5);
      assert(tuple.getLong("field_i") == 30);
      assert(tuple.get("str_s").equals("c"));

      tuple = tuples.get(4);
      assert(tuple.getLong("id") == 3);
      assert(tuple.getLong("field_i") == 20);
      assert(tuple.get("str_s").equals("a"));

      tuple = tuples.get(5);
      assert(tuple.getLong("id") == 4);
      assert(tuple.getLong("field_i") == 11);
      assert(tuple.get("str_s").equals("b"));

      tuple = tuples.get(6);
      assert(tuple.getLong("id") == 2);
      assert(tuple.getLong("field_i") == 8);
      assert(tuple.get("str_s").equals("b"));

      tuple = tuples.get(7);
      assert(tuple.getLong("id") == 1);
      assert(tuple.getLong("field_i") == 7);
      assert(tuple.get("str_s").equals("a"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select id, field_i, str_s from collection1 where text='XXXX' order by field_i desc limit 1");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.getLong("id") == 8);
      assert(tuple.getLong("field_i") == 60);
      assert(tuple.get("str_s").equals("c"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select id, field_i, str_s from collection1 where text='XXXX' AND id='(1 2 3)' order by field_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 3);

      tuple = tuples.get(0);
      assert(tuple.getLong("id") == 3);
      assert(tuple.getLong("field_i") == 20);
      assert(tuple.get("str_s").equals("a"));

      tuple = tuples.get(1);
      assert(tuple.getLong("id") == 2);
      assert(tuple.getLong("field_i") == 8);
      assert(tuple.get("str_s").equals("b"));

      tuple = tuples.get(2);
      assert(tuple.getLong("id") == 1);
      assert(tuple.getLong("field_i") == 7);
      assert(tuple.get("str_s").equals("a"));

    } finally {
      delete();
    }
  }

  private void testSQLException() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
      indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
      indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
      indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
      indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
      indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
      indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
      indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
      commit();

      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select id, field_i, str_s from collection1 where text='XXXX' order by field_iff desc");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      Tuple tuple = getTuple(new ExceptionStream(solrStream));
      assert(tuple.EOF);
      assert(tuple.EXCEPTION);
      //A parse exception detected before being sent to the search engine
      assert(tuple.getException().contains("Fields in the sort spec must be included in the field list"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select id, field_iff, str_s from collection1 where text='XXXX' order by field_iff desc");

      solrStream = new SolrStream(jetty.url, params);
      tuple = getTuple(new ExceptionStream(solrStream));
      assert(tuple.EOF);
      assert(tuple.EXCEPTION);
      //An exception not detected by the parser thrown from the /select handler
      assert(tuple.getException().contains("sort param field can't be found:"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_iff), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_iff) = 19) AND (min(field_i) = 8))");

      solrStream = new SolrStream(jetty.url, params);
      tuple = getTuple(new ExceptionStream(solrStream));
      assert(tuple.EOF);
      assert(tuple.EXCEPTION);
      //An exception not detected by the parser thrown from the /export handler
      assert(tuple.getException().contains("undefined field:"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), blah(field_iff), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_iff) = 19) AND (min(field_i) = 8))");

      solrStream = new SolrStream(jetty.url, params);
      tuple = getTuple(new ExceptionStream(solrStream));
      assert(tuple.EOF);
      assert(tuple.EXCEPTION);
      //An exception not detected by the parser thrown from the /export handler
      assert(tuple.getException().contains("Invalid function: blah"));

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s from collection1 where text='XXXX' group by str_s");

      solrStream = new SolrStream(jetty.url, params);
      tuple = getTuple(new ExceptionStream(solrStream));
      assert(tuple.EOF);
      assert(tuple.EXCEPTION);
      assert(tuple.getException().contains("Group by queries must include atleast one aggregate function."));

    } finally {
      delete();
    }
  }

  private void testBasicGrouping() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
      indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
      indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
      indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
      indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
      indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
      indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
      indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s order by sum(field_i) asc limit 2");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where (text='XXXX' AND NOT text='XXXX XXX') group by str_s order by str_s desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //The sort by and order by match and no limit is applied. All the Tuples should be returned in
      //this scenario.

      assert(tuples.size() == 3);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("c"));
      assert(tuple.getDouble("count(*)") == 4);
      assert(tuple.getDouble("sum(field_i)") == 180);
      assert(tuple.getDouble("min(field_i)") == 30);
      assert(tuple.getDouble("max(field_i)") == 60);
      assert(tuple.getDouble("avg(field_i)") == 45);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(2);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);


      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 100))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 0);


    } finally {
      delete();
    }
  }

  private void testBasicGroupingFacets() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
      indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
      indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
      indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
      indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
      indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
      indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
      indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s order by sum(field_i) asc limit 2");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where (text='XXXX' AND NOT text='XXXX XXX') group by str_s order by str_s desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //The sort by and order by match and no limit is applied. All the Tuples should be returned in
      //this scenario.

      assert(tuples.size() == 3);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("c"));
      assert(tuple.getDouble("count(*)") == 4);
      assert(tuple.getDouble("sum(field_i)") == 180);
      assert(tuple.getDouble("min(field_i)") == 30);
      assert(tuple.getDouble("max(field_i)") == 60);
      assert(tuple.getDouble("avg(field_i)") == 45);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(2);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);


      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 100))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 0);


    } finally {
      delete();
    }
  }





  private void testParallelBasicGrouping() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
      indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
      indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
      indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
      indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
      indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
      indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
      indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", "2");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s order by sum(field_i) asc limit 2");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", "2");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s order by str_s desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //The sort by and order by match and no limit is applied. All the Tuples should be returned in
      //this scenario.

      assert(tuples.size() == 3);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("c"));
      assert(tuple.getDouble("count(*)") == 4);
      assert(tuple.getDouble("sum(field_i)") == 180);
      assert(tuple.getDouble("min(field_i)") == 30);
      assert(tuple.getDouble("max(field_i)") == 60);
      assert(tuple.getDouble("avg(field_i)") == 45);

      tuple = tuples.get(1);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(2);
      assert(tuple.get("str_s").equals("a"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 27);
      assert(tuple.getDouble("min(field_i)") == 7);
      assert(tuple.getDouble("max(field_i)") == 20);
      assert(tuple.getDouble("avg(field_i)") == 13.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", "2");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", "2");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 1);

      tuple = tuples.get(0);
      assert(tuple.get("str_s").equals("b"));
      assert(tuple.getDouble("count(*)") == 2);
      assert(tuple.getDouble("sum(field_i)") == 19);
      assert(tuple.getDouble("min(field_i)") == 8);
      assert(tuple.getDouble("max(field_i)") == 11);
      assert(tuple.getDouble("avg(field_i)") == 9.5D);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", "2");
      params.put("sql", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 100))");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      assert(tuples.size() == 0);

    } finally {
      delete();
    }
  }


  private void testAggregatesWithoutGrouping() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5");
    indexr(id, "5", "a_s", "hello3", "a_i", "10", "a_f", "6");
    indexr(id, "6", "a_s", "hello4", "a_i", "11", "a_f", "7");
    indexr(id, "7", "a_s", "hello3", "a_i", "12", "a_f", "8");
    indexr(id, "8", "a_s", "hello3", "a_i", "13", "a_f", "9");
    indexr(id, "9", "a_s", "hello0", "a_i", "14", "a_f", "10");

    commit();

    Map params = new HashMap();
    params.put(CommonParams.QT, "/sql");
    params.put("sql", "select count(*), sum(a_i), min(a_i), max(a_i), avg(a_i), sum(a_f), min(a_f), max(a_f), avg(a_f) from collection1");

    SolrStream solrStream = new SolrStream(jetty.url, params);


    List<Tuple> tuples = getTuples(solrStream);

    assert(tuples.size() == 1);

    //Test Long and Double Sums

    Tuple tuple = tuples.get(0);

    Double sumi = tuple.getDouble("sum(a_i)");
    Double sumf = tuple.getDouble("sum(a_f)");
    Double mini = tuple.getDouble("min(a_i)");
    Double minf = tuple.getDouble("min(a_f)");
    Double maxi = tuple.getDouble("max(a_i)");
    Double maxf = tuple.getDouble("max(a_f)");
    Double avgi = tuple.getDouble("avg(a_i)");
    Double avgf = tuple.getDouble("avg(a_f)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(sumi.longValue() == 70);
    assertTrue(sumf.doubleValue() == 55.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 7.0D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 10);


    // Test where clause hits

    params = new HashMap();
    params.put(CommonParams.QT, "/sql");
    params.put("sql", "select count(*), sum(a_i), min(a_i), max(a_i), avg(a_i), sum(a_f), min(a_f), max(a_f), avg(a_f) from collection1 where id = 2");

    solrStream = new SolrStream(jetty.url, params);

    tuples = getTuples(solrStream);

    assert(tuples.size() == 1);

    tuple = tuples.get(0);

    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(sumi.longValue() == 2);
    assertTrue(sumf.doubleValue() == 2.0D);
    assertTrue(mini == 2);
    assertTrue(minf == 2);
    assertTrue(maxi == 2);
    assertTrue(maxf == 2);
    assertTrue(avgi.doubleValue() == 2.0D);
    assertTrue(avgf.doubleValue() == 2.0);
    assertTrue(count.doubleValue() == 1);


    // Test zero hits

    params = new HashMap();
    params.put(CommonParams.QT, "/sql");
    params.put("sql", "select count(*), sum(a_i), min(a_i), max(a_i), avg(a_i), sum(a_f), min(a_f), max(a_f), avg(a_f) from collection1 where a_s = 'blah'");

    solrStream = new SolrStream(jetty.url, params);

    tuples = getTuples(solrStream);

    assert(tuples.size() == 1);

    tuple = tuples.get(0);

    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(sumi.longValue() == 0);
    assertTrue(sumf.doubleValue() == 0.0D);
    assertTrue(mini == null);
    assertTrue(minf == null);
    assertTrue(maxi == null);
    assertTrue(maxf == null);
    assertTrue(Double.isNaN(avgi));
    assertTrue(Double.isNaN(avgf));
    assertTrue(count.doubleValue() == 0);

    del("*:*");
    commit();
  }




  private void testTimeSeriesGrouping() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
      indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
      indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
      indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
      indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
      indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
      indexr("id", "7", "year_i", "2014", "month_i", "4",  "day_i", "4", "item_i", "6");
      indexr("id", "8", "year_i", "2014", "month_i", "4",  "day_i", "2", "item_i", "1");

      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getDouble("sum(item_i)") == 66);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getDouble("sum(item_i)") == 7);

      params.put("sql", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i order by year_i desc, month_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 3);

      tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getDouble("sum(item_i)") == 57);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getDouble("sum(item_i)") == 9);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 7);

      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("sql", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i order by year_i desc, month_i desc, day_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 6);

      tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 8);
      assert(tuple.getDouble("sum(item_i)") == 42);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 7);
      assert(tuple.getDouble("sum(item_i)") == 15);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 3);
      assert(tuple.getDouble("sum(item_i)") == 5);

      tuple = tuples.get(3);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 1);
      assert(tuple.getDouble("sum(item_i)") == 4);

      tuple = tuples.get(4);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 6);

      tuple = tuples.get(5);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 2);
      assert(tuple.getDouble("sum(item_i)") == 1);

    } finally {
      delete();
    }
  }


  private void testTimeSeriesGroupingFacet() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
      indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
      indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
      indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
      indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
      indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
      indexr("id", "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6");
      indexr("id", "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1");

      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getDouble("sum(item_i)") == 66);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getDouble("sum(item_i)") == 7);


      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i order by year_i desc, month_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 3);

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getDouble("sum(item_i)") == 57);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getDouble("sum(item_i)") == 9);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 7);


      params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("aggregationMode", "facet");
      params.put("sql", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i order by year_i desc, month_i desc, day_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 6);

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 8);
      assert(tuple.getDouble("sum(item_i)") == 42);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 7);
      assert(tuple.getDouble("sum(item_i)") == 15);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 3);
      assert(tuple.getDouble("sum(item_i)") == 5);

      tuple = tuples.get(3);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 1);
      assert(tuple.getDouble("sum(item_i)") == 4);

      tuple = tuples.get(4);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 6);

      tuple = tuples.get(5);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 2);
      assert(tuple.getDouble("sum(item_i)") == 1);
    } finally {
      delete();
    }
  }

  private void testParallelTimeSeriesGrouping() throws Exception {
    try {

      CloudJettyRunner jetty = this.cloudJettys.get(0);

      del("*:*");

      commit();

      indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
      indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
      indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
      indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
      indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
      indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
      indexr("id", "7", "year_i", "2014", "month_i", "4",  "day_i", "4", "item_i", "6");
      indexr("id", "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1");

      commit();
      Map params = new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", 2);
      params.put("sql", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

      SolrStream solrStream = new SolrStream(jetty.url, params);
      List<Tuple> tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 2);

      Tuple tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getDouble("sum(item_i)") == 66);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getDouble("sum(item_i)") == 7);

      new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", 2);
      params.put("sql", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i order by year_i desc, month_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 3);

      tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getDouble("sum(item_i)") == 57);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getDouble("sum(item_i)") == 9);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 7);


      new HashMap();
      params.put(CommonParams.QT, "/sql");
      params.put("numWorkers", 2);
      params.put("sql", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i order by year_i desc, month_i desc, day_i desc");

      solrStream = new SolrStream(jetty.url, params);
      tuples = getTuples(solrStream);

      //Only two results because of the limit.
      assert(tuples.size() == 6);

      tuple = null;

      tuple = tuples.get(0);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 8);
      assert(tuple.getDouble("sum(item_i)") == 42);

      tuple = tuples.get(1);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 11);
      assert(tuple.getLong("day_i") == 7);
      assert(tuple.getDouble("sum(item_i)") == 15);

      tuple = tuples.get(2);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 3);
      assert(tuple.getDouble("sum(item_i)") == 5);

      tuple = tuples.get(3);
      assert(tuple.getLong("year_i") == 2015);
      assert(tuple.getLong("month_i") == 10);
      assert(tuple.getLong("day_i") == 1);
      assert(tuple.getDouble("sum(item_i)") == 4);

      tuple = tuples.get(4);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 4);
      assert(tuple.getDouble("sum(item_i)") == 6);

      tuple = tuples.get(5);
      assert(tuple.getLong("year_i") == 2014);
      assert(tuple.getLong("month_i") == 4);
      assert(tuple.getLong("day_i") == 2);
      assert(tuple.getDouble("sum(item_i)") == 1);

    } finally {
      delete();
    }
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList();
    for(;;) {
      Tuple t = tupleStream.read();
      if(t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }

  protected Tuple getTuple(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    Tuple t = tupleStream.read();
    tupleStream.close();
    return t;
  }
}
