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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
*/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class JDBCStreamTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  static {
    schemaString = "schema-streaming.xml";
  }

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
    
    // Initialize Database
    // Ok, so.....hsqldb is doing something totally weird so I thought I'd take a moment to explain it.
    // According to http://www.hsqldb.org/doc/1.8/guide/guide.html#N101EF, section "Components of SQL Expressions", clause "name",
    // "When an SQL statement is issued, any lowercase characters in unquoted identifiers are converted to uppercase."
    // :(   Like seriously....
    // So, for this reason and to simplify writing these tests I've decided that in all statements all table and column names 
    // will be in UPPERCASE. This is to ensure things look and behave consistently. Note that this is not a requirement of the 
    // JDBCStream and is only a carryover from the driver we are testing with.
    Class.forName("org.hsqldb.jdbcDriver").newInstance();
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement = connection.createStatement();
    statement.executeUpdate("create table COUNTRIES(CODE varchar(3) not null primary key, COUNTRY_NAME varchar(50), DELETED char(1) default 'N')");
    statement.executeUpdate("create table PEOPLE(ID int not null primary key, NAME varchar(50), COUNTRY_CODE char(2), DELETED char(1) default 'N')");
    statement.executeUpdate("create table PEOPLE_SPORTS(ID int not null primary key, PERSON_ID int, SPORT_NAME varchar(50), DELETED char(1) default 'N')");
    
  }

  @AfterClass
  public static void afterSuperClass() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("shutdown");
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-streaming.xml";
  }


  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }

  public static String SOLR_HOME() {
    return SOLR_HOME;
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

  public JDBCStreamTest() {
    super();
    sliceCount = 2;
  }

  @Test
  public void testAll() throws Exception{
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    // Run JDBC Only tests
    testJDBCSelect();
    testJDBCJoin();
    
    // Run JDBC + Solr tests
    testJDBCSolrMerge();
    testJDBCSolrInnerJoinExpression();
    testJDBCSolrInnerJoinRollupExpression();
    testJDBCSolrInnerJoinExpressionWithProperties();
    
    // Clear all data
    clearData();
    
    // Delete database
    // done during afterSuperClass(...)
  }
  
  private void clearData() throws Exception {
    // Clear Solr index
    del("*:*");
    commit();
    
    // Clear database
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("delete from COUNTRIES WHERE 1=1");
    statement.executeUpdate("delete from PEOPLE WHERE 1=1");
    statement.executeUpdate("delete from PEOPLE_SPORTS WHERE 1=1");
    statement.close();
    connection.close();
  }
  
  private void testJDBCSelect() throws Exception {
    clearData();
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.close();
    connection.close();
    
    TupleStream stream;
    List<Tuple> tuples;
    
    // Simple 1
    stream = new JDBCStream("jdbc:hsqldb:mem:.", "select CODE,COUNTRY_NAME from COUNTRIES order by CODE", new FieldComparator("CODE", ComparatorOrder.ASCENDING));
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrderOf(tuples, "CODE", "NL", "NO", "NP", "US");
    assertOrderOf(tuples, "COUNTRY_NAME", "Netherlands", "Norway", "Nepal", "United States");
    
    // Simple 2
    stream = new JDBCStream("jdbc:hsqldb:mem:.", "select CODE,COUNTRY_NAME from COUNTRIES order by COUNTRY_NAME", new FieldComparator("COUNTRY_NAME", ComparatorOrder.ASCENDING));
    tuples = getTuples(stream);
    
    assertEquals(4, tuples.size());
    assertOrderOf(tuples, "CODE", "NP", "NL", "NO", "US");
    assertOrderOf(tuples, "COUNTRY_NAME", "Nepal", "Netherlands", "Norway", "United States");
    
  }
  
  private void testJDBCJoin() throws Exception {
    clearData();
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (11,'Emma','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (12,'Grace','NI')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (13,'Hailey','NG')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (14,'Isabella','NF')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (15,'Lily','NE')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (16,'Madison','NC')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (17,'Mia','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (18,'Natalie','NZ')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (19,'Olivia','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (20,'Samantha','NR')");
    statement.close();
    connection.close();
    
    TupleStream stream;
    List<Tuple> tuples;
    
    // Simple 1
    stream = new JDBCStream("jdbc:hsqldb:mem:.", "select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE where COUNTRIES.CODE = 'NL' order by PEOPLE.ID", new FieldComparator("ID", ComparatorOrder.ASCENDING));
    tuples = getTuples(stream);
    
    assertEquals(3, tuples.size());
    assertOrderOf(tuples, "ID", 11, 17, 19);
    assertOrderOf(tuples, "NAME", "Emma", "Mia", "Olivia");    
  }
  
  private void testJDBCSolrMerge() throws Exception {
    clearData();
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('AL', 'Algeria')");
    statement.close();
    connection.close();
    
    // Load Solr
    indexr(id, "0", "code_s", "GB", "name_s", "Great Britian");
    indexr(id, "1", "code_s", "CA", "name_s", "Canada");
    commit();
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class);
    
    List<Tuple> tuples;
    
    // Simple 1
    TupleStream jdbcStream = new JDBCStream("jdbc:hsqldb:mem:.", "select CODE,COUNTRY_NAME from COUNTRIES order by CODE", new FieldComparator("CODE", ComparatorOrder.ASCENDING));
    TupleStream selectStream = new SelectStream(jdbcStream, new HashMap<String, String>(){{ put("CODE", "code_s"); put("COUNTRY_NAME", "name_s"); }});
    TupleStream searchStream = factory.constructStream("search(collection1, fl=\"code_s,name_s\",q=\"*:*\",sort=\"code_s asc\")");
    TupleStream mergeStream = new MergeStream(new FieldComparator("code_s", ComparatorOrder.ASCENDING), new TupleStream[]{selectStream,searchStream});
    
    tuples = getTuples(mergeStream);
    
    assertEquals(7, tuples.size());
    assertOrderOf(tuples, "code_s", "AL","CA","GB","NL","NO","NP","US");
    assertOrderOf(tuples, "name_s", "Algeria", "Canada", "Great Britian", "Netherlands", "Norway", "Nepal", "United States");
  }
  
  private void testJDBCSolrInnerJoinExpression() throws Exception{
    clearData();
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("jdbc", JDBCStream.class);
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (11,'Emma','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (12,'Grace','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (13,'Hailey','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (14,'Isabella','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (15,'Lily','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (16,'Madison','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (17,'Mia','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (18,'Natalie','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (19,'Olivia','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (20,'Samantha','US')");
    statement.close();
    connection.close();
    
    // Load solr data
    indexr(id, "1", "rating_f", "3.5", "personId_i", "11");
    indexr(id, "2", "rating_f", "5", "personId_i", "12");
    indexr(id, "3", "rating_f", "2.2", "personId_i", "13");
    indexr(id, "4", "rating_f", "4.3", "personId_i", "14");
    indexr(id, "5", "rating_f", "3.5", "personId_i", "15");
    indexr(id, "6", "rating_f", "3", "personId_i", "16");
    indexr(id, "7", "rating_f", "3", "personId_i", "17");
    indexr(id, "8", "rating_f", "4", "personId_i", "18");
    indexr(id, "9", "rating_f", "4.1", "personId_i", "19");
    indexr(id, "10", "rating_f", "4.8", "personId_i", "20");
    commit();

    String expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression =   
              "innerJoin("
            + "  select("
            + "    search(collection1, fl=\"personId_i,rating_f\", q=\"rating_f:*\", sort=\"personId_i asc\"),"
            + "    personId_i as personId,"
            + "    rating_f as rating"
            + "  ),"
            + "  select("
            + "    jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\", sort=\"ID asc\"),"
            + "    ID as personId,"
            + "    NAME as personName,"
            + "    COUNTRY_NAME as country"
            + "  ),"
            + "  on=\"personId\""
            + ")";

    stream = factory.constructStream(expression);
    tuples = getTuples(stream);
    
    assertEquals(10, tuples.size());
    assertOrderOf(tuples, "personId", 11,12,13,14,15,16,17,18,19,20);
    assertOrderOf(tuples, "rating", 3.5d,5d,2.2d,4.3d,3.5d,3d,3d,4d,4.1d,4.8d);
    assertOrderOf(tuples, "personName", "Emma","Grace","Hailey","Isabella","Lily","Madison","Mia","Natalie","Olivia","Samantha");
    assertOrderOf(tuples, "country", "Netherlands","United States","Netherlands","Netherlands","Netherlands","United States","United States","Netherlands","Netherlands","United States");
  }

  private void testJDBCSolrInnerJoinExpressionWithProperties() throws Exception{
    clearData();
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("jdbc", JDBCStream.class);
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (11,'Emma','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (12,'Grace','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (13,'Hailey','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (14,'Isabella','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (15,'Lily','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (16,'Madison','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (17,'Mia','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (18,'Natalie','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (19,'Olivia','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (20,'Samantha','US')");
    statement.close();
    connection.close();
    
    // Load solr data
    indexr(id, "1", "rating_f", "3.5", "personId_i", "11");
    indexr(id, "2", "rating_f", "5", "personId_i", "12");
    indexr(id, "3", "rating_f", "2.2", "personId_i", "13");
    indexr(id, "4", "rating_f", "4.3", "personId_i", "14");
    indexr(id, "5", "rating_f", "3.5", "personId_i", "15");
    indexr(id, "6", "rating_f", "3", "personId_i", "16");
    indexr(id, "7", "rating_f", "3", "personId_i", "17");
    indexr(id, "8", "rating_f", "4", "personId_i", "18");
    indexr(id, "9", "rating_f", "4.1", "personId_i", "19");
    indexr(id, "10", "rating_f", "4.8", "personId_i", "20");
    commit();

    String expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    // Basic test
    // the test here is the setting of the property get_column_name=true. In hsqldb if this value is set to true then the use of an 
    // as clause in a select will have no effect. As such even though we have PEOPLE.ID as PERSONID we will still expect the column
    // name to come out as ID and not PERSONID
    expression =   
              "innerJoin("
            + "  select("
            + "    search(collection1, fl=\"personId_i,rating_f\", q=\"rating_f:*\", sort=\"personId_i asc\"),"
            + "    personId_i as personId,"
            + "    rating_f as rating"
            + "  ),"
            + "  select("
            + "    jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID as PERSONID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\", sort=\"ID asc\", get_column_name=true),"
            + "    ID as personId,"
            + "    NAME as personName,"
            + "    COUNTRY_NAME as country"
            + "  ),"
            + "  on=\"personId\""
            + ")";

    stream = factory.constructStream(expression);
    tuples = getTuples(stream);
    
    assertEquals(10, tuples.size());
    assertOrderOf(tuples, "personId", 11,12,13,14,15,16,17,18,19,20);
    assertOrderOf(tuples, "rating", 3.5d,5d,2.2d,4.3d,3.5d,3d,3d,4d,4.1d,4.8d);
    assertOrderOf(tuples, "personName", "Emma","Grace","Hailey","Isabella","Lily","Madison","Mia","Natalie","Olivia","Samantha");
    assertOrderOf(tuples, "country", "Netherlands","United States","Netherlands","Netherlands","Netherlands","United States","United States","Netherlands","Netherlands","United States");
    
    // Basic test
    // the test here is the setting of the property get_column_name=false. In hsqldb if this value is set to false then the use of an 
    // as clause in a select will have effect. As such we have PEOPLE.ID as PERSONID we will still expect the column name to come out 
    // PERSONID and not ID
    expression =   
              "innerJoin("
            + "  select("
            + "    search(collection1, fl=\"personId_i,rating_f\", q=\"rating_f:*\", sort=\"personId_i asc\"),"
            + "    personId_i as personId,"
            + "    rating_f as rating"
            + "  ),"
            + "  select("
            + "    jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID as PERSONID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by PEOPLE.ID\", sort=\"PERSONID asc\", get_column_name=false),"
            + "    PERSONID as personId,"
            + "    NAME as personName,"
            + "    COUNTRY_NAME as country"
            + "  ),"
            + "  on=\"personId\""
            + ")";

    stream = factory.constructStream(expression);
    tuples = getTuples(stream);
    
    assertEquals(10, tuples.size());
    assertOrderOf(tuples, "personId", 11,12,13,14,15,16,17,18,19,20);
    assertOrderOf(tuples, "rating", 3.5d,5d,2.2d,4.3d,3.5d,3d,3d,4d,4.1d,4.8d);
    assertOrderOf(tuples, "personName", "Emma","Grace","Hailey","Isabella","Lily","Madison","Mia","Natalie","Olivia","Samantha");
    assertOrderOf(tuples, "country", "Netherlands","United States","Netherlands","Netherlands","Netherlands","United States","United States","Netherlands","Netherlands","United States");
  }

  
  private void testJDBCSolrInnerJoinRollupExpression() throws Exception{
    clearData();
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("hashJoin", HashJoinStream.class)
      .withFunctionName("rollup", RollupStream.class)
      .withFunctionName("jdbc", JDBCStream.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("count", CountMetric.class)
      ;
    
    // Load Database Data
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
    Statement statement = connection.createStatement();
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('US', 'United States')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NL', 'Netherlands')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NP', 'Nepal')");
    statement.executeUpdate("insert into COUNTRIES (CODE,COUNTRY_NAME) values ('NO', 'Norway')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (11,'Emma','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (12,'Grace','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (13,'Hailey','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (14,'Isabella','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (15,'Lily','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (16,'Madison','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (17,'Mia','US')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (18,'Natalie','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (19,'Olivia','NL')");
    statement.executeUpdate("insert into PEOPLE (ID, NAME, COUNTRY_CODE) values (20,'Samantha','US')");
    statement.close();
    connection.close();
    
    // Load solr data
    indexr(id, "1", "rating_f", "3.5", "personId_i", "11");
    indexr(id, "3", "rating_f", "2.2", "personId_i", "13");
    indexr(id, "4", "rating_f", "4.3", "personId_i", "14");
    indexr(id, "5", "rating_f", "3.5", "personId_i", "15");
    indexr(id, "8", "rating_f", "4", "personId_i", "18");
    indexr(id, "9", "rating_f", "4.1", "personId_i", "19");
    
    indexr(id, "2", "rating_f", "5", "personId_i", "12");
    indexr(id, "6", "rating_f", "3", "personId_i", "16");
    indexr(id, "7", "rating_f", "3", "personId_i", "17");
    indexr(id, "10", "rating_f", "4.8", "personId_i", "20");
    commit();

    String expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression =   
              "rollup("
            + "  hashJoin("
            + "    hashed=select("
            + "      search(collection1, fl=\"personId_i,rating_f\", q=\"rating_f:*\", sort=\"personId_i asc\"),"
            + "      personId_i as personId,"
            + "      rating_f as rating"
            + "    ),"
            + "    select("
            + "      jdbc(connection=\"jdbc:hsqldb:mem:.\", sql=\"select PEOPLE.ID, PEOPLE.NAME, COUNTRIES.COUNTRY_NAME from PEOPLE inner join COUNTRIES on PEOPLE.COUNTRY_CODE = COUNTRIES.CODE order by COUNTRIES.COUNTRY_NAME\", sort=\"COUNTRIES.COUNTRY_NAME asc\"),"
            + "      ID as personId,"
            + "      NAME as personName,"
            + "      COUNTRY_NAME as country"
            + "    ),"
            + "    on=\"personId\""
            + "  ),"
            + "  over=\"country\","
            + "  max(rating),"
            + "  min(rating),"
            + "  avg(rating),"
            + "  count(*)"
            + ")";

    stream = factory.constructStream(expression);
    tuples = getTuples(stream);
    
    assertEquals(2, tuples.size());
    
    Tuple tuple = tuples.get(0);
    assertEquals("Netherlands",tuple.getString("country"));
    assertTrue(4.3D == tuple.getDouble("max(rating)"));
    assertTrue(2.2D == tuple.getDouble("min(rating)"));
    assertTrue(3.6D == tuple.getDouble("avg(rating)"));
    assertTrue(6D == tuple.getDouble("count(*)"));
    
    tuple = tuples.get(1);
    assertEquals("United States",tuple.getString("country"));
    assertTrue(5D == tuple.getDouble("max(rating)"));
    assertTrue(3D == tuple.getDouble("min(rating)"));
    assertTrue(3.95D == tuple.getDouble("avg(rating)"));
    assertTrue(4D == tuple.getDouble("count(*)"));
    
  }
  
  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<Tuple>();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }
  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... values) throws Exception {
    int i = 0;
    for(int val : values) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get(fieldName);
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }
  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, double... values) throws Exception {
    int i = 0;
    for(double val : values) {
      Tuple t = tuples.get(i);
      double tip = (double)t.get(fieldName);
      if(tip != val) {
        throw new Exception("Found value:"+tip+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }
  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, String... values) throws Exception {
    int i = 0;
    for(String val : values) {
      Tuple t = tuples.get(i);
      
      if(null == val){
        if(null != t.get(fieldName)){
          throw new Exception("Found value:"+(String)t.get(fieldName)+" expecting:null");
        }
      }
      else{
        String tip = (String)t.get(fieldName);
        if(!tip.equals(val)) {
          throw new Exception("Found value:"+tip+" expecting:"+val);
        }
      }
      ++i;
    }
    return true;
  }

  protected boolean assertFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(!tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Expected field '%s' not found", field));
        }
      }
    }
    return true;
  }
  protected boolean assertNotFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Unexpected field '%s' found", field));
        }
      }
    }
    return true;
  }  

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long)tuple.get(fieldName);
    if(lv != l) {
      throw new Exception("Longs not equal:"+l+" : "+lv);
    }

    return true;
  }
  
  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String)tuple.get(fieldName);
    
    if( (null == expected && null != actual) ||
        (null != expected && null == actual) ||
        (null != expected && !expected.equals(actual))){
      throw new Exception("Longs not equal:"+expected+" : "+actual);
    }

    return true;
  }

  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
}
