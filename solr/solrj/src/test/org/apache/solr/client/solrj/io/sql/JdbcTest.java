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
package org.apache.solr.client.solrj.io.sql;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 * SolrStream will get fully exercised through these tests.
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class JdbcTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();


  static {
    schemaString = "schema-sql.xml";
  }

  @BeforeClass
  public static void beforeSuperClass() {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME);
  }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig-sql.xml";
  }

  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }


  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
  }


  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  @Test
  @ShardsFixed(num = 2)
  public void doTest() throws Exception {

    waitForRecoveriesToFinish(false);

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1", "testnull_i", null);
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2", "testnull_i", "2");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "testnull_i", null);
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "testnull_i", "4");
    indexr(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5", "testnull_i", null);
    indexr(id, "5", "a_s", "hello3", "a_i", "10", "a_f", "6", "testnull_i", "6");
    indexr(id, "6", "a_s", "hello4", "a_i", "11", "a_f", "7", "testnull_i", null);
    indexr(id, "7", "a_s", "hello3", "a_i", "12", "a_f", "8", "testnull_i", "8");
    indexr(id, "8", "a_s", "hello3", "a_i", "13", "a_f", "9", "testnull_i", null);
    indexr(id, "9", "a_s", "hello0", "a_i", "14", "a_f", "10", "testnull_i", "10");

    commit();

    String zkHost = zkServer.getZkAddress();

    Properties props = new Properties();

    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i desc limit 2")) {
          assertTrue(rs.getMetaData() != null);

          assert(rs.next());
          assert(rs.getLong("a_i") == 14);
          assert(rs.getLong(2) == 14);
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(3).equals("hello0"));
          assert(rs.getDouble("a_f") == 10);
          assert(rs.getDouble(4) == 10);

          assert(rs.next());
          assert(rs.getLong("a_i") == 13);
          assert(rs.getLong(2) == 13);
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(3).equals("hello3"));
          assert(rs.getDouble("a_f") == 9);
          assert(rs.getDouble(4) == 9);
          assert(!rs.next());
        }

        //Test statement reuse
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc limit 2")) {
          assert(rs.next());
          assert(rs.getLong("a_i") == 0);
          assert(rs.getLong(2) == 0);
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(3).equals("hello0"));
          assert(rs.getDouble("a_f") == 1);
          assert(rs.getDouble(4) == 1);

          assert(rs.next());
          assert(rs.getLong("a_i") == 1);
          assert(rs.getLong(2) == 1);
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(3).equals("hello0"));
          assert(rs.getDouble("a_f") == 5);
          assert(rs.getDouble(4) == 5);
          assert(!rs.next());
        }
      }

      //Test connection reuse
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i desc limit 2")) {
          assert(rs.next());
          assert(rs.getLong("a_i") == 14);
          assert(rs.getLong(2) == 14);
          assert(rs.next());
          assert(rs.getLong("a_i") == 13);
          assert(rs.getLong(2) == 13);
        }

        //Test statement reuse
        stmt.setMaxRows(2);
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc")) {
          assert(rs.next());
          assert(rs.getLong("a_i") == 0);
          assert(rs.getLong(2) == 0);
          assert(rs.next());
          assert(rs.getLong("a_i") == 1);
          assert(rs.getLong(2) == 1);
          assert(!rs.next());
        }

        //Test simple loop. Since limit is set it will override the statement maxRows.
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc    LIMIT   100")) {
          int count = 0;
          while (rs.next()) {
            ++count;
          }
          assert(count == 10);
        }
      }
    }

    //Test facet aggregation
    props = new Properties();
    props.put("aggregationMode", "facet");
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s " +
            "order by sum(a_f) desc")) {

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(1).equals("hello3"));
          assert(rs.getDouble("sum(a_f)") == 26);
          assert(rs.getDouble(2) == 26);


          assert(rs.next());
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(1).equals("hello0"));
          assert(rs.getDouble("sum(a_f)") == 18);
          assert(rs.getDouble(2) == 18);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello4"));
          assert(rs.getString(1).equals("hello4"));
          assert(rs.getDouble("sum(a_f)") == 11);
          assert(rs.getDouble(2) == 11);
        }
      }
    }

    //Test map / reduce aggregation
    props = new Properties();
    props.put("aggregationMode", "map_reduce");
    props.put("numWorkers", "2");
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s " +
            "order by sum(a_f) desc")) {

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(1).equals("hello3"));
          assert(rs.getDouble("sum(a_f)") == 26);
          assert(rs.getDouble(2) == 26);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(1).equals("hello0"));
          assert(rs.getDouble("sum(a_f)") == 18);
          assert(rs.getDouble(2) == 18);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello4"));
          assert(rs.getString(1).equals("hello4"));
          assert(rs.getDouble("sum(a_f)") == 11);
          assert(rs.getDouble(2) == 11);
        }
      }
    }
    
    //Test params on the url
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + 
        "?collection=collection1&aggregationMode=map_reduce&numWorkers=2")) {

      Properties p = ((ConnectionImpl) con).getProperties();

      assert(p.getProperty("aggregationMode").equals("map_reduce"));
      assert(p.getProperty("numWorkers").equals("2"));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s " +
            "order by sum(a_f) desc")) {

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(1).equals("hello3"));
          assert(rs.getDouble("sum(a_f)") == 26);
          assert(rs.getDouble(2) == 26);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(1).equals("hello0"));
          assert(rs.getDouble("sum(a_f)") == 18);
          assert(rs.getDouble(2) == 18);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello4"));
          assert(rs.getString(1).equals("hello4"));
          assert(rs.getDouble("sum(a_f)") == 11);
          assert(rs.getDouble(2) == 11);
        }
      }
    }

    // Test JDBC paramters in URL
    try (Connection con = DriverManager.getConnection(
        "jdbc:solr://" + zkHost + "?collection=collection1&username=&password=&testKey1=testValue&testKey2")) {

      Properties p = ((ConnectionImpl) con).getProperties();
      assert(p.getProperty("username").equals(""));
      assert(p.getProperty("password").equals(""));
      assert(p.getProperty("testKey1").equals("testValue"));
      assert(p.getProperty("testKey2").equals(""));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s " +
            "order by sum(a_f) desc")) {

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(1).equals("hello3"));
          assert(rs.getDouble("sum(a_f)") == 26);
          assert(rs.getDouble(2) == 26);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(1).equals("hello0"));
          assert(rs.getDouble("sum(a_f)") == 18);
          assert(rs.getDouble(2) == 18);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello4"));
          assert(rs.getString(1).equals("hello4"));
          assert(rs.getDouble("sum(a_f)") == 11);
          assert(rs.getDouble(2) == 11);
        }
      }
    }

    // Test JDBC paramters in properties
    Properties providedProperties = new Properties();
    providedProperties.put("collection", "collection1");
    providedProperties.put("username", "");
    providedProperties.put("password", "");
    providedProperties.put("testKey1", "testValue");
    providedProperties.put("testKey2", "");

    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost, providedProperties)) {
      Properties p = ((ConnectionImpl) con).getProperties();
      assert(p.getProperty("username").equals(""));
      assert(p.getProperty("password").equals(""));
      assert(p.getProperty("testKey1").equals("testValue"));
      assert(p.getProperty("testKey2").equals(""));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s " +
            "order by sum(a_f) desc")) {

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello3"));
          assert(rs.getString(1).equals("hello3"));
          assert(rs.getDouble("sum(a_f)") == 26);
          assert(rs.getDouble(2) == 26);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello0"));
          assert(rs.getString(1).equals("hello0"));
          assert(rs.getDouble("sum(a_f)") == 18);
          assert(rs.getDouble(2) == 18);

          assert(rs.next());
          assert(rs.getString("a_s").equals("hello4"));
          assert(rs.getString(1).equals("hello4"));
          assert(rs.getDouble("sum(a_f)") == 11);
          assert(rs.getDouble(2) == 11);
        }
      }
    }

    testDriverMetadata();
  }

  private void testDriverMetadata() throws Exception {
    String collection = DEFAULT_COLLECTION;

    String connectionString1 = "jdbc:solr://" + zkServer.getZkAddress() + "?collection=" + collection +
        "&username=&password=&testKey1=testValue&testKey2";
    Properties properties1 = new Properties();

    String sql = "select id, a_i, a_s, a_f as my_float_col, testnull_i from " + collection +
        " order by a_i desc";

    String connectionString2 = "jdbc:solr://" + zkServer.getZkAddress() + "?collection=" + collection +
        "&aggregationMode=map_reduce&numWorkers=2&username=&password=&testKey1=testValue&testKey2";
    Properties properties2 = new Properties();

    String sql2 = sql + " limit 2";

    //testJDBCMethods(collection, connectionString1, properties1, sql);
    //testJDBCMethods(collection, connectionString2, properties2, sql);
    testJDBCMethods(collection, connectionString1, properties1, sql2);
    testJDBCMethods(collection, connectionString2, properties2, sql2);
  }

  private void testJDBCMethods(String collection, String connectionString, Properties properties, String sql) throws Exception {
    try (Connection con = DriverManager.getConnection(connectionString, properties)) {
      assertTrue(con.isValid(DEFAULT_CONNECTION_TIMEOUT));

      assertEquals(zkServer.getZkAddress(), con.getCatalog());
      con.setCatalog(zkServer.getZkAddress());
      assertEquals(zkServer.getZkAddress(), con.getCatalog());

      assertEquals(collection, con.getSchema());
      con.setSchema(collection);
      assertEquals(collection, con.getSchema());

      DatabaseMetaData databaseMetaData = con.getMetaData();
      assertNotNull(databaseMetaData);

      assertEquals(con, databaseMetaData.getConnection());
      assertEquals(connectionString, databaseMetaData.getURL());

      assertEquals(4, databaseMetaData.getJDBCMajorVersion());
      assertEquals(0, databaseMetaData.getJDBCMinorVersion());

      assertEquals("Apache Solr", databaseMetaData.getDatabaseProductName());

      // The following tests require package information that is not available when running via Maven
//      assertEquals(this.getClass().getPackage().getSpecificationVersion(), databaseMetaData.getDatabaseProductVersion());
//      assertEquals(0, databaseMetaData.getDatabaseMajorVersion());
//      assertEquals(0, databaseMetaData.getDatabaseMinorVersion());

//      assertEquals(this.getClass().getPackage().getSpecificationTitle(), databaseMetaData.getDriverName());
//      assertEquals(this.getClass().getPackage().getSpecificationVersion(), databaseMetaData.getDriverVersion());
//      assertEquals(0, databaseMetaData.getDriverMajorVersion());
//      assertEquals(0, databaseMetaData.getDriverMinorVersion());

      try(ResultSet rs = databaseMetaData.getCatalogs()) {
        assertTrue(rs.next());
        assertEquals(zkServer.getZkAddress(), rs.getString("TABLE_CAT"));
        assertFalse(rs.next());
      }

      List<String> collections = new ArrayList<>();
      collections.addAll(cloudClient.getZkStateReader().getClusterState().getCollections());
      Collections.sort(collections);
      try(ResultSet rs = databaseMetaData.getSchemas()) {
        for(String acollection : collections) {
          assertTrue(rs.next());
          assertEquals(acollection, rs.getString("TABLE_SCHEM"));
          assertEquals(zkServer.getZkAddress(), rs.getString("TABLE_CATALOG"));
        }
        assertFalse(rs.next());
      }

      assertNull(con.getWarnings());
      con.clearWarnings();
      assertNull(con.getWarnings());

      try (Statement statement = con.createStatement()) {
        assertEquals(con, statement.getConnection());

        assertNull(statement.getWarnings());
        statement.clearWarnings();
        assertNull(statement.getWarnings());

        try (ResultSet rs = statement.executeQuery(sql)) {
          assertEquals(statement, rs.getStatement());

          checkResultSetMetadata(rs);
          checkResultSet(rs);
        }

        assertTrue(statement.execute(sql));
        assertEquals(-1, statement.getUpdateCount());

        try (ResultSet rs = statement.getResultSet()) {
          assertEquals(statement, rs.getStatement());

          checkResultSetMetadata(rs);
          checkResultSet(rs);
        }

        assertFalse(statement.getMoreResults());
      }
    }
  }

  private void checkResultSetMetadata(ResultSet rs) throws Exception {
    ResultSetMetaData resultSetMetaData = rs.getMetaData();

    assertNotNull(resultSetMetaData);

    assertEquals(5, resultSetMetaData.getColumnCount());

    assertEquals("id", resultSetMetaData.getColumnName(1));
    assertEquals("a_i", resultSetMetaData.getColumnName(2));
    assertEquals("a_s", resultSetMetaData.getColumnName(3));
    assertEquals("a_f", resultSetMetaData.getColumnName(4));
    assertEquals("testnull_i", resultSetMetaData.getColumnName(5));

    assertEquals("id", resultSetMetaData.getColumnLabel(1));
    assertEquals("a_i", resultSetMetaData.getColumnLabel(2));
    assertEquals("a_s", resultSetMetaData.getColumnLabel(3));
    assertEquals("my_float_col", resultSetMetaData.getColumnLabel(4));
    assertEquals("testnull_i", resultSetMetaData.getColumnLabel(5));

    assertEquals("id".length(), resultSetMetaData.getColumnDisplaySize(1));
    assertEquals("a_i".length(), resultSetMetaData.getColumnDisplaySize(2));
    assertEquals("a_s".length(), resultSetMetaData.getColumnDisplaySize(3));
    assertEquals("my_float_col".length(), resultSetMetaData.getColumnDisplaySize(4));
    assertEquals("testnull_i".length(), resultSetMetaData.getColumnDisplaySize(5));

    assertEquals("Long", resultSetMetaData.getColumnTypeName(1));
    assertEquals("Long", resultSetMetaData.getColumnTypeName(2));
    assertEquals("String", resultSetMetaData.getColumnTypeName(3));
    assertEquals("Double", resultSetMetaData.getColumnTypeName(4));
    assertEquals("Long", resultSetMetaData.getColumnTypeName(5));

    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(1));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(2));
    assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(3));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(4));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(5));
  }

  private void checkResultSet(ResultSet rs) throws Exception {
    assertNull(rs.getWarnings());
    rs.clearWarnings();
    assertNull(rs.getWarnings());

    assertTrue(rs.next());

    assertEquals(14L, rs.getObject("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(14L, rs.getObject(2));
    assertFalse(rs.wasNull());
    assertEquals(14L, rs.getLong("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(14L, rs.getLong(2));
    assertFalse(rs.wasNull());
    assertEquals(14D, rs.getDouble("a_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(14D, rs.getDouble(2), 0);
    assertFalse(rs.wasNull());
    assertEquals(14f, rs.getFloat("a_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(14f, rs.getFloat(2), 0);
    assertFalse(rs.wasNull());
    assertEquals(14, rs.getShort("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(14, rs.getShort(2));
    assertFalse(rs.wasNull());
    assertEquals(14, rs.getByte("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(14, rs.getByte(2));
    assertFalse(rs.wasNull());

    assertEquals("hello0", rs.getObject("a_s"));
    assertFalse(rs.wasNull());
    assertEquals("hello0", rs.getObject(3));
    assertFalse(rs.wasNull());
    assertEquals("hello0", rs.getString("a_s"));
    assertFalse(rs.wasNull());
    assertEquals("hello0", rs.getString(3));
    assertFalse(rs.wasNull());

    assertEquals(10D, rs.getObject("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(10D, rs.getObject(4));
    assertFalse(rs.wasNull());
    assertEquals(10D, rs.getDouble("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10D, rs.getDouble(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(10F, rs.getFloat("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10F, rs.getFloat(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte(4), 0);
    assertFalse(rs.wasNull());

    assertEquals(10L, rs.getObject("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getObject(5));
    assertFalse(rs.wasNull());
    assertEquals("10", rs.getString("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals("10", rs.getString(5));
    assertFalse(rs.wasNull());
    assertEquals(10D, rs.getDouble("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10D, rs.getDouble(5), 0);
    assertFalse(rs.wasNull());
    assertEquals(10F, rs.getFloat("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10F, rs.getFloat(5), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt(5), 0);
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong(5), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort(5), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte("testnull_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte(5), 0);
    assertFalse(rs.wasNull());


    assertTrue(rs.next());

    assertEquals(13L, rs.getObject("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(13L, rs.getObject(2));
    assertFalse(rs.wasNull());
    assertEquals(13L, rs.getLong("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(13L, rs.getLong(2));
    assertFalse(rs.wasNull());
    assertEquals(13D, rs.getDouble("a_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(13D, rs.getDouble(2), 0);
    assertFalse(rs.wasNull());
    assertEquals(13f, rs.getFloat("a_i"), 0);
    assertFalse(rs.wasNull());
    assertEquals(13f, rs.getFloat(2), 0);
    assertFalse(rs.wasNull());
    assertEquals(13, rs.getShort("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(13, rs.getShort(2));
    assertFalse(rs.wasNull());
    assertEquals(13, rs.getByte("a_i"));
    assertFalse(rs.wasNull());
    assertEquals(13, rs.getByte(2));
    assertFalse(rs.wasNull());

    assertEquals("hello3", rs.getObject("a_s"));
    assertFalse(rs.wasNull());
    assertEquals("hello3", rs.getObject(3));
    assertFalse(rs.wasNull());
    assertEquals("hello3", rs.getString("a_s"));
    assertFalse(rs.wasNull());
    assertEquals("hello3", rs.getString(3));
    assertFalse(rs.wasNull());

    assertEquals(9D, rs.getObject("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(9D, rs.getObject(4));
    assertFalse(rs.wasNull());
    assertEquals(9D, rs.getDouble("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9D, rs.getDouble(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(9F, rs.getFloat("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9F, rs.getFloat(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getInt("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getInt(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(9L, rs.getLong("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9L, rs.getLong(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getShort("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getShort(4), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getByte("my_float_col"), 0);
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getByte(4), 0);
    assertFalse(rs.wasNull());

    assertEquals(null, rs.getObject("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(null, rs.getObject(5));
    assertTrue(rs.wasNull());
    assertEquals(null, rs.getString("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(null, rs.getString(5));
    assertTrue(rs.wasNull());
    assertEquals(0D, rs.getDouble("testnull_i"), 0);
    assertTrue(rs.wasNull());
    assertEquals(0D, rs.getDouble(5), 0);
    assertTrue(rs.wasNull());
    assertEquals(0F, rs.getFloat("testnull_i"), 0);
    assertTrue(rs.wasNull());
    assertEquals(0F, rs.getFloat(5), 0);
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt(5));
    assertTrue(rs.wasNull());
    assertEquals(0L, rs.getLong("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(0L, rs.getLong(5));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getShort("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getShort(5));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getByte("testnull_i"));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getByte(5));
    assertTrue(rs.wasNull());

    assertFalse(rs.next());
  }
}
