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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 * SolrStream will get fully exercised through these tests.
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class JdbcTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";

  private static final String id = "id";

  private static String zkHost;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();

    String collection;
    boolean useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }
    CollectionAdminRequest.createCollection(collection, "conf", 2, 1).process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collection, 2, 2);
    
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, cluster.getSolrClient().getZkStateReader(),
        false, true, DEFAULT_TIMEOUT);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
    }

    UpdateRequest update = new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1", "testnull_i", null)
        .add(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2", "testnull_i", "2")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "testnull_i", null)
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "testnull_i", "4")
        .add(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5", "testnull_i", null)
        .add(id, "5", "a_s", "hello3", "a_i", "10", "a_f", "6", "testnull_i", "6")
        .add(id, "6", "a_s", "hello4", "a_i", "11", "a_f", "7", "testnull_i", null)
        .add(id, "7", "a_s", "hello3", "a_i", "12", "a_f", "8", "testnull_i", "8")
        .add(id, "8", "a_s", "hello3", "a_i", "13", "a_f", "9", "testnull_i", null)
        .add(id, "9", "a_s", "hello0", "a_i", "14", "a_f", "10", "testnull_i", "10");

    SolrInputDocument withMVs = new SolrInputDocument();
    withMVs.setField(id, "10");
    withMVs.setField("s_multi", Arrays.asList("abc", "lmn", "xyz"));
    withMVs.setField("d_multi", Arrays.asList(1d, 2d, 3d));
    update.add(withMVs);

    withMVs = new SolrInputDocument();
    withMVs.setField(id, "11");
    withMVs.setField("s_multi", Arrays.asList("a", "b", "c"));
    withMVs.setField("d_multi", Arrays.asList(3d, 4d, 5d));
    update.add(withMVs);

    update.commit(cluster.getSolrClient(), collection);

    zkHost = cluster.getZkServer().getZkAddress();
  }

  @Test
  public void doTest() throws Exception {

    Properties props = new Properties();

    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS, props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from " + COLLECTIONORALIAS + " WHERE a_i IS NOT NULL order by a_i desc limit 2")) {
          assertTrue(rs.next());

          assertEquals(14, rs.getLong("a_i"));
          assertEquals(14, rs.getLong(2));
          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(3));
          assertEquals(10, rs.getDouble("a_f"), 0);
          assertEquals(10, rs.getDouble(4), 0);

          assertTrue(rs.next());

          assertEquals(13, rs.getLong("a_i"));
          assertEquals(13, rs.getLong(2));
          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(3));
          assertEquals(9, rs.getDouble("a_f"), 0);
          assertEquals(9, rs.getDouble(4), 0);

          assertFalse(rs.next());
        }

        //Test statement reuse
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from " + COLLECTIONORALIAS + " WHERE a_i IS NOT NULL order by a_i asc limit 2")) {
          assertTrue(rs.next());

          assertEquals(0, rs.getLong("a_i"));
          assertEquals(0, rs.getLong(2));
          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(3));
          assertEquals(1, rs.getDouble("a_f"), 0);
          assertEquals(1, rs.getDouble(4), 0);

          assertTrue(rs.next());

          assertEquals(1, rs.getLong("a_i"));
          assertEquals(1, rs.getLong(2));
          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(3));
          assertEquals(5, rs.getDouble("a_f"), 0);
          assertEquals(5, rs.getDouble(4), 0);

          assertFalse(rs.next());
        }
      }

      //Test connection reuse
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from " + COLLECTIONORALIAS + " WHERE a_i IS NOT NULL order by a_i desc limit 2")) {
          assertTrue(rs.next());

          assertEquals(14, rs.getLong("a_i"));
          assertEquals(14, rs.getLong(2));

          assertTrue(rs.next());

          assertEquals(13, rs.getLong("a_i"));
          assertEquals(13, rs.getLong(2));

          assertFalse(rs.next());
        }

        //Test statement reuse
        stmt.setMaxRows(2);
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from " + COLLECTIONORALIAS + " WHERE a_i IS NOT NULL order by a_i asc")) {
          assertTrue(rs.next());

          assertEquals(0, rs.getLong("a_i"));
          assertEquals(0, rs.getLong(2));

          assertTrue(rs.next());

          assertEquals(1, rs.getLong("a_i"));
          assertEquals(1, rs.getLong(2));

          assertFalse(rs.next());
        }

        //Test simple loop. Since limit is set it will override the statement maxRows.
        try (ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from " + COLLECTIONORALIAS + " WHERE a_i IS NOT NULL order by a_i asc    LIMIT   100")) {
          int count = 0;
          while (rs.next()) {
            ++count;
          }
          assertEquals(10, count);
        }
      }
    }

  }

  @Test
  public void testFacetAggregation() throws Exception {

    //Test facet aggregation
    Properties props = new Properties();
    props.put("aggregationMode", "facet");
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS, props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from " + COLLECTIONORALIAS + " group by a_s " +
            "order by sum(a_f) desc")) {

          assertTrue(rs.next());

          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(1));
          assertEquals(26, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(26, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(1));
          assertEquals(18, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(18, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello4", rs.getString("a_s"));
          assertEquals("hello4", rs.getString(1));
          assertEquals(11, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(11, rs.getDouble(2), 0);

          assertFalse(rs.next());
        }
      }
    }

  }

  @Test
  public void testMapReduceAggregation() throws Exception {

    //Test map / reduce aggregation
    Properties props = new Properties();
    props.put("aggregationMode", "map_reduce");
    props.put("numWorkers", "2");
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS, props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from " + COLLECTIONORALIAS + "  WHERE a_s IS NOT NULL group by a_s " +
            "order by sum(a_f) desc")) {

          assertTrue(rs.next());

          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(1));
          assertEquals(26, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(26, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(1));
          assertEquals(18, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(18, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello4", rs.getString("a_s"));
          assertEquals("hello4", rs.getString(1));
          assertEquals(11, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(11, rs.getDouble(2), 0);

          assertFalse(rs.next());
        }
      }
    }

  }

  @Test
  public void testConnectionParams() throws Exception {

    //Test params on the url
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost +
        "?collection=" + COLLECTIONORALIAS + "&aggregationMode=map_reduce&numWorkers=2")) {

      Properties p = ((ConnectionImpl) con).getProperties();

      assert (p.getProperty("aggregationMode").equals("map_reduce"));
      assert (p.getProperty("numWorkers").equals("2"));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from " + COLLECTIONORALIAS + "  WHERE a_s IS NOT NULL group by a_s " +
            "order by sum(a_f) desc")) {

          assertTrue(rs.next());

          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(1));
          assertEquals(26, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(26, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(1));
          assertEquals(18, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(18, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello4", rs.getString("a_s"));
          assertEquals("hello4", rs.getString(1));
          assertEquals(11, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(11, rs.getDouble(2), 0);

          assertFalse(rs.next());
        }
      }
    }

  }

  @Test
  public void testJDBCUrlParameters() throws Exception {

    // Test JDBC paramters in URL
    try (Connection con = DriverManager.getConnection(
        "jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS + "&username=&password=&testKey1=testValue&testKey2")) {

      Properties p = ((ConnectionImpl) con).getProperties();
      assertEquals("", p.getProperty("username"));
      assertEquals("", p.getProperty("password"));
      assertEquals("testValue", p.getProperty("testKey1"));
      assertEquals("", p.getProperty("testKey2"));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from " + COLLECTIONORALIAS + " group by a_s " +
            "order by sum(a_f) desc")) {

          assertTrue(rs.next());

          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(1));
          assertEquals(26, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(26, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(1));
          assertEquals(18, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(18, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello4", rs.getString("a_s"));
          assertEquals("hello4", rs.getString(1));
          assertEquals(11, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(11, rs.getDouble(2), 0);

          assertFalse(rs.next());
        }
      }
    }

  }

  @Test
  public void testJDBCPropertiesParameters() throws Exception {

    // Test JDBC paramters in properties
    Properties providedProperties = new Properties();
    providedProperties.put("collection", COLLECTIONORALIAS);
    providedProperties.put("username", "");
    providedProperties.put("password", "");
    providedProperties.put("testKey1", "testValue");
    providedProperties.put("testKey2", "");

    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost, providedProperties)) {
      Properties p = ((ConnectionImpl) con).getProperties();
      assert (p.getProperty("username").equals(""));
      assert (p.getProperty("password").equals(""));
      assert (p.getProperty("testKey1").equals("testValue"));
      assert (p.getProperty("testKey2").equals(""));

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s, sum(a_f) from " + COLLECTIONORALIAS + " group by a_s " +
            "order by sum(a_f) desc")) {

          assertTrue(rs.next());

          assertEquals("hello3", rs.getString("a_s"));
          assertEquals("hello3", rs.getString(1));
          assertEquals(26, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(26, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello0", rs.getString("a_s"));
          assertEquals("hello0", rs.getString(1));
          assertEquals(18, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(18, rs.getDouble(2), 0);

          assertTrue(rs.next());

          assertEquals("hello4", rs.getString("a_s"));
          assertEquals("hello4", rs.getString(1));
          assertEquals(11, rs.getDouble("EXPR$1"), 0); //sum(a_f)
          assertEquals(11, rs.getDouble(2), 0);

          assertFalse(rs.next());
        }
      }
    }
  }

  @Ignore("Fix error checking")
  @Test
  @SuppressWarnings({"try"})
  public void testErrorPropagation() throws Exception {
    //Test error propagation
    Properties props = new Properties();
    props.put("aggregationMode", "facet");
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS, props)) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select crap from " + COLLECTIONORALIAS + " group by a_s " +
            "order by sum(a_f) desc")) {
        } catch (Exception e) {
          String errorMessage = e.getMessage();
          assertTrue(errorMessage.contains("Group by queries must include at least one aggregate function"));
        }
      }
    }
  }

  @Test
  @SuppressWarnings({"try"})
  public void testSQLExceptionThrownWhenQueryAndConnUseDiffCollections() throws Exception  {
    String badCollection = COLLECTIONORALIAS + "bad";
    String connectionString = "jdbc:solr://" + zkHost + "?collection=" + badCollection;
    String sql = "select id, a_i, a_s, a_f from " + badCollection + " order by a_i desc limit 2";

    //Bad connection string: wrong collection name
    try(Connection connection = DriverManager.getConnection(connectionString)) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet ignored = statement.executeQuery(sql)) {
          fail("Expected query against wrong collection to throw a SQLException.");
        }
      }
    } catch (SQLException ignore) {
      // Expected exception due to miss matched collection
    }
  }

  @Test
  public void testOneEqualZeroMetadata() throws Exception {
    // SOLR-8845 - Make sure that 1 = 1 (literal comparison literal) works
    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost +
        "?collection=" + COLLECTIONORALIAS)) {

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("select a_s from " + COLLECTIONORALIAS + " where 1 = 0")) {
          assertFalse(rs.next());

          ResultSetMetaData resultSetMetaData = rs.getMetaData();
          assertNotNull(resultSetMetaData);
          assertEquals(1, resultSetMetaData.getColumnCount());
          assertEquals("a_s", resultSetMetaData.getColumnName(1));
        }
      }
    }
  }

  @Test
  public void testDriverMetadata() throws Exception {
    String collection = COLLECTIONORALIAS;

    String connectionString1 = "jdbc:solr://" + zkHost + "?collection=" + collection +
        "&username=&password=&testKey1=testValue&testKey2";
    Properties properties1 = new Properties();

    String sql = "select id, a_i, a_s, a_f as my_float_col, testnull_i from " + collection +
        " order by a_i desc";

    String connectionString2 = "jdbc:solr://" + zkHost + "?collection=" + collection +
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
      assertTrue("connection should be valid when checked with timeout = 0 -> con.isValid(0)", con.isValid(0));


      assertEquals(zkHost, con.getCatalog());
      con.setCatalog(zkHost);
      assertEquals(zkHost, con.getCatalog());

      assertEquals(null, con.getSchema());
      con.setSchema("myschema");
      assertEquals(null, con.getSchema());

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

      List<String> tableSchemas = new ArrayList<>(Arrays.asList(zkHost, "metadata"));
      try(ResultSet rs = databaseMetaData.getSchemas()) {
        assertTrue(rs.next());
        assertTrue(tableSchemas.contains(rs.getString("tableSchem")));
        tableSchemas.remove(rs.getString("tableSchem"));
        assertNull(rs.getString("tableCat"));
        assertTrue(rs.next());
        assertTrue(tableSchemas.contains(rs.getString("tableSchem")));
        tableSchemas.remove(rs.getString("tableSchem"));
        assertNull(rs.getString("tableCat"));
        assertFalse(rs.next());
        assertTrue(tableSchemas.isEmpty());
      }

      try(ResultSet rs = databaseMetaData.getCatalogs()) {
        assertTrue(rs.next());
        assertNull(rs.getString("tableCat"));
        assertFalse(rs.next());
      }

      CloudSolrClient solrClient = cluster.getSolrClient();
      solrClient.connect();
      ZkStateReader zkStateReader = solrClient.getZkStateReader();

      Set<String> collectionsSet = zkStateReader.getClusterState().getCollectionsMap().keySet();
      SortedSet<String> tables = new TreeSet<>(collectionsSet);

      Aliases aliases = zkStateReader.getAliases();
      tables.addAll(aliases.getCollectionAliasListMap().keySet());

      try(ResultSet rs = databaseMetaData.getTables(null, zkHost, "%", null)) {
        for(String table : tables) {
          assertTrue(rs.next());
          assertNull(rs.getString("tableCat"));
          assertEquals(zkHost, rs.getString("tableSchem"));
          assertEquals(table, rs.getString("tableName"));
          assertEquals("TABLE", rs.getString("tableType"));
          assertNull(rs.getString("remarks"));
        }
        assertFalse(rs.next());
      }

      assertEquals(Connection.TRANSACTION_NONE, con.getTransactionIsolation());
      try {
        con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        fail("should not have been able to set transaction isolation");
      } catch (SQLException e) {
        assertEquals(UnsupportedOperationException.class, e.getCause().getClass());
      }
      assertEquals(Connection.TRANSACTION_NONE, con.getTransactionIsolation());

      assertTrue(con.isReadOnly());
      con.setReadOnly(true);
      assertTrue(con.isReadOnly());

      assertNull(con.getWarnings());
      con.clearWarnings();
      assertNull(con.getWarnings());

      try (Statement statement = con.createStatement()) {
        checkStatement(con, statement);

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

      try (PreparedStatement statement = con.prepareStatement(sql)) {
        checkStatement(con, statement);

        try (ResultSet rs = statement.executeQuery()) {
          assertEquals(statement, rs.getStatement());

          checkResultSetMetadata(rs);
          checkResultSet(rs);
        }

        assertTrue(statement.execute());
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

  private void checkStatement(Connection con, Statement statement) throws Exception {
    assertEquals(con, statement.getConnection());

    assertNull(statement.getWarnings());
    statement.clearWarnings();
    assertNull(statement.getWarnings());

    assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
    assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());

    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    statement.setFetchDirection(ResultSet.FETCH_FORWARD);
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

    assertEquals(0, statement.getFetchSize());
    statement.setFetchSize(0);
    assertEquals(0, statement.getFetchSize());
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

    assertEquals("String", resultSetMetaData.getColumnTypeName(1));
    assertEquals("Long", resultSetMetaData.getColumnTypeName(2));
    assertEquals("String", resultSetMetaData.getColumnTypeName(3));
    assertEquals("Double", resultSetMetaData.getColumnTypeName(4));
    assertEquals("Long", resultSetMetaData.getColumnTypeName(5));

    assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(2));
    assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(3));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(4));
    assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(5));
  }

  private void checkResultSet(ResultSet rs) throws Exception {
    assertNull(rs.getWarnings());
    rs.clearWarnings();
    assertNull(rs.getWarnings());

    assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

    assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

    assertEquals(0, rs.getFetchSize());
    rs.setFetchSize(10);
    assertEquals(0, rs.getFetchSize());

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
    assertEquals(10, rs.getInt("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt(4));
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong(4));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort(4));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte(4));
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
    assertEquals(10, rs.getInt("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getInt(5));
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals(10L, rs.getLong(5));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getShort(5));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte("testnull_i"));
    assertFalse(rs.wasNull());
    assertEquals(10, rs.getByte(5));
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
    assertEquals(9, rs.getInt("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getInt(4));
    assertFalse(rs.wasNull());
    assertEquals(9L, rs.getLong("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(9L, rs.getLong(4));
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getShort("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getShort(4));
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getByte("my_float_col"));
    assertFalse(rs.wasNull());
    assertEquals(9, rs.getByte(4));
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

  @Test
  public void doTestMultiValued() throws Exception {

    Properties props = new Properties();

    try (Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=" + COLLECTIONORALIAS, props)) {
      try (Statement stmt = con.createStatement()) {
        // Multi-valued field projection
        try (ResultSet rs = stmt.executeQuery("select id, s_multi, d_multi from " + COLLECTIONORALIAS + " WHERE s_multi IS NOT NULL order by id desc limit 2")) {
          assertTrue(rs.next());

          assertEquals(Arrays.asList("a", "b", "c"), rs.getObject("s_multi"));
          assertEquals(Arrays.asList(3d, 4d, 5d), rs.getObject("d_multi"));

          assertTrue(rs.next());

          assertEquals(Arrays.asList("abc", "lmn", "xyz"), rs.getObject("s_multi"));
          assertEquals(Arrays.asList(1d, 2d, 3d), rs.getObject("d_multi"));

          assertFalse(rs.next());
        }

        // Filtering with multi-valued fields
        try (ResultSet rs = stmt.executeQuery("select id, s_multi, d_multi from " + COLLECTIONORALIAS + " WHERE s_multi IN ('a', 'abc') AND d_multi >= 1 order by id desc limit 2")) {
          assertTrue(rs.next());

          assertEquals(Arrays.asList("a", "b", "c"), rs.getObject("s_multi"));
          assertEquals(Arrays.asList(3d, 4d, 5d), rs.getObject("d_multi"));

          assertTrue(rs.next());

          assertEquals(Arrays.asList("abc", "lmn", "xyz"), rs.getObject("s_multi"));
          assertEquals(Arrays.asList(1d, 2d, 3d), rs.getObject("d_multi"));

          assertFalse(rs.next());
        }

        // group by multi-valued
        try (ResultSet rs = stmt.executeQuery("select count(*) as the_count, d_multi from " + COLLECTIONORALIAS + " WHERE d_multi IS NOT NULL GROUP BY d_multi ORDER BY count(*) DESC")) {
          assertTrue(rs.next());
          assertEquals(2, rs.getLong("the_count"));
          assertTrue(3d == rs.getDouble("d_multi"));
          assertTrue(rs.next()); // 4 other values, all with count 1
          assertTrue(rs.next());
          assertTrue(rs.next());
          assertTrue(rs.next());
          assertFalse(rs.next());
        }
      }
    }
  }
}
