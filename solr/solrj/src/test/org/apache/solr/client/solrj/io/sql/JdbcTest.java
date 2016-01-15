package org.apache.solr.client.solrj.io.sql;

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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

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

    String zkHost = zkServer.getZkAddress();

    Properties props = new Properties();
    Connection con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props);
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i desc limit 2");
    assertTrue(rs.getMetaData() != null);

    assert(rs.next());
    assert(rs.getLong("a_i") == 14);
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("a_f") == 10);

    assert(rs.next());
    assert(rs.getLong("a_i") == 13);
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("a_f") == 9);
    assert(!rs.next());
    stmt.close();

    //Test statement reuse
    rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc limit 2");
    assert(rs.next());
    assert(rs.getLong("a_i") == 0);
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("a_f") == 1);

    assert(rs.next());
    assert(rs.getLong("a_i") == 1);
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("a_f") == 5);
    assert(!rs.next());
    stmt.close();

    //Test connection reuse
    stmt = con.createStatement();
    rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i desc limit 2");
    assert(rs.next());
    assert(rs.getLong("a_i") == 14);
    assert(rs.next());
    assert(rs.getLong("a_i") == 13);
    stmt.close();

    //Test statement reuse
    rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc limit 2");
    assert(rs.next());
    assert(rs.getLong("a_i") == 0);
    assert(rs.next());
    assert(rs.getLong("a_i") == 1);
    assert(!rs.next());
    stmt.close();

    //Test simple loop
    rs = stmt.executeQuery("select id, a_i, a_s, a_f from collection1 order by a_i asc limit 100");
    int count = 0;
    while(rs.next()) {
      ++count;
    }

    assert(count == 10);

    stmt.close();
    con.close();

    //Test facet aggregation
    props = new Properties();
    props.put("aggregationMode", "facet");
    con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props);
    stmt = con.createStatement();
    rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s order by sum(a_f) desc");

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("sum(a_f)") == 26);


    assert(rs.next());
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("sum(a_f)") == 18);


    assert(rs.next());
    assert(rs.getString("a_s").equals("hello4"));
    assert(rs.getDouble("sum(a_f)") == 11);

    stmt.close();
    con.close();

    //Test map / reduce aggregation
    props = new Properties();
    props.put("aggregationMode", "map_reduce");
    props.put("numWorkers", "2");
    con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1", props);
    stmt = con.createStatement();
    rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s order by sum(a_f) desc");

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("sum(a_f)") == 26);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("sum(a_f)") == 18);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello4"));
    assert(rs.getDouble("sum(a_f)") == 11);

    stmt.close();
    con.close();

    //Test params on the url
    con = DriverManager.getConnection("jdbc:solr://" + zkHost + "?collection=collection1&aggregationMode=map_reduce&numWorkers=2");

    Properties p = ((ConnectionImpl) con).props;

    assert(p.getProperty("aggregationMode").equals("map_reduce"));
    assert(p.getProperty("numWorkers").equals("2"));

    stmt = con.createStatement();
    rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s order by sum(a_f) desc");

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("sum(a_f)") == 26);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("sum(a_f)") == 18);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello4"));
    assert(rs.getDouble("sum(a_f)") == 11);

    stmt.close();
    con.close();

    // Test JDBC paramters in URL
    con = DriverManager.getConnection(
        "jdbc:solr://" + zkHost + "?collection=collection1&username=&password=&testKey1=testValue&testKey2");

    p = ((ConnectionImpl) con).props;
    assert(p.getProperty("username").equals(""));
    assert(p.getProperty("password").equals(""));
    assert(p.getProperty("testKey1").equals("testValue"));
    assert(p.getProperty("testKey2").equals(""));

    stmt = con.createStatement();
    rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s order by sum(a_f) desc");

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("sum(a_f)") == 26);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("sum(a_f)") == 18);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello4"));
    assert(rs.getDouble("sum(a_f)") == 11);

    stmt.close();
    con.close();

    // Test JDBC paramters in properties
    Properties providedProperties = new Properties();
    providedProperties.put("collection", "collection1");
    providedProperties.put("username", "");
    providedProperties.put("password", "");
    providedProperties.put("testKey1", "testValue");
    providedProperties.put("testKey2", "");

    con = DriverManager.getConnection("jdbc:solr://" + zkHost, providedProperties);

    p = ((ConnectionImpl) con).props;
    assert(p.getProperty("username").equals(""));
    assert(p.getProperty("password").equals(""));
    assert(p.getProperty("testKey1").equals("testValue"));
    assert(p.getProperty("testKey2").equals(""));

    stmt = con.createStatement();
    rs = stmt.executeQuery("select a_s, sum(a_f) from collection1 group by a_s order by sum(a_f) desc");

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello3"));
    assert(rs.getDouble("sum(a_f)") == 26);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello0"));
    assert(rs.getDouble("sum(a_f)") == 18);

    assert(rs.next());
    assert(rs.getString("a_s").equals("hello4"));
    assert(rs.getDouble("sum(a_f)") == 11);

    stmt.close();
    con.close();

    testDriverMetadata();
  }

  private void testDriverMetadata() throws Exception {
    String collection = DEFAULT_COLLECTION;
    String connectionString = "jdbc:solr://" + zkServer.getZkAddress() + "?collection=" + collection +
        "&username=&password=&testKey1=testValue&testKey2";

    try (Connection con = DriverManager.getConnection(connectionString)) {
      assertEquals(collection, con.getCatalog());

      DatabaseMetaData databaseMetaData = con.getMetaData();
      assertNotNull(databaseMetaData);

      assertEquals(connectionString, databaseMetaData.getURL());
    }
  }
}
