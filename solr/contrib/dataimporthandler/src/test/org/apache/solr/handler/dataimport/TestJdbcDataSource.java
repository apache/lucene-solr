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
package org.apache.solr.handler.dataimport;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.lucene.util.Constants;
import org.apache.solr.handler.dataimport.JdbcDataSource.ResultSetIterator;
import static org.mockito.Mockito.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * <p>
 * Test for JdbcDataSource
 * </p>
 * <p>
 * Note: The tests are ignored for the lack of DB support for testing
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestJdbcDataSource extends AbstractDataImportHandlerTestCase {
  private Driver driver;
  private DataSource dataSource;
  private Connection connection;
  private JdbcDataSource jdbcDataSource = new JdbcDataSource();
  List<Map<String, String>> fields = new ArrayList<>();

  Context context = AbstractDataImportHandlerTestCase.getContext(null, null,
          jdbcDataSource, Context.FULL_DUMP, fields, null);

  Properties props = new Properties();

  String sysProp = System.getProperty("java.naming.factory.initial");

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("java.naming.factory.initial",
            MockInitialContextFactory.class.getName());
    
    driver = mock(Driver.class);
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    props.clear();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (sysProp == null) {
      System.getProperties().remove("java.naming.factory.initial");
    } else {
      System.setProperty("java.naming.factory.initial", sysProp);
    }
    super.tearDown();
    reset(driver, dataSource, connection);
  }

  @Test
  public void testRetrieveFromJndi() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    when(dataSource.getConnection()).thenReturn(connection);

    Connection conn = jdbcDataSource.createConnectionFactory(context, props)
            .call();

    verify(connection).setAutoCommit(false);
    verify(dataSource).getConnection();

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiWithCredentials() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    props.put("user", "Fred");
    props.put("password", "4r3d");
    props.put("holdability", "HOLD_CURSORS_OVER_COMMIT");

    when(dataSource.getConnection("Fred", "4r3d")).thenReturn(
            connection);

    Connection conn = jdbcDataSource.createConnectionFactory(context, props)
            .call();

    verify(connection).setAutoCommit(false);
    verify(connection).setHoldability(1);
    verify(dataSource).getConnection("Fred", "4r3d");

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiWithCredentialsEncryptedAndResolved() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    String user = "Fred";
    String plainPassword = "MyPassword";
    String encryptedPassword = "U2FsdGVkX18QMjY0yfCqlfBMvAB4d3XkwY96L7gfO2o=";
    String propsNamespace = "exampleNamespace";

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    props.put("user", "${" +propsNamespace +".user}");
    props.put("encryptKeyFile", "${" +propsNamespace +".encryptKeyFile}");
    props.put("password", "${" +propsNamespace +".password}");

    when(dataSource.getConnection(user, plainPassword)).thenReturn(
             connection);

    Map<String,Object> values = new HashMap<>();
    values.put("user", user);
    values.put("encryptKeyFile", createEncryptionKeyFile());
    values.put("password", encryptedPassword);
    context.getVariableResolver().addNamespace(propsNamespace, values);

    jdbcDataSource.init(context, props);
    Connection conn = jdbcDataSource.getConnection();

    verify(connection).setAutoCommit(false);
    verify(dataSource).getConnection(user, plainPassword);

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiWithCredentialsWithEncryptedAndResolvedPwd() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    Properties properties = new Properties();
    properties.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    properties.put("user", "Fred");
    properties.put("encryptKeyFile", "${foo.bar}");
    properties.put("password", "U2FsdGVkX18QMjY0yfCqlfBMvAB4d3XkwY96L7gfO2o=");
    when(dataSource.getConnection("Fred", "MyPassword")).thenReturn(
        connection);

    Map<String,Object> values = new HashMap<>();
    values.put("bar", createEncryptionKeyFile());
    context.getVariableResolver().addNamespace("foo", values);

    jdbcDataSource.init(context, properties);
    jdbcDataSource.getConnection();

    verify(connection).setAutoCommit(false);
    verify(dataSource).getConnection("Fred", "MyPassword");
  }

  @Test
  public void testRetrieveFromJndiFailureNotHidden() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    SQLException sqlException = new SQLException("fake");
    when(dataSource.getConnection()).thenThrow(sqlException);

    try {
      jdbcDataSource.createConnectionFactory(context, props).call();
    } catch (SQLException ex) {
      assertSame(sqlException, ex);
    }

    verify(dataSource).getConnection();
  }

  @Test
  public void testClosesConnectionWhenExceptionThrownOnSetAutocommit() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    SQLException sqlException = new SQLException("fake");
    when(dataSource.getConnection()).thenReturn(connection);
    doThrow(sqlException).when(connection).setAutoCommit(false);

    try {
      jdbcDataSource.createConnectionFactory(context, props).call();
    } catch (DataImportHandlerException ex) {
      assertSame(sqlException, ex.getCause());
    }
    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection).close();
  }

  @Test
  public void testClosesStatementWhenExceptionThrownOnExecuteQuery() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDataSource.init(context, props);

    SQLException sqlException = new SQLException("fake");
    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenThrow(sqlException);

    try {
      jdbcDataSource.getData("query");
      fail("exception expected");
    } catch (DataImportHandlerException ex) {
      assertSame(sqlException, ex.getCause());
    }

    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement).close();
  }

  @Test
  public void testClosesStatementWhenResultSetNull() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDataSource.init(context, props);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(-1);

    jdbcDataSource.getData("query");

    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement).getUpdateCount();
    verify(statement).close();
  }

  @Test
  public void testClosesStatementWhenHasNextCalledAndResultSetNull() throws Exception {

    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDataSource.init(context, props);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(0);

    Iterator<Map<String,Object>> data = jdbcDataSource.getData("query");

    ResultSetIterator resultSetIterator = (ResultSetIterator) data.getClass().getDeclaredField("this$1").get(data);
    resultSetIterator.setResultSet(null);

    data.hasNext();

    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement).getResultSet();
    verify(statement).close();
    verify(resultSet).getMetaData();
    verify(metaData).getColumnCount();
  }

  @Test
  public void testClosesResultSetAndStatementWhenDataSourceIsClosed() throws Exception {

    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDataSource.init(context, props);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(0);

    jdbcDataSource.getData("query");
    jdbcDataSource.close();

    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement).getResultSet();
    verify(resultSet).getMetaData();
    verify(metaData).getColumnCount();
    verify(resultSet).close();
    verify(statement).close();
    verify(connection).commit();
    verify(connection).close();
  }

  @Test
  public void testClosesCurrentResultSetIteratorWhenNewOneIsCreated() throws Exception {

    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);

    jdbcDataSource.init(context, props);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(0);
    when(statement.execute("other query")).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(-1);

    jdbcDataSource.getData("query");
    jdbcDataSource.getData("other query");

    verify(dataSource).getConnection();
    verify(connection).setAutoCommit(false);
    verify(connection, times(2)).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement, times(2)).setFetchSize(500);
    verify(statement, times(2)).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement).getResultSet();
    verify(resultSet).getMetaData();
    verify(metaData).getColumnCount();
    verify(resultSet).close();
    verify(statement, times(2)).close();
    verify(statement).execute("other query");
  }
  
  @Test
  public void testMultipleResultsSets_UpdateCountUpdateCountResultSet() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);
    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);
    jdbcDataSource.init(context, props);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(1);
    when(statement.getMoreResults()).thenReturn(false).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(0);

    final ResultSetIterator resultSetIterator = jdbcDataSource.new ResultSetIterator("query");
    assertSame(resultSet, resultSetIterator.getResultSet());

    verify(connection).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement, times(2)).getUpdateCount();
    verify(statement, times(2)).getMoreResults();
  }

  @Test
  public void testMultipleResultsSets_ResultSetResultSet() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);
    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    when(dataSource.getConnection()).thenReturn(connection);
    jdbcDataSource.init(context, props);
    connection.setAutoCommit(false);

    Statement statement = mock(Statement.class);
    when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        .thenReturn(statement);
    when(statement.execute("query")).thenReturn(true);
    ResultSet resultSet1 = mock(ResultSet.class);
    ResultSet resultSet2 = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet1).thenReturn(resultSet2).thenReturn(null);
    when(statement.getMoreResults()).thenReturn(true).thenReturn(false);
    ResultSetMetaData metaData1 = mock(ResultSetMetaData.class);
    when(resultSet1.getMetaData()).thenReturn(metaData1);
    when(metaData1.getColumnCount()).thenReturn(0);
    when(resultSet1.next()).thenReturn(false);
    ResultSetMetaData metaData2 = mock(ResultSetMetaData.class);
    when(resultSet2.getMetaData()).thenReturn(metaData2);
    when(metaData2.getColumnCount()).thenReturn(0);
    when(resultSet2.next()).thenReturn(true).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(-1);

    final ResultSetIterator resultSetIterator = jdbcDataSource.new ResultSetIterator("query");
    assertSame(resultSet1, resultSetIterator.getResultSet());
    assertTrue(resultSetIterator.hasnext());
    assertSame(resultSet2, resultSetIterator.getResultSet());
    assertFalse(resultSetIterator.hasnext());

    verify(dataSource).getConnection();
    verify(connection, times(2)).setAutoCommit(false);
    verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    verify(statement).setFetchSize(500);
    verify(statement).setMaxRows(0);
    verify(statement).execute("query");
    verify(statement, times(2)).getResultSet();
    verify(resultSet1).getMetaData();
    verify(metaData1).getColumnCount();
    verify(resultSet1).next();
    verify(resultSet1).close();
    verify(resultSet2).getMetaData();
    verify(metaData2).getColumnCount();
    verify(resultSet2, times(2)).next();
    verify(resultSet2).close();
    verify(statement, times(2)).getMoreResults();
    verify(statement).getUpdateCount();
    verify(statement).close();
  }

  @Test
  public void testRetrieveFromDriverManager() throws Exception {
    // in JDK9, Class.forName will throw exception for mock classes
    if (Constants.JRE_IS_MINIMUM_JAVA9) return;
    DriverManager.registerDriver(driver);
    try {
      when(driver.connect(notNull(),notNull())).thenReturn(connection);

      props.put(JdbcDataSource.DRIVER, driver.getClass().getName());
      props.put(JdbcDataSource.URL, "jdbc:fakedb");
      props.put("holdability", "HOLD_CURSORS_OVER_COMMIT");

      Connection conn = jdbcDataSource.createConnectionFactory(context, props)
              .call();

      verify(connection).setAutoCommit(false);
      verify(connection).setHoldability(1);

      assertSame("connection", conn, connection);
    } catch(Exception e) {
      throw e;
    } finally {
      DriverManager.deregisterDriver(driver);
    }
  }


  @Test
  public void testEmptyResultSet() throws Exception {
      MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

      props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
      when(dataSource.getConnection()).thenReturn(connection);

      jdbcDataSource.init(context, props);

      Statement statement = mock(Statement.class);
      when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
          .thenReturn(statement);
      when(statement.execute("query")).thenReturn(true);
      ResultSet resultSet = mock(ResultSet.class);
      when(statement.getResultSet()).thenReturn(resultSet);
      ResultSetMetaData metaData = mock(ResultSetMetaData.class);
      when(resultSet.getMetaData()).thenReturn(metaData);
      when(metaData.getColumnCount()).thenReturn(0);
      when(resultSet.next()).thenReturn(false);
      when(statement.getMoreResults()).thenReturn(false);
      when(statement.getUpdateCount()).thenReturn(-1);

      Iterator<Map<String,Object>> resultSetIterator = jdbcDataSource.getData("query");
      resultSetIterator.hasNext();
      resultSetIterator.hasNext();

      verify(connection).setAutoCommit(false);
      verify(connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      verify(statement).setFetchSize(500);
      verify(statement).setMaxRows(0);
      verify(statement).execute("query");
      verify(statement).getResultSet();
      verify(resultSet).getMetaData();
      verify(metaData).getColumnCount();
      verify(resultSet).next();
      verify(resultSet).close();
      verify(statement).getMoreResults();
      verify(statement).getUpdateCount();
      verify(statement).close();
  }

  @Test
  @Ignore("Needs a Mock database server to work")
  public void testBasic() throws Exception {
    JdbcDataSource dataSource = new JdbcDataSource();
    Properties p = new Properties();
    p.put("driver", "com.mysql.jdbc.Driver");
    p.put("url", "jdbc:mysql://127.0.0.1/autos");
    p.put("user", "root");
    p.put("password", "");

    List<Map<String, String>> flds = new ArrayList<>();
    Map<String, String> f = new HashMap<>();
    f.put("column", "trim_id");
    f.put("type", "long");
    flds.add(f);
    f = new HashMap<>();
    f.put("column", "msrp");
    f.put("type", "float");
    flds.add(f);

    Context c = getContext(null, null,
            dataSource, Context.FULL_DUMP, flds, null);
    dataSource.init(c, p);
    Iterator<Map<String, Object>> i = dataSource
            .getData("select make,model,year,msrp,trim_id from atrimlisting where make='Acura'");
    int count = 0;
    Object msrp = null;
    Object trim_id = null;
    while (i.hasNext()) {
      Map<String, Object> map = i.next();
      msrp = map.get("msrp");
      trim_id = map.get("trim_id");
      count++;
    }
    assertEquals(5, count);
    assertEquals(Float.class, msrp.getClass());
    assertEquals(Long.class, trim_id.getClass());
  }
  
  private String createEncryptionKeyFile() throws IOException {
    File tmpdir = createTempDir().toFile();
    byte[] content = "secret".getBytes(StandardCharsets.UTF_8);
    createFile(tmpdir, "enckeyfile.txt", content, false);
    return new File(tmpdir, "enckeyfile.txt").getAbsolutePath();
  }  
}
