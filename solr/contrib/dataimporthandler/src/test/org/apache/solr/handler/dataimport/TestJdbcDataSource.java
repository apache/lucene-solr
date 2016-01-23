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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
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
  private IMocksControl mockControl;
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
    
    mockControl = EasyMock.createStrictControl();
    driver = mockControl.createMock(Driver.class);
    dataSource = mockControl.createMock(DataSource.class);
    connection = mockControl.createMock(Connection.class);
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
    mockControl.reset();
  }

  @Test
  public void testRetrieveFromJndi() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    EasyMock.expect(dataSource.getConnection()).andReturn(connection);
    connection.setAutoCommit(false);
//    connection.setHoldability(1);

    mockControl.replay();

    Connection conn = jdbcDataSource.createConnectionFactory(context, props)
            .call();

    mockControl.verify();

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiWithCredentials() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    props.put("user", "Fred");
    props.put("password", "4r3d");
    props.put("holdability", "HOLD_CURSORS_OVER_COMMIT");

    EasyMock.expect(dataSource.getConnection("Fred", "4r3d")).andReturn(
            connection);
    connection.setAutoCommit(false);
    connection.setHoldability(1);

    mockControl.replay();

    Connection conn = jdbcDataSource.createConnectionFactory(context, props)
            .call();

    mockControl.verify();

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiWithCredentialsWithEncryptedPwd() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);
    File tmpdir = File.createTempFile("test", "tmp", createTempDir().toFile());
    Files.delete(tmpdir.toPath());
    tmpdir.mkdir();
    byte[] content = "secret".getBytes(StandardCharsets.UTF_8);
    createFile(tmpdir, "enckeyfile.txt", content, false);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");
    props.put("user", "Fred");
    props.put("encryptKeyFile", new File(tmpdir, "enckeyfile.txt").getAbsolutePath());
    props.put("password", "U2FsdGVkX18QMjY0yfCqlfBMvAB4d3XkwY96L7gfO2o=");
    props.put("holdability", "HOLD_CURSORS_OVER_COMMIT");
    EasyMock.expect(dataSource.getConnection("Fred", "MyPassword")).andReturn(
        connection);
    jdbcDataSource.init(context, props);

    connection.setAutoCommit(false);
    connection.setHoldability(1);

    mockControl.replay();

    Connection conn = jdbcDataSource.getConnection();

    mockControl.verify();

    assertSame("connection", conn, connection);
  }

  @Test
  public void testRetrieveFromJndiFailureNotHidden() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    SQLException sqlException = new SQLException("fake");
    EasyMock.expect(dataSource.getConnection()).andThrow(sqlException);

    mockControl.replay();
    
    try {
      jdbcDataSource.createConnectionFactory(context, props).call();
    } catch (SQLException ex) {
      assertSame(sqlException, ex);
    }
    
    mockControl.verify();
  }
  
  @Test
  public void testClosesConnectionWhenExceptionThrownOnSetAutocommit() throws Exception {
    MockInitialContextFactory.bind("java:comp/env/jdbc/JndiDB", dataSource);

    props.put(JdbcDataSource.JNDI_NAME, "java:comp/env/jdbc/JndiDB");

    SQLException sqlException = new SQLException("fake");
    EasyMock.expect(dataSource.getConnection()).andReturn(connection);
    connection.setAutoCommit(false);
    EasyMock.expectLastCall().andThrow(sqlException);
    connection.close();
    mockControl.replay();
    
    try {
      jdbcDataSource.createConnectionFactory(context, props).call();
    } catch (DataImportHandlerException ex) {
      assertSame(sqlException, ex.getCause());
    }
    
    mockControl.verify();
  }
  
  @Test
  public void testRetrieveFromDriverManager() throws Exception {
    DriverManager.registerDriver(driver);
    try {
    EasyMock.expect(
            driver.connect((String) EasyMock.notNull(), (Properties) EasyMock
                    .notNull())).andReturn(connection);
    connection.setAutoCommit(false);
    connection.setHoldability(1);

    props.put(JdbcDataSource.DRIVER, driver.getClass().getName());
    props.put(JdbcDataSource.URL, "jdbc:fakedb");
    props.put("holdability", "HOLD_CURSORS_OVER_COMMIT");
    mockControl.replay();

    Connection conn = jdbcDataSource.createConnectionFactory(context, props)
            .call();

    mockControl.verify();

    assertSame("connection", conn, connection);
    } catch(Exception e) {
      throw e;
    } finally {
      DriverManager.deregisterDriver(driver);
    }
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
}
