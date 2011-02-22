/**
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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
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
 * <p/>
 * <p>
 * Note: The tests are ignored for the lack of DB support for testing
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestJdbcDataSource extends AbstractDataImportHandlerTestCase {
  Driver driver;
  DataSource dataSource;
  Connection connection;
  IMocksControl mockControl;
  JdbcDataSource jdbcDataSource = new JdbcDataSource();
  List<Map<String, String>> fields = new ArrayList<Map<String, String>>();

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
  public void testRetrieveFromDriverManager() throws Exception {
    DriverManager.registerDriver(driver);

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
  }

  @Test
  @Ignore("Needs a Mock database server to work")
  public void testBasic() throws Exception {
    JdbcDataSource dataSource = new JdbcDataSource();
    Properties p = new Properties();
    p.put("driver", "com.mysql.jdbc.Driver");
    p.put("url", "jdbc:mysql://localhost/autos");
    p.put("user", "root");
    p.put("password", "");

    List<Map<String, String>> flds = new ArrayList<Map<String, String>>();
    Map<String, String> f = new HashMap<String, String>();
    f.put("column", "trim_id");
    f.put("type", "long");
    flds.add(f);
    f = new HashMap<String, String>();
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
