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

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This sets up an in-memory Sql database with a little sample data.
 */
public abstract class AbstractDIHJdbcTestCase extends
    AbstractDataImportHandlerTestCase {
  
  protected Database dbToUse;
  
  public enum Database {
    RANDOM, DERBY, HSQLDB
  }
  
  protected boolean skipThisTest = false;
  
  private static final Pattern totalRequestsPattern = Pattern
      .compile(".str name..Total Requests made to DataSource..(\\d+)..str.");
    
  @BeforeClass
  public static void beforeClassDihJdbcTest() throws Exception {
    try {
      Class.forName("org.hsqldb.jdbcDriver").getConstructor().newInstance();
      String oldProp = System.getProperty("derby.stream.error.field");
      System
          .setProperty("derby.stream.error.field",
              "org.apache.solr.handler.dataimport.AbstractDIHJdbcTestCase$DerbyUtil.DEV_NULL");
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getConstructor().newInstance();
      if (oldProp != null) {
        System.setProperty("derby.stream.error.field", oldProp);
      }
    } catch (Exception e) {
      throw e;
    }
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @AfterClass
  public static void afterClassDihJdbcTest() throws Exception {
    try {
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    } catch (SQLException e) {
      // ignore...we might not even be using derby this time...
    }
  }
  
  protected Database setAllowedDatabases() {
    return Database.RANDOM;
  }
  
  @Before
  public void beforeDihJdbcTest() throws Exception {  
    skipThisTest = false;
    dbToUse = setAllowedDatabases();
    if (dbToUse == Database.RANDOM) {
      if (random().nextBoolean()) {
        dbToUse = Database.DERBY;
      } else {
        dbToUse = Database.HSQLDB;
      }
    }
    
    clearIndex();
    assertU(commit());
    buildDatabase();
  }
  
  @After
  public void afterDihJdbcTest() throws Exception {
    Connection conn = null;
    Statement s = null;
    try {
      if (dbToUse == Database.DERBY) {
        try {
          conn = DriverManager
              .getConnection("jdbc:derby:memory:derbyDB;drop=true;territory=en_US");
        } catch (SQLException e) {
          if (!"08006".equals(e.getSQLState())) {
            throw e;
          }
        }
      } else if (dbToUse == Database.HSQLDB) {
        conn = DriverManager.getConnection("jdbc:hsqldb:mem:.");
        s = conn.createStatement();
        s.executeUpdate("shutdown");
      }
    } catch (SQLException e) {
      if(!skipThisTest) {
        throw e;
      }
    } finally {
      try {
        s.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
  }
  
  protected Connection newConnection() throws Exception {
    if (dbToUse == Database.DERBY) {
      return DriverManager.getConnection("jdbc:derby:memory:derbyDB;territory=en_US");
    } else if (dbToUse == Database.HSQLDB) {
      return DriverManager.getConnection("jdbc:hsqldb:mem:.");
    }
    throw new AssertionError("Invalid database to use: " + dbToUse);
  }
  
  protected void buildDatabase() throws Exception {
    Connection conn = null;
    try {
      if (dbToUse == Database.DERBY) {
        conn = DriverManager
            .getConnection("jdbc:derby:memory:derbyDB;create=true;territory=en_US");
      } else if (dbToUse == Database.HSQLDB) {
        conn = DriverManager.getConnection("jdbc:hsqldb:mem:.");
      } else {
        throw new AssertionError("Invalid database to use: " + dbToUse);
      }
      populateData(conn);
    } catch (SQLException sqe) {
      Throwable cause = sqe;
      while(cause.getCause()!=null) {
        cause = cause.getCause();
      }
    } finally {
      try {
        conn.close();
      } catch (Exception e1) {}
    }
  }
  
  protected void populateData(Connection conn) throws Exception {
    // no-op
  }
  
  public int totalDatabaseRequests(String dihHandlerName) throws Exception {
    LocalSolrQueryRequest request = lrf.makeRequest("indent", "true");
    String response = h.query(dihHandlerName, request);
    Matcher m = totalRequestsPattern.matcher(response);
    Assert.assertTrue("The handler " + dihHandlerName
        + " is not reporting any database requests. ",
        m.find() && m.groupCount() == 1);
    return Integer.parseInt(m.group(1));
  }
  
  public int totalDatabaseRequests() throws Exception {
    return totalDatabaseRequests("/dataimport");
  }
  
  protected LocalSolrQueryRequest generateRequest() {
    return lrf.makeRequest("command", "full-import", "dataConfig",
        generateConfig(), "clean", "true", "commit", "true", "synchronous",
        "true", "indent", "true");
  }
  
  protected abstract String generateConfig();
  
  public static class DerbyUtil {
    public static final OutputStream DEV_NULL = new OutputStream() {
      @Override
      public void write(int b) {}
    };
  }
}