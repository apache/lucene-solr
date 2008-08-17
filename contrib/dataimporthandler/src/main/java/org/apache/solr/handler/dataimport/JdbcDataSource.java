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

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * A DataSource implementation which can fetch data using JDBC.
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class JdbcDataSource extends
        DataSource<Iterator<Map<String, Object>>> {
  private static final Logger LOG = Logger.getLogger(JdbcDataSource.class
          .getName());

  private Callable<Connection> factory;

  private long connLastUsed = 0;

  private Connection conn;

  private Map<String, Integer> fieldNameVsType = new HashMap<String, Integer>();

  private boolean convertType = false;

  private int batchSize = FETCH_SIZE;

  public void init(Context context, Properties initProps) {
    Object o = initProps.get(CONVERT_TYPE);
    if (o != null)
      convertType = Boolean.parseBoolean(o.toString());

    createConnectionFactory(context, initProps);

    String bsz = initProps.getProperty("batchSize");
    if (bsz != null) {
      try {
        batchSize = Integer.parseInt(bsz);
        if (batchSize == -1)
          batchSize = Integer.MIN_VALUE;
      } catch (NumberFormatException e) {
        LOG.log(Level.WARNING, "Invalid batch size: " + bsz);
      }
    }

    for (Map<String, String> map : context.getAllEntityFields()) {
      String n = map.get(DataImporter.COLUMN);
      String t = map.get(DataImporter.TYPE);
      if ("sint".equals(t) || "integer".equals(t))
        fieldNameVsType.put(n, Types.INTEGER);
      else if ("slong".equals(t) || "long".equals(t))
        fieldNameVsType.put(n, Types.BIGINT);
      else if ("float".equals(t) || "sfloat".equals(t))
        fieldNameVsType.put(n, Types.FLOAT);
      else if ("double".equals(t) || "sdouble".equals(t))
        fieldNameVsType.put(n, Types.DOUBLE);
      else if ("date".equals(t))
        fieldNameVsType.put(n, Types.DATE);
      else if ("boolean".equals(t))
        fieldNameVsType.put(n, Types.BOOLEAN);
      else
        fieldNameVsType.put(n, Types.VARCHAR);
    }
  }

  private void createConnectionFactory(final Context context,
                                       final Properties initProps) {

    final String url = initProps.getProperty(URL);
    String driver = initProps.getProperty(DRIVER);

    if (url == null)
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "JDBC URL cannot be null");

    try {
      if (driver != null)
        Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
              "driver could not be loaded");
    }
    factory = new Callable<Connection>() {
      public Connection call() throws Exception {
        LOG.info("Creating a connection for entity "
                + context.getEntityAttribute(DataImporter.NAME) + " with URL: "
                + url);
        long start = System.currentTimeMillis();
        Connection c = DriverManager.getConnection(url, initProps);
        LOG.info("Time taken for getConnection(): "
                + (System.currentTimeMillis() - start));
        return c;
      }
    };
  }

  public Iterator<Map<String, Object>> getData(String query) {
    ResultSetIterator r = new ResultSetIterator(query);
    return r.getIterator();
  }

  private void logError(String msg, Exception e) {
    LOG.log(Level.WARNING, msg, e);
  }

  private List<String> readFieldNames(ResultSetMetaData metaData)
          throws SQLException {
    List<String> colNames = new ArrayList<String>();
    int count = metaData.getColumnCount();
    for (int i = 0; i < count; i++) {
      colNames.add(metaData.getColumnLabel(i + 1));
    }
    return colNames;
  }

  private class ResultSetIterator {
    ResultSet resultSet;

    Statement stmt = null;

    List<String> colNames;

    Iterator<Map<String, Object>> rSetIterator;

    public ResultSetIterator(String query) {

      try {
        Connection c = getConnection();
        stmt = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(batchSize);
        LOG.finer("Executing SQL: " + query);
        long start = System.currentTimeMillis();
        if (stmt.execute(query)) {
          resultSet = stmt.getResultSet();
        }
        LOG.finest("Time taken for sql :"
                + (System.currentTimeMillis() - start));
        colNames = readFieldNames(resultSet.getMetaData());
      } catch (Exception e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
                "Unable to execute query: " + query, e);
      }
      if (resultSet == null) {
        rSetIterator = new ArrayList<Map<String, Object>>().iterator();
        return;
      }

      rSetIterator = new Iterator<Map<String, Object>>() {
        public boolean hasNext() {
          return hasnext();
        }

        public Map<String, Object> next() {
          return getARow();
        }

        public void remove() {/* do nothing */
        }
      };
    }

    private Iterator<Map<String, Object>> getIterator() {
      return rSetIterator;
    }

    private Map<String, Object> getARow() {
      if (resultSet == null)
        return null;
      Map<String, Object> result = new HashMap<String, Object>();
      for (String colName : colNames) {
        try {
          if (!convertType) {
            // Use underlying database's type information
            result.put(colName, resultSet.getObject(colName));
            continue;
          }

          Integer type = fieldNameVsType.get(colName);
          if (type == null)
            type = 12;
          switch (type) {
            case Types.INTEGER:
              result.put(colName, resultSet.getInt(colName));
              break;
            case Types.FLOAT:
              result.put(colName, resultSet.getFloat(colName));
              break;
            case Types.BIGINT:
              result.put(colName, resultSet.getLong(colName));
              break;
            case Types.DOUBLE:
              result.put(colName, resultSet.getDouble(colName));
              break;
            case Types.DATE:
              result.put(colName, resultSet.getDate(colName));
              break;
            case Types.BOOLEAN:
              result
                      .put(colName, resultSet.getBoolean(colName));
              break;
            default:
              result.put(colName, resultSet.getString(colName));
              break;
          }
        } catch (SQLException e) {
          logError("Error reading data ", e);
          throw new DataImportHandlerException(
                  DataImportHandlerException.SEVERE,
                  "Error reading data from database", e);
        }
      }
      return result;
    }

    private boolean hasnext() {
      if (resultSet == null)
        return false;
      try {
        if (resultSet.next()) {
          return true;
        } else {
          close();
          return false;
        }
      } catch (SQLException e) {
        logError("Error reading data ", e);
        close();
        return false;
      }
    }

    private void close() {
      try {
        if (resultSet != null)
          resultSet.close();
        if (stmt != null)
          stmt.close();

      } catch (Exception e) {
        logError("Exception while closing result set", e);
      } finally {
        resultSet = null;
        stmt = null;
      }
    }
  }

  private Connection getConnection() throws Exception {
    long currTime = System.currentTimeMillis();
    if (currTime - connLastUsed > CONN_TIME_OUT) {
      synchronized (this) {
        Connection tmpConn = factory.call();
        close();
        connLastUsed = System.currentTimeMillis();
        return conn = tmpConn;
      }

    } else {
      connLastUsed = currTime;
      return conn;
    }
  }

  protected void finalize() {
    try {
      conn.close();
    } catch (Exception e) {
    }
  }

  public void close() {
    try {
      conn.close();
    } catch (Exception e) {
    }

  }

  private static final long CONN_TIME_OUT = 10 * 1000; // 10 seconds

  private static final int FETCH_SIZE = 500;

  public static final String URL = "url";

  public static final String DRIVER = "driver";

  public static final String CONVERT_TYPE = "convertType";
}
