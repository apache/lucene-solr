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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import org.apache.solr.common.SolrException;
import org.apache.solr.util.CryptoKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * <p> A DataSource implementation which can fetch data using JDBC. </p> <p> Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a> for more
 * details. </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public class JdbcDataSource extends
        DataSource<Iterator<Map<String, Object>>> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected Callable<Connection> factory;

  private long connLastUsed = 0;

  private Connection conn;
  
  private ResultSetIterator resultSetIterator;  

  private Map<String, Integer> fieldNameVsType = new HashMap<>();

  private boolean convertType = false;

  private int batchSize = FETCH_SIZE;

  private int maxRows = 0;

  @Override
  public void init(Context context, Properties initProps) {
    resolveVariables(context, initProps);
    initProps = decryptPwd(context, initProps);
    Object o = initProps.get(CONVERT_TYPE);
    if (o != null)
      convertType = Boolean.parseBoolean(o.toString());

    factory = createConnectionFactory(context, initProps);

    String bsz = initProps.getProperty("batchSize");
    if (bsz != null) {
      bsz = context.replaceTokens(bsz);
      try {
        batchSize = Integer.parseInt(bsz);
        if (batchSize == -1)
          batchSize = Integer.MIN_VALUE;
      } catch (NumberFormatException e) {
        log.warn("Invalid batch size: " + bsz);
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
      else if ("binary".equals(t))
        fieldNameVsType.put(n, Types.BLOB);
      else
        fieldNameVsType.put(n, Types.VARCHAR);
    }
  }

  private Properties decryptPwd(Context context, Properties initProps) {
    String encryptionKey = initProps.getProperty("encryptKeyFile");
    if (initProps.getProperty("password") != null && encryptionKey != null) {
      // this means the password is encrypted and use the file to decode it
      try {
        try (Reader fr = new InputStreamReader(new FileInputStream(encryptionKey), UTF_8)) {
          char[] chars = new char[100];//max 100 char password
          int len = fr.read(chars);
          if (len < 6)
            throw new DataImportHandlerException(SEVERE, "There should be a password of length 6 atleast " + encryptionKey);
          Properties props = new Properties();
          props.putAll(initProps);
          String password = null;
          try {
            password = CryptoKeys.decodeAES(initProps.getProperty("password"), new String(chars, 0, len)).trim();
          } catch (SolrException se) {
            throw new DataImportHandlerException(SEVERE, "Error decoding password", se.getCause());
          }
          props.put("password", password);
          initProps = props;
        }
      } catch (IOException e) {
        throw new DataImportHandlerException(SEVERE, "Could not load encryptKeyFile  " + encryptionKey);
      }
    }
    return initProps;
  }

  protected Callable<Connection> createConnectionFactory(final Context context,
                                       final Properties initProps) {
//    final VariableResolver resolver = context.getVariableResolver();
    final String jndiName = initProps.getProperty(JNDI_NAME);
    final String url = initProps.getProperty(URL);
    final String driver = initProps.getProperty(DRIVER);

    if (url == null && jndiName == null)
      throw new DataImportHandlerException(SEVERE,
              "JDBC URL or JNDI name has to be specified");

    if (driver != null) {
      try {
        DocBuilder.loadClass(driver, context.getSolrCore());
      } catch (ClassNotFoundException e) {
        wrapAndThrow(SEVERE, e, "Could not load driver: " + driver);
      }
    } else {
      if(jndiName == null){
        throw new DataImportHandlerException(SEVERE, "One of driver or jndiName must be specified in the data source");
      }
    }

    String s = initProps.getProperty("maxRows");
    if (s != null) {
      maxRows = Integer.parseInt(s);
    }

    return factory = new Callable<Connection>() {
      @SuppressWarnings("unchecked")
      @Override
      public Connection call() throws Exception {
        log.info("Creating a connection for entity "
                + context.getEntityAttribute(DataImporter.NAME) + " with URL: "
                + url);
        long start = System.nanoTime();
        Connection c = null;

        if (jndiName != null) {
          c = getFromJndi(initProps, jndiName);
        } else if (url != null) {
          try {
            c = DriverManager.getConnection(url, initProps);
          } catch (SQLException e) {
            // DriverManager does not allow you to use a driver which is not loaded through
            // the class loader of the class which is trying to make the connection.
            // This is a workaround for cases where the user puts the driver jar in the
            // solr.home/lib or solr.home/core/lib directories.
            Driver d = (Driver) DocBuilder.loadClass(driver, context.getSolrCore()).getConstructor().newInstance();
            c = d.connect(url, initProps);
          }
        }
        if (c != null) {
          try {
            initializeConnection(c, initProps);
          } catch (SQLException e) {
            try {
              c.close();
            } catch (SQLException e2) {
              log.warn("Exception closing connection during cleanup", e2);
            }

            throw new DataImportHandlerException(SEVERE, "Exception initializing SQL connection", e);
          }
        }
        log.info("Time taken for getConnection(): "
            + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
        return c;
      }

      private void initializeConnection(Connection c, final Properties initProps)
          throws SQLException {
        if (Boolean.parseBoolean(initProps.getProperty("readOnly"))) {
          c.setReadOnly(true);
          // Add other sane defaults
          c.setAutoCommit(true);
          c.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
          c.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
        if (!Boolean.parseBoolean(initProps.getProperty("autoCommit"))) {
          c.setAutoCommit(false);
        }
        String transactionIsolation = initProps.getProperty("transactionIsolation");
        if ("TRANSACTION_READ_UNCOMMITTED".equals(transactionIsolation)) {
          c.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } else if ("TRANSACTION_READ_COMMITTED".equals(transactionIsolation)) {
          c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } else if ("TRANSACTION_REPEATABLE_READ".equals(transactionIsolation)) {
          c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } else if ("TRANSACTION_SERIALIZABLE".equals(transactionIsolation)) {
          c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } else if ("TRANSACTION_NONE".equals(transactionIsolation)) {
          c.setTransactionIsolation(Connection.TRANSACTION_NONE);
        }
        String holdability = initProps.getProperty("holdability");
        if ("CLOSE_CURSORS_AT_COMMIT".equals(holdability)) {
          c.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        } else if ("HOLD_CURSORS_OVER_COMMIT".equals(holdability)) {
          c.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        }
      }

      private Connection getFromJndi(final Properties initProps, final String jndiName) throws NamingException,
          SQLException {

        Connection c = null;
        InitialContext ctx =  new InitialContext();
        Object jndival =  ctx.lookup(jndiName);
        if (jndival instanceof javax.sql.DataSource) {
          javax.sql.DataSource dataSource = (javax.sql.DataSource) jndival;
          String user = (String) initProps.get("user");
          String pass = (String) initProps.get("password");
          if(user == null || user.trim().equals("")){
            c = dataSource.getConnection();
          } else {
            c = dataSource.getConnection(user, pass);
          }
        } else {
          throw new DataImportHandlerException(SEVERE,
                  "the jndi name : '"+jndiName +"' is not a valid javax.sql.DataSource");
        }
        return c;
      }
    };
  }

  private void resolveVariables(Context ctx, Properties initProps) {
    for (Map.Entry<Object, Object> entry : initProps.entrySet()) {
      if (entry.getValue() != null) {
        entry.setValue(ctx.replaceTokens((String) entry.getValue()));
      }
    }
  }

  @Override
  public Iterator<Map<String, Object>> getData(String query) {
    if (resultSetIterator != null) {
      resultSetIterator.close();
      resultSetIterator = null;
    }
    resultSetIterator = createResultSetIterator(query);
    return resultSetIterator.getIterator();
  }

  protected ResultSetIterator createResultSetIterator(String query) {
    return new ResultSetIterator(query);
  }

  private void logError(String msg, Exception e) {
    log.warn(msg, e);
  }

  protected List<String> readFieldNames(ResultSetMetaData metaData)
          throws SQLException {
    List<String> colNames = new ArrayList<>();
    int count = metaData.getColumnCount();
    for (int i = 0; i < count; i++) {
      colNames.add(metaData.getColumnLabel(i + 1));
    }
    return colNames;
  }

  protected class ResultSetIterator {
    private ResultSet resultSet;

    private Statement stmt = null;

    private List<String> colNames; 
   
    private Iterator<Map<String, Object>> rSetIterator;

    public ResultSetIterator(String query) {

      try {
        Connection c = getConnection();
        stmt = createStatement(c, batchSize, maxRows);
        log.debug("Executing SQL: " + query);
        long start = System.nanoTime();
        resultSet = executeStatement(stmt, query);
        log.trace("Time taken for sql :"
                + TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
        setColNames(resultSet);
      } catch (Exception e) {
        close();
        wrapAndThrow(SEVERE, e, "Unable to execute query: " + query);
        return;
      }
      if (resultSet == null) {
        close();
        rSetIterator = new ArrayList<Map<String, Object>>().iterator();
        return;
      }

      rSetIterator = createIterator(convertType, fieldNameVsType);
    }

    
    protected Statement createStatement(final Connection c, final int batchSize, final int maxRows)
        throws SQLException {
      Statement statement = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(batchSize);
      statement.setMaxRows(maxRows);
      return statement;
    }

    protected ResultSet executeStatement(Statement statement, String query) throws SQLException {
      boolean resultSetReturned = statement.execute(query);
      return getNextResultSet(resultSetReturned, statement);
    }

    protected ResultSet getNextResultSet(final boolean initialResultSetAvailable, final Statement statement) throws SQLException {
      boolean resultSetAvailable = initialResultSetAvailable;
      while (!resultSetAvailable && statement.getUpdateCount() != -1) {
        resultSetAvailable = statement.getMoreResults();
      }
      if (resultSetAvailable) {
        return statement.getResultSet();
      }
      return null;
    }
    
    protected void setColNames(final ResultSet resultSet) throws SQLException {
      if (resultSet != null) {
        colNames = readFieldNames(resultSet.getMetaData());
      } else {
        colNames = Collections.emptyList();
      }
    }

    protected Iterator<Map<String,Object>> createIterator(final boolean convertType,
        final Map<String,Integer> fieldNameVsType) {
      return new Iterator<Map<String,Object>>() {
        @Override
        public boolean hasNext() {
          return hasnext();
        }

        @Override
        public Map<String,Object> next() {
          return getARow(convertType, fieldNameVsType);
        }

        @Override
        public void remove() {/* do nothing */
        }
      };
    }
    
 

    protected Map<String,Object> getARow(boolean convertType, Map<String,Integer> fieldNameVsType) {
      if (getResultSet() == null)
        return null;
      Map<String, Object> result = new HashMap<>();
      for (String colName : getColNames()) {
        try {
          if (!convertType) {
            // Use underlying database's type information except for BigDecimal and BigInteger
            // which cannot be serialized by JavaBin/XML. See SOLR-6165
            Object value = getResultSet().getObject(colName);
            if (value instanceof BigDecimal || value instanceof BigInteger) {
              result.put(colName, value.toString());
            } else {
              result.put(colName, value);
            }
            continue;
          }

          Integer type = fieldNameVsType.get(colName);
          if (type == null)
            type = Types.VARCHAR;
          switch (type) {
            case Types.INTEGER:
              result.put(colName, getResultSet().getInt(colName));
              break;
            case Types.FLOAT:
              result.put(colName, getResultSet().getFloat(colName));
              break;
            case Types.BIGINT:
              result.put(colName, getResultSet().getLong(colName));
              break;
            case Types.DOUBLE:
              result.put(colName, getResultSet().getDouble(colName));
              break;
            case Types.DATE:
              result.put(colName, getResultSet().getTimestamp(colName));
              break;
            case Types.BOOLEAN:
              result.put(colName, getResultSet().getBoolean(colName));
              break;
            case Types.BLOB:
              result.put(colName, getResultSet().getBytes(colName));
              break;
            default:
              result.put(colName, getResultSet().getString(colName));
              break;
          }
        } catch (SQLException e) {
          logError("Error reading data ", e);
          wrapAndThrow(SEVERE, e, "Error reading data from database");
        }
      }
      return result;
    }

    protected boolean hasnext() {
      if (getResultSet() == null) {
        close();
        return false;
      }
      try {
        if (getResultSet().next()) {
          return true;
        } else {
          closeResultSet();
          setResultSet(getNextResultSet(getStatement().getMoreResults(), getStatement()));
          setColNames(getResultSet());
          return hasnext();
        }
      } catch (SQLException e) {
        close();
        wrapAndThrow(SEVERE,e);
        return false;
      }
    }

    protected void close() {
      closeResultSet();
      try {
        if (getStatement() != null)
          getStatement().close();
      } catch (Exception e) {
        logError("Exception while closing statement", e);
      } finally {
        setStatement(null);
      }
    }

    protected void closeResultSet() {
      try {
        if (getResultSet() != null) {
          getResultSet().close();
        }
      } catch (Exception e) {
        logError("Exception while closing result set", e);
      } finally {
        setResultSet(null);
      }
    }

    protected final Iterator<Map<String,Object>> getIterator() {
      return rSetIterator;
    }
    
    
    protected final Statement getStatement() {
      return stmt;
    }
    
    protected final void setStatement(Statement stmt) {
      this.stmt = stmt;
    }
    
    protected final ResultSet getResultSet() {
      return resultSet;
    }
    
    protected final void setResultSet(ResultSet resultSet) {
      this.resultSet = resultSet;
    }
    
    protected final List<String> getColNames() {
      return colNames;
    }

    protected final void setColNames(List<String> colNames) {
      this.colNames = colNames;
    }
    
  }

  protected Connection getConnection() throws Exception {
    long currTime = System.nanoTime();
    if (currTime - connLastUsed > CONN_TIME_OUT) {
      synchronized (this) {
        Connection tmpConn = factory.call();
        closeConnection();
        connLastUsed = System.nanoTime();
        return conn = tmpConn;
      }

    } else {
      connLastUsed = currTime;
      return conn;
    }
  }

  private boolean isClosed = false;

  @Override
  public void close() {
    if (resultSetIterator != null) {
      resultSetIterator.close();
    }
    try {
      closeConnection();
    } finally {
      isClosed = true;
    }
  }

  private void closeConnection()  {
    try {
      if (conn != null) {
        try {
          //SOLR-2045
          conn.commit();
        } catch(Exception ex) {
          //ignore.
        }
        conn.close();
      }
    } catch (Exception e) {
      log.error("Ignoring Error when closing connection", e);
    }
  }

  private static final long CONN_TIME_OUT = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

  private static final int FETCH_SIZE = 500;

  public static final String URL = "url";

  public static final String JNDI_NAME = "jndiName";

  public static final String DRIVER = "driver";

  public static final String CONVERT_TYPE = "convertType";
}
