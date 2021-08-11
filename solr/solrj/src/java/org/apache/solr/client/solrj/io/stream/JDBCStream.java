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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.StreamParams;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * Connects to a datasource using a registered JDBC driver and execute a query. The results of
 * that query will be returned as tuples. An EOF tuple will indicate that all have been read.
 * 
 * Supported Datatypes for most types vary by JDBC driver based on the specific 
 * java type as reported by {@link java.sql.ResultSetMetaData#getColumnClassName(int)}. 
 * The exception are {@link Types#DATE}, {@link Types#TIME} or {@link Types#TIMESTAMP}
 * which are determined by the JDBC type.
 * 
 * <table rules="all" frame="box" cellpadding="3" summary="Supported Java Types">
 * <tr>
 *   <th>Java or JDBC Type</th>
 *   <th>Tuple Type</th>
 *   <th>Notes</th>
 * </tr>
 * <tr>
 *   <td>Boolean</td>
 *   <td>Boolean</td>
 *   <td></td>
 * </tr>
 * <tr>
 *   <td>String</td>
 *   <td>String</td>
 *   <td></td>
 * </tr>
 * <tr>
 *   <td>Short, Integer, Long</td>
 *   <td>Long</td>
 *   <td></td>
 * </tr>
 * <tr>
 *   <td>Float, Double</td>
 *   <td>Double</td>
 *   <td></td>
 * </tr>
 * <tr>
 *   <td>{@link Clob} and subclasses</td>
 *   <td>String</td>
 *   <td>Clobs up to length 2<sup>31</sup>-1 are supported.</td>
 * </tr>
 * <tr>
 *   <td>Other subclasses of {@link Number}</td>
 *   <td>Long, Double</td>
 *   <td>Tuple Type based on {@link BigDecimal#scale()}.</td>
 * </tr>
 * <tr>
 *   <td>JDBC {@link Types#DATE}</td>
 *   <td>String</td>
 *   <td>yyyy-MM-dd, calls {@link Date#toString}</td>
 * </tr>
 * <tr>
 *   <td>JDBC {@link Types#TIME}</td>
 *   <td>String</td>
 *   <td>hh:mm:ss, calls {@link Time#toString}</td>
 * </tr>
 * <tr>
 *   <td>JDBC {@link Types#TIMESTAMP}</td>
 *   <td>String</td>
 *   <td>See {@link DateTimeFormatter#ISO_INSTANT}</td>
 * </tr>
 * </table>
 * 
 * @since 6.0.0
 **/

public class JDBCStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  // These are java types that we can directly support as an Object instance. Other supported
  // types will require some level of conversion (short -> long, etc...)
  // We'll use a static constructor to load this set.
  private static final HashSet<String> directSupportedTypes = new HashSet<>();
  static {
      directSupportedTypes.add(String.class.getName()); 
      directSupportedTypes.add(Double.class.getName()); 
      directSupportedTypes.add(Long.class.getName()); 
      directSupportedTypes.add(Boolean.class.getName());
  }
  
  // Provided as input
  private String driverClassName;
  private String connectionUrl;
  private String sqlQuery;
  private StreamComparator definedSort;
  private int fetchSize;
  
  // Internal
  private Connection connection;
  private Properties connectionProperties;
  private Statement statement;
  private ResultSetValueSelector[] valueSelectors;
  protected ResultSet resultSet;
  protected transient StreamContext streamContext;
  protected String sep = Character.toString((char)31);

  public JDBCStream(String connectionUrl, String sqlQuery, StreamComparator definedSort) throws IOException {
    this(connectionUrl, sqlQuery, definedSort, null, null);
  }
  
  public JDBCStream(String connectionUrl, String sqlQuery, StreamComparator definedSort, Properties connectionProperties, String driverClassName) throws IOException {
    init(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClassName, 5000);
  }
  
  public JDBCStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter connectionUrlExpression = factory.getNamedOperand(expression, "connection");
    StreamExpressionNamedParameter sqlQueryExpression = factory.getNamedOperand(expression, "sql");
    StreamExpressionNamedParameter definedSortExpression = factory.getNamedOperand(expression, SORT);
    StreamExpressionNamedParameter driverClassNameExpression = factory.getNamedOperand(expression, "driver");
    StreamExpressionNamedParameter fetchSizeExpression = factory.getNamedOperand(expression, "fetchSize");


    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found", expression));
    }
           
    // All named params we don't care about will be passed to the driver on connection
    Properties connectionProperties = new Properties();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("driver") && !namedParam.getName().equals("connection") && !namedParam.getName().equals("sql") && !namedParam.getName().equals(SORT)){
        connectionProperties.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    int fetchSize = 5000;
    if(null != fetchSizeExpression && fetchSizeExpression.getParameter() instanceof StreamExpressionValue){
      String fetchSizeString = ((StreamExpressionValue)fetchSizeExpression.getParameter()).getValue();
      fetchSize = Integer.parseInt(fetchSizeString);
    }

    // connectionUrl, required
    String connectionUrl = null;
    if(null != connectionUrlExpression && connectionUrlExpression.getParameter() instanceof StreamExpressionValue){
      connectionUrl = ((StreamExpressionValue)connectionUrlExpression.getParameter()).getValue();
    }
    if(null == connectionUrl){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - connection not found", connectionUrlExpression));
    }
    
    // sql, required
    String sqlQuery = null;
    if(null != sqlQueryExpression && sqlQueryExpression.getParameter() instanceof StreamExpressionValue){
      sqlQuery = ((StreamExpressionValue)sqlQueryExpression.getParameter()).getValue();
    }
    if(null == sqlQuery){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - sql not found", sqlQueryExpression));
    }
    
    // definedSort, required
    StreamComparator definedSort = null;
    if(null != definedSortExpression && definedSortExpression.getParameter() instanceof StreamExpressionValue){
      definedSort = factory.constructComparator(((StreamExpressionValue)definedSortExpression.getParameter()).getValue(), FieldComparator.class);
    }
    if(null == definedSort){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - sort not found", definedSortExpression));
    }
    
    // driverClass, optional
    String driverClass = null;
    if(null != driverClassNameExpression && driverClassNameExpression.getParameter() instanceof StreamExpressionValue){
      driverClass = ((StreamExpressionValue)driverClassNameExpression.getParameter()).getValue();
    }

    // We've got all the required items
    init(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClass, fetchSize);
  }
    
  private void init(String connectionUrl, String sqlQuery, StreamComparator definedSort, Properties connectionProperties, String driverClassName, int fetchSize) {
    this.connectionUrl = connectionUrl;
    this.sqlQuery = sqlQuery;
    this.definedSort = definedSort;
    this.connectionProperties = connectionProperties;
    this.driverClassName = driverClassName;
    this.fetchSize = fetchSize;
  }
  
  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  /**
  * Opens the JDBCStream
  *
  ***/
  public void open() throws IOException {
    
    try{
      if(null != driverClassName){
        Class.forName(driverClassName);
      }
    } catch (ClassNotFoundException e){
      throw new IOException(String.format(Locale.ROOT, "Failed to load JDBC driver for '%s'", driverClassName), e);
    }
    
    // See if we can figure out the driver based on the url, if not then tell the user they most likely want to provide the driverClassName.
    // Not being able to find a driver generally means the driver has not been loaded.
    try{
      if(null == DriverManager.getDriver(connectionUrl)){
        throw new SQLException("DriverManager.getDriver(url) returned null");
      }
    } catch(SQLException e){
      throw new IOException(String.format(Locale.ROOT,
          "Failed to determine JDBC driver from connection url '%s'. Usually this means the driver is not loaded - " +
              "you can have JDBCStream try to load it by providing the 'driverClassName' value", connectionUrl), e);
    }
    
    try {
      connection = DriverManager.getConnection(connectionUrl, connectionProperties);
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT, "Failed to open JDBC connection to '%s'", connectionUrl), e);
    }
    
    try{
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT, "Failed to create a statement from JDBC connection '%s'",
          connectionUrl), e);
    }
    
    try{
      resultSet = statement.executeQuery(sqlQuery);
      resultSet.setFetchSize(fetchSize);
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT, "Failed to execute sqlQuery '%s' against JDBC connection '%s'.%nCaused by: %s", sqlQuery, connectionUrl, e.getMessage()), e);
    }
    
    try{
      // using the metadata, build selectors for each column
      valueSelectors = constructValueSelectors(resultSet.getMetaData());
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT,
          "Failed to generate value selectors for sqlQuery '%s' against JDBC connection '%s'", sqlQuery, connectionUrl), e);
    }
  }

  private ResultSetValueSelector[] constructValueSelectors(ResultSetMetaData metadata) throws SQLException{
    ResultSetValueSelector[] valueSelectors = new ResultSetValueSelector[metadata.getColumnCount()];    
    for (int columnIdx = 0; columnIdx < metadata.getColumnCount(); ++columnIdx) {      
      ResultSetValueSelector valueSelector = determineValueSelector(columnIdx, metadata);
      if(valueSelector==null) {
        int columnNumber = columnIdx + 1;
        String columnName = metadata.getColumnLabel(columnNumber);
        String className = metadata.getColumnClassName(columnNumber);
        String typeName = metadata.getColumnTypeName(columnNumber);
        throw new SQLException(String.format(Locale.ROOT,
            "Unable to determine the valueSelector for column '%s' (col #%d) of java class '%s' and type '%s'",
            columnName, columnNumber, className, typeName));
      }
      valueSelectors[columnIdx] = valueSelector;
    }        
    return valueSelectors;
  }
  
  protected ResultSetValueSelector determineValueSelector(int columnIdx, ResultSetMetaData metadata) throws SQLException {
    final int columnNumber = columnIdx + 1; // cause it starts at 1
    // Use getColumnLabel instead of getColumnName to make sure fields renamed with AS as picked up properly
    final String columnName = metadata.getColumnLabel(columnNumber);
    final int jdbcType = metadata.getColumnType(columnNumber);      
    final String className = metadata.getColumnClassName(columnNumber);
    ResultSetValueSelector valueSelector = null;
    
    // Directly supported types can be just directly returned - no conversion really necessary
    if(directSupportedTypes.contains(className)){
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Object obj = resultSet.getObject(columnNumber);
          if(resultSet.wasNull()){ return null; }
          if(obj instanceof String) {
            String s = (String)obj;
            if(s.indexOf(sep) > -1) {
              s = s.substring(1);
              return s.split(sep);
            }
          }

          return obj;
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } 
    // We're checking the Java class names because there are lots of SQL types across
    // lots of database drivers that can be mapped to standard Java types. Basically, 
    // this makes it easier and we don't have to worry about esoteric type names in the 
    // JDBC family of types
    else if(Short.class.getName().equals(className)) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Short obj = resultSet.getShort(columnNumber);
          if(resultSet.wasNull()){ return null; }
          return obj.longValue();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } else if(Integer.class.getName().equals(className)) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Integer obj = resultSet.getInt(columnNumber);
          if(resultSet.wasNull()){ return null; }
          return obj.longValue();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } else if(Float.class.getName().equals(className)) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Float obj = resultSet.getFloat(columnNumber);
          if(resultSet.wasNull()){ return null; }
          return obj.doubleValue();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    }
    // Here we are switching to check against the SQL type because date/times are
    // notorious for not being consistent. We don't know if the driver is mapping
    // to a java.time.* type or some old-school type. 
    else if (jdbcType == Types.DATE) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Date sqlDate = resultSet.getDate(columnNumber);
          return resultSet.wasNull() ? null : sqlDate.toString();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } else if (jdbcType == Types.TIME ) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Time sqlTime = resultSet.getTime(columnNumber);
          return resultSet.wasNull() ? null : sqlTime.toString();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } else if (jdbcType == Types.TIMESTAMP) {
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Timestamp sqlTimestamp = resultSet.getTimestamp(columnNumber);
          return resultSet.wasNull() ? null : sqlTimestamp.toInstant().toString();
        }
        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    } else if (Object.class.getName().equals(className)) {
      // Calcite SQL type ANY comes across as generic Object (for multi-valued fields)
      valueSelector = new ResultSetValueSelector() {
        @Override
        public Object selectValue(ResultSet resultSet) throws SQLException {
          Object obj = resultSet.getObject(columnNumber);
          return resultSet.wasNull() ? null : obj;
        }

        @Override
        public String getColumnName() {
          return columnName;
        }
      };
    }
    // Now we're going to start seeing if things are assignable from the returned type
    // to a more general type - this allows us to cover cases where something we weren't 
    // explicitly expecting, but can handle, is being returned.
    else {
      Class<?> clazz;
      try {
        clazz = Class.forName(className, false, getClass().getClassLoader());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final int scale = metadata.getScale(columnNumber);
      if (Number.class.isAssignableFrom(clazz)) {
        if (scale > 0) {
          valueSelector = new ResultSetValueSelector() {
            @Override
            public Object selectValue(ResultSet resultSet) throws SQLException {
              BigDecimal bd = resultSet.getBigDecimal(columnNumber);
              return resultSet.wasNull() ? null : bd.doubleValue();                
            }
            @Override
            public String getColumnName() {
              return columnName;
            }
          };            
        } else {
          valueSelector = new ResultSetValueSelector() {
            @Override
            public Object selectValue(ResultSet resultSet) throws SQLException {
              BigDecimal bd = resultSet.getBigDecimal(columnNumber);
              return resultSet.wasNull() ? null : bd.longValue();
            }
            @Override
            public String getColumnName() {
              return columnName;
            }
          };            
        }          
      } else if (Clob.class.isAssignableFrom(clazz)) {
        valueSelector = new ResultSetValueSelector() {
          @Override
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Clob c = resultSet.getClob(columnNumber);
            if (resultSet.wasNull()) {
              return null;
            }
            long length = c.length();
            int lengthInt = (int) length;
            if (length != lengthInt) {
              throw new SQLException(String.format(Locale.ROOT,
                  "Encountered a clob of length #%l in column '%s' (col #%d).  Max supported length is #%i.",
                  length, columnName, columnNumber, Integer.MAX_VALUE));
            }
            return c.getSubString(1, lengthInt);
          }
          @Override
          public String getColumnName() {
            return columnName;
          }
        };
      } 
    }
    return valueSelector;
  }
  
  /**
   *  Closes the JDBCStream
   **/
  public void close() throws IOException {
    try{
      if(null != resultSet){ // it's not required in JDBC that ResultSet implements the isClosed() function
        resultSet.close();
      }
      if(null != statement && !statement.isClosed()){
        statement.close();
      }
      if(null != connection && !connection.isClosed()){
        connection.close();
      }
    } catch (SQLException e) {
      throw new IOException("Failed to properly close JDBCStream", e);
    }    
  }
  
  public Tuple read() throws IOException {
    
    try {
      Tuple tuple = new Tuple();
      if (resultSet.next()) {
        // we have a record
        for (ResultSetValueSelector selector : valueSelectors) {
          tuple.put(selector.getColumnName(), selector.selectValue(resultSet));
        }
      } else {
        // we do not have a record
        tuple.put(StreamParams.EOF, true);
      }
      
      return tuple;
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT, "Failed to read next record with error '%s'", e.getMessage()), e);
    }
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])
    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // connection url
    expression.addParameter(new StreamExpressionNamedParameter("connection", connectionUrl));
    
    // sql
    expression.addParameter(new StreamExpressionNamedParameter("sql", sqlQuery));

    // fetchSize
    expression.addParameter(new StreamExpressionNamedParameter("fetchSize", Integer.toString(fetchSize)));

    // sort
    expression.addParameter(new StreamExpressionNamedParameter(SORT, definedSort.toExpression(factory)));
    
    // driver class
    if(null != driverClassName){
      expression.addParameter(new StreamExpressionNamedParameter("driver", driverClassName));      
    }
    
    // connection properties
    if(null != connectionProperties){
      for(String propertyName : connectionProperties.stringPropertyNames()){
        expression.addParameter(new StreamExpressionNamedParameter(propertyName, connectionProperties.getProperty(propertyName)));    
      }
    }
        
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    
    StreamExpression expression = (StreamExpression)toExpression(factory);
    explanation.setExpression(expression.toString());
    
    String driverClassName = this.driverClassName;
    if(null == driverClassName){
      try{
        driverClassName = DriverManager.getDriver(connectionUrl).getClass().getName();
      }
      catch(Exception e){
        driverClassName = String.format(Locale.ROOT, "Failed to find driver for connectionUrl='%s'", connectionUrl);
      }
    }
    
    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName("jdbc-source");
    child.setImplementingClass(driverClassName);
    child.setExpressionType(ExpressionType.DATASTORE);    
    child.setExpression(sqlQuery);
    
    explanation.addChild(child);
    
    return explanation;
  }
  
  @Override
  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  @Override
  public StreamComparator getStreamSort() {
    // TODO: Need to somehow figure out the sort applied to the incoming data. This is not something you can ask a JDBC stream
    // Possibly we can ask the creator to tell us the fields the data is sorted by. This would be duplicate information because
    // it's already in the sqlQuery but there's no way we can reliably determine the sort from the query.
    return definedSort;
  }
  
  public interface ResultSetValueSelector {
    String getColumnName();
    Object selectValue(ResultSet resultSet) throws SQLException;
  }
}

