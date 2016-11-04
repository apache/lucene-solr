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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

/**
 * Connects to a datasource using a registered JDBC driver and execute a query. The results of
 * that query will be returned as tuples. An EOF tuple will indicate that all have been read.
 * 
 * Supported Datatypes
 * JDBC Type     | Tuple Type
 * --------------|---------------
 * String        | String
 * Short         | Long
 * Integer       | Long
 * Long          | Long
 * Float         | Double
 * Double        | Double
 * Boolean       | Boolean
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
  
  // Internal
  private Connection connection;
  private Properties connectionProperties;
  private Statement statement;
  private ResultSetValueSelector[] valueSelectors;
  protected ResultSet resultSet;
  protected transient StreamContext streamContext;

  public JDBCStream(String connectionUrl, String sqlQuery, StreamComparator definedSort) throws IOException {
    this(connectionUrl, sqlQuery, definedSort, null, null);
  }
  
  public JDBCStream(String connectionUrl, String sqlQuery, StreamComparator definedSort, Properties connectionProperties, String driverClassName) throws IOException {
    init(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClassName);
  }
  
  public JDBCStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter connectionUrlExpression = factory.getNamedOperand(expression, "connection");
    StreamExpressionNamedParameter sqlQueryExpression = factory.getNamedOperand(expression, "sql");
    StreamExpressionNamedParameter definedSortExpression = factory.getNamedOperand(expression, "sort");
    StreamExpressionNamedParameter driverClassNameExpression = factory.getNamedOperand(expression, "driver");
    
    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found", expression));
    }
           
    // All named params we don't care about will be passed to the driver on connection
    Properties connectionProperties = new Properties();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("driver") && !namedParam.getName().equals("connection") && !namedParam.getName().equals("sql") && !namedParam.getName().equals("sort")){
        connectionProperties.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
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
    init(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClass);
  }
    
  private void init(String connectionUrl, String sqlQuery, StreamComparator definedSort, Properties connectionProperties, String driverClassName) {
    this.connectionUrl = connectionUrl;
    this.sqlQuery = sqlQuery;
    this.definedSort = definedSort;
    this.connectionProperties = connectionProperties;
    this.driverClassName = driverClassName;
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
    } catch (SQLException e) {
      throw new IOException(String.format(Locale.ROOT, "Failed to execute sqlQuery '%s' against JDBC connection '%s'",
          sqlQuery, connectionUrl), e);
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
    
    for(int columnIdx = 0; columnIdx < metadata.getColumnCount(); ++columnIdx){
      
      final int columnNumber = columnIdx + 1; // cause it starts at 1
      // Use getColumnLabel instead of getColumnName to make sure fields renamed with AS as picked up properly
      final String columnName = metadata.getColumnLabel(columnNumber);
      String className = metadata.getColumnClassName(columnNumber);
      String typeName = metadata.getColumnTypeName(columnNumber);
            
      if(directSupportedTypes.contains(className)){
        valueSelectors[columnIdx] = new ResultSetValueSelector() {
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Object obj = resultSet.getObject(columnNumber);
            if(resultSet.wasNull()){ return null; }
            return obj;
          }
          public String getColumnName() {
            return columnName;
          }
        };
      } else if(Short.class.getName().equals(className)) {
        valueSelectors[columnIdx] = new ResultSetValueSelector() {
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Short obj = resultSet.getShort(columnNumber);
            if(resultSet.wasNull()){ return null; }
            return obj.longValue();
          }
          public String getColumnName() {
            return columnName;
          }
        };
      } else if(Integer.class.getName().equals(className)) {
        valueSelectors[columnIdx] = new ResultSetValueSelector() {
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Integer obj = resultSet.getInt(columnNumber);
            if(resultSet.wasNull()){ return null; }
            return obj.longValue();
          }
          public String getColumnName() {
            return columnName;
          }
        };
      } else if(Float.class.getName().equals(className)) {
        valueSelectors[columnIdx] = new ResultSetValueSelector() {
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Float obj = resultSet.getFloat(columnNumber);
            if(resultSet.wasNull()){ return null; }
            return obj.doubleValue();
          }
          public String getColumnName() {
            return columnName;
          }
        };
      } else {
        throw new SQLException(String.format(Locale.ROOT,
            "Unable to determine the valueSelector for column '%s' (col #%d) of java class '%s' and type '%s'",
            columnName, columnNumber, className, typeName));
      }
    }
    
    return valueSelectors;
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
    
    try{
      Map<Object,Object> fields = new HashMap<>();
      if(resultSet.next()){
        // we have a record
        for(ResultSetValueSelector selector : valueSelectors){
          fields.put(selector.getColumnName(), selector.selectValue(resultSet));
        }
      }
      else{
        // we do not have a record
        fields.put("EOF", true);
      }
      
      return new Tuple(fields);
    }
    catch(SQLException e){
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
    
    // sort
    expression.addParameter(new StreamExpressionNamedParameter("sort", definedSort.toExpression(factory)));
    
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
}

interface ResultSetValueSelector {
  String getColumnName();
  Object selectValue(ResultSet resultSet) throws SQLException;
}