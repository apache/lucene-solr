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
package org.apache.solr.handler.sql;

import java.lang.reflect.Type;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;

/**
 * A filter that contains another {@link CalciteConnection} and
 * allows adding pre- post-method behaviors.
 */
class FilterCalciteConnection implements CalciteConnection {

  protected final CalciteConnection in;

  FilterCalciteConnection(CalciteConnection in) {
    this.in = in;
  }

  public CalciteConnection getDelegate() {
    return in;
  }

  @Override
  public SchemaPlus getRootSchema() {
    return in.getRootSchema();
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return in.getTypeFactory();
  }

  @Override
  public Properties getProperties() {
    return in.getProperties();
  }

  @Override
  public Statement createStatement() throws SQLException {
    return in.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return in.prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return in.prepareCall(sql);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return in.nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    in.setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return in.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    in.commit();
  }

  @Override
  public void rollback() throws SQLException {
    in.rollback();
  }

  @Override
  public void close() throws SQLException {
    in.close();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return in.isClosed();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return in.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    in.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return in.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    in.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    return in.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    in.setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return in.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return in.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    in.clearWarnings();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return in.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return in.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return in.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return in.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    in.setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    in.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return in.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return in.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return in.setSavepoint(name);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    in.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    in.releaseSavepoint(savepoint);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return in.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return in.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return in.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return in.prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return in.prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return in.prepareStatement(sql, columnNames);
  }

  @Override
  public Clob createClob() throws SQLException {
    return in.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return in.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return in.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return in.createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return in.isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    in.setClientInfo(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    in.setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return in.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return in.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return in.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return in.createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    in.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return in.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    in.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    in.setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return in.getNetworkTimeout();
  }

  @Override
  public CalciteConnectionConfig config() {
    return in.config();
  }

  @Override
  public CalcitePrepare.Context createPrepareContext() {
    return in.createPrepareContext();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return in.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return in.isWrapperFor(iface);
  }

  @Override
  public <T> Queryable<T> createQuery(Expression expression, Class<T> aClass) {
    return in.createQuery(expression, aClass);
  }

  @Override
  public <T> Queryable<T> createQuery(Expression expression, Type type) {
    return in.createQuery(expression, type);
  }

  @Override
  public <T> T execute(Expression expression, Class<T> aClass) {
    return in.execute(expression, aClass);
  }

  @Override
  public <T> T execute(Expression expression, Type type) {
    return in.execute(expression, type);
  }

  @Override
  public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
    return in.executeQuery(queryable);
  }
}
