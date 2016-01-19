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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;

class ConnectionImpl implements Connection {

  private final String url;
  private final SolrClientCache solrClientCache = new SolrClientCache();
  private final CloudSolrClient client;
  private final String collection;
  private final Properties properties;
  private boolean closed;

  ConnectionImpl(String url, String zkHost, String collection, Properties properties) {
    this.url = url;
    this.client = solrClientCache.getCloudSolrClient(zkHost);
    this.collection = collection;
    this.properties = properties;
  }

  String getUrl() {
    return url;
  }

  CloudSolrClient getClient() {
    return client;
  }

  Properties getProperties() {
    return properties;
  }

  SolrClientCache getSolrClientCache() {
    return this.solrClientCache;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new StatementImpl(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return null;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {

  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {

  }

  @Override
  public void close() throws SQLException {
    if(closed) {
      return;
    }
    try {
      this.solrClientCache.close();
      this.closed = true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new DatabaseMetaDataImpl(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCatalog() throws SQLException {
    return this.collection;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSchema() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }
}