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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;

class StatementImpl implements Statement {

  private CloudSolrClient client;
  private SolrClientCache sqlSolrClientCache;
  private String collection;
  private Properties properties;
  private SolrStream solrStream;
  private boolean closed;

  StatementImpl(CloudSolrClient client, String collection, Properties properties, SolrClientCache sqlSolrClientCache) {
    this.client = client;
    this.collection = collection;
    this.properties = properties;
    this.sqlSolrClientCache = sqlSolrClientCache;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {

    try {
      closed = false;  // If closed reopen so Statement can be reused.
      this.solrStream = constructStream(sql);
      StreamContext context = new StreamContext();
      context.setSolrClientCache(sqlSolrClientCache);
      this.solrStream.setStreamContext(context);
      this.solrStream.open();
      return new ResultSetImpl(this.solrStream);
    } catch(Exception e) {
      throw new SQLException(e);
    }
  }

  protected SolrStream constructStream(String sql) throws IOException {

    try {
      ZkStateReader zkStateReader = client.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);

      if(slices == null) {
        throw new Exception("Collection not found:"+this.collection);
      }

      Map params = new HashMap();

      List<Replica> shuffler = new ArrayList();
      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          shuffler.add(replica);
        }
      }

      Collections.shuffle(shuffler, new Random());

      params.put(CommonParams.QT, "/sql");
      params.put("stmt", sql);
      params.putAll(properties);

      Replica rep = shuffler.get(0);
      ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
      String url = zkProps.getCoreUrl();
      return new SolrStream(url, params);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return 0;
  }

  @Override
  public void close() throws SQLException {

    if(closed) {
      return;
    }

    try {
      this.solrStream.close();
      this.closed = true;
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancel() throws SQLException {
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
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
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