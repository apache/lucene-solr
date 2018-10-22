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
package org.apache.solr.client.solrj.io.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.PushBackStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;

class ResultSetImpl implements ResultSet {
  private final StatementImpl statement;
  private final PushBackStream solrStream;
  private final ResultSetMetaData resultSetMetaData;
  private final Tuple metadataTuple;
  private final Tuple firstTuple;
  private Tuple tuple;
  private boolean done;
  private boolean closed;
  private SQLWarning currentWarning;
  private boolean wasLastValueNull;

  ResultSetImpl(StatementImpl statement, SolrStream solrStream) throws SQLException {
    this.statement = statement;

    try {
      this.solrStream = new PushBackStream(solrStream);

      StreamContext context = new StreamContext();
      context.setSolrClientCache(((ConnectionImpl)this.statement.getConnection()).getSolrClientCache());
      this.solrStream.setStreamContext(context);

      this.solrStream.open();

      this.metadataTuple = this.solrStream.read();

      Object isMetadata = this.metadataTuple.get("isMetadata");
      if(isMetadata == null || !isMetadata.equals(true)) {
        throw new RuntimeException("First tuple is not a metadata tuple");
      }

      this.firstTuple = this.solrStream.read();
      this.solrStream.pushBack(firstTuple);
    } catch (IOException e) {
      throw new SQLException(e);
    }

    this.resultSetMetaData = new ResultSetMetaDataImpl(this);
  }

  Tuple getMetadataTuple() {
    return this.metadataTuple;
  }

  Tuple getFirstTuple() {
    return this.firstTuple;
  }

  private void checkClosed() throws SQLException {
    if(isClosed()) {
      throw new SQLException("ResultSet is closed.");
    }
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    try {
      if(done) {
        return false;
      }

      tuple = this.solrStream.read();
      if(tuple.EOF) {
        done = true;
        return false;
      } else {
        return true;
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    this.done = this.closed = true;

    try {
      this.solrStream.close();
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    return this.wasLastValueNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return this.getString(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return this.getBoolean(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return this.getByte(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return this.getShort(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return this.getInt(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return this.getLong(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return this.getFloat(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return this.getDouble(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  @SuppressForbidden(reason = "Implements deprecated method")
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return this.getBigDecimal(this.resultSetMetaData.getColumnLabel(columnIndex), scale);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return this.getBytes(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return this.getDate(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return this.getTime(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return this.getTimestamp(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    String value = tuple.getString(columnLabel);
    if(value.equals(String.valueOf((Object)null))) {
      this.wasLastValueNull = true;
      return null;
    }
    return value;
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return false;
    }
    return (boolean)value;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0;
    } else {
      return number.byteValue();
    }
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0;
    } else {
      return number.shortValue();
    }
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0;
    } else {
      return number.intValue();
    }
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0L;
    } else {
      return number.longValue();
    }
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0.0F;
    } else {
      return number.floatValue();
    }
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Number number = (Number)getObject(columnLabel);
    if(number == null) {
      this.wasLastValueNull = true;
      return 0.0D;
    } else {
      return number.doubleValue();
    }
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return (byte[])value;
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return (Date)value;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return (Time)value;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return (Timestamp)value;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if(isClosed()) {
      throw new SQLException("Statement is closed.");
    }

    return this.currentWarning;
  }

  @Override
  public void clearWarnings() throws SQLException {
    if(isClosed()) {
      throw new SQLException("Statement is closed.");
    }

    this.currentWarning = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();

    return this.resultSetMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return this.getObject(this.resultSetMetaData.getColumnLabel(columnIndex));
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = this.tuple.get(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return value;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    this.wasLastValueNull = false;
    checkClosed();

    Object value = this.getObject(columnLabel);
    if(value == null) {
      this.wasLastValueNull = true;
      return null;
    }
    return (BigDecimal)value;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();

    throw new SQLException("beforeFirst() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public void afterLast() throws SQLException {
    checkClosed();

    throw new SQLException("afterLast() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public boolean first() throws SQLException {
    checkClosed();

    throw new SQLException("first() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public boolean last() throws SQLException {
    checkClosed();

    throw new SQLException("last() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkClosed();

    throw new SQLException("absolute() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClosed();

    throw new SQLException("relative() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public boolean previous() throws SQLException {
    checkClosed();

    throw new SQLException("previous() not supported on ResultSet with type TYPE_FORWARD_ONLY");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();

    if(direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Direction must be FETCH_FORWARD since ResultSet " +
          "type is TYPE_FORWARD_ONLY");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();

    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClosed();

    if(rows < 0) {
      throw new SQLException("Rows must be >= 0");
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClosed();

    return 0;
  }

  @Override
  public int getType() throws SQLException {
    checkClosed();

    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    checkClosed();

    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();

  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
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