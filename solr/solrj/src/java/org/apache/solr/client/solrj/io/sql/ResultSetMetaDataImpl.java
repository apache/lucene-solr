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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;

class ResultSetMetaDataImpl implements ResultSetMetaData {
  private final ResultSetImpl resultSet;
  private final Tuple metadataTuple;
  private final Tuple firstTuple;

  ResultSetMetaDataImpl(ResultSetImpl resultSet) {
    this.resultSet = resultSet;
    this.metadataTuple = this.resultSet.getMetadataTuple();
    this.firstTuple = this.resultSet.getFirstTuple();
  }

  @SuppressWarnings({"rawtypes"})
  private Class getColumnClass(int column) throws SQLException {
    Object o = this.firstTuple.get(this.getColumnLabel(column));
    if(o == null) {
      return String.class; //Nulls will only be present with Strings.
    } else {
      return o.getClass();
    }
  }

  @Override
  public int getColumnCount() throws SQLException {
    List<String> fields = metadataTuple.getStrings("fields");
    if(fields == null) {
      throw new SQLException("Unable to determine fields for column count");
    }
    return fields.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return 0;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return this.getColumnLabel(column).length();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    @SuppressWarnings({"unchecked"})
    Map<String, String> aliases = (Map<String, String>) metadataTuple.get("aliases");
    return aliases.get(this.getColumnName(column));
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    List<String> columns = metadataTuple.getStrings("fields");
    if(column < 1 || column > columns.size()) {
      throw new SQLException("Column index " + column + " is not valid");
    }
    return columns.get(column - 1);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return null;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    switch (getColumnTypeName(column)) {
      case "String":
        return Types.VARCHAR;
      case "Integer":
        return Types.INTEGER;
      case "Long":
        return Types.DOUBLE;
      case "Double":
        return Types.DOUBLE;
      default:
        return Types.JAVA_OBJECT;
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return this.getColumnClass(column).getSimpleName();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return this.getColumnClass(column).getTypeName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
