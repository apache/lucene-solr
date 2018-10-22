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
package org.apache.solr.handler;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.JDBCStream;

/**
 * Used with o.a.s.Handler.SQLHandler.
 * 
 * @lucene.internal
 * @since 7.0.0
 */
public class CalciteJDBCStream extends JDBCStream {
  private static final long serialVersionUID = 1L;

  public CalciteJDBCStream(String connectionUrl, String sqlQuery, StreamComparator definedSort,
      Properties connectionProperties, String driverClassName) throws IOException {
    super(connectionUrl, sqlQuery, definedSort, connectionProperties, driverClassName);
  }

  @Override
  protected ResultSetValueSelector determineValueSelector(int columnIdx, ResultSetMetaData metadata)
      throws SQLException {
    ResultSetValueSelector valueSelector = super.determineValueSelector(columnIdx, metadata);
    if (valueSelector == null) {
      final int columnNumber = columnIdx + 1;
      final String columnName = metadata.getColumnLabel(columnNumber);
      final String className = metadata.getColumnClassName(columnNumber);
      if (Array.class.getName().equals(className)) {
        valueSelector = new ResultSetValueSelector() {
          @Override
          public Object selectValue(ResultSet resultSet) throws SQLException {
            Object o = resultSet.getObject(columnNumber);
            if (resultSet.wasNull()) {
              return null;
            }
            if (o instanceof Array) {
              Array array = (Array) o;
              return array.getArray();
            } else {
              return o;
            }
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
}
