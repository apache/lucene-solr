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

package org.apache.lucene.luke.app.desktop.components;

import java.util.Map;
import javax.swing.table.AbstractTableModel;
import org.apache.lucene.luke.app.desktop.util.TableUtils;

/**
 * Base table model that stores table's meta data and content. This also provides some default
 * implementation of the {@link javax.swing.table.TableModel} interface.
 */
public abstract class TableModelBase<T extends TableColumnInfo> extends AbstractTableModel {

  private final Map<Integer, T> columnMap = TableUtils.columnMap(columnInfos());

  private final String[] colNames = TableUtils.columnNames(columnInfos());

  protected final Object[][] data;

  protected TableModelBase() {
    this.data = new Object[0][colNames.length];
  }

  protected TableModelBase(int rows) {
    this.data = new Object[rows][colNames.length];
  }

  protected abstract T[] columnInfos();

  @Override
  public int getRowCount() {
    return data.length;
  }

  @Override
  public int getColumnCount() {
    return colNames.length;
  }

  @Override
  public String getColumnName(int colIndex) {
    if (columnMap.containsKey(colIndex)) {
      return columnMap.get(colIndex).getColName();
    }
    return "";
  }

  @Override
  public Class<?> getColumnClass(int colIndex) {
    if (columnMap.containsKey(colIndex)) {
      return columnMap.get(colIndex).getType();
    }
    return Object.class;
  }

  @Override
  public Object getValueAt(int rowIndex, int columnIndex) {
    return data[rowIndex][columnIndex];
  }
}
