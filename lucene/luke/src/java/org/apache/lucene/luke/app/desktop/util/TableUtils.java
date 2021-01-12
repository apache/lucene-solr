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

package org.apache.lucene.luke.app.desktop.util;

import java.awt.Color;
import java.awt.event.MouseListener;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;

/** Table utilities */
public class TableUtils {

  public static void setupTable(
      JTable table,
      int selectionModel,
      TableModel model,
      MouseListener mouseListener,
      int... colWidth) {
    table.setFillsViewportHeight(true);
    table.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    table.setRowHeight(StyleConstants.TABLE_ROW_HEIGHT_DEFAULT);
    table.setShowHorizontalLines(true);
    table.setShowVerticalLines(false);
    table.setGridColor(Color.lightGray);
    table.getColumnModel().setColumnMargin(StyleConstants.TABLE_COLUMN_MARGIN_DEFAULT);
    table.setRowMargin(StyleConstants.TABLE_ROW_MARGIN_DEFAULT);
    table.setSelectionMode(selectionModel);
    if (model != null) {
      table.setModel(model);
    } else {
      table.setModel(new DefaultTableModel());
    }
    if (mouseListener != null) {
      table.removeMouseListener(mouseListener);
      table.addMouseListener(mouseListener);
    }
    for (int i = 0; i < colWidth.length; i++) {
      table.getColumnModel().getColumn(i).setMinWidth(colWidth[i]);
      table.getColumnModel().getColumn(i).setMaxWidth(colWidth[i]);
    }
  }

  public static void setEnabled(JTable table, boolean enabled) {
    table.setEnabled(enabled);
    if (enabled) {
      table.setRowSelectionAllowed(true);
      table.setForeground(Color.black);
      table.setBackground(Color.white);
    } else {
      table.setRowSelectionAllowed(false);
      table.setForeground(Color.gray);
      table.setBackground(Color.lightGray);
    }
  }

  public static <T extends TableColumnInfo> String[] columnNames(T[] columns) {
    return columnMap(columns).entrySet().stream()
        .map(e -> e.getValue().getColName())
        .toArray(String[]::new);
  }

  public static <T extends TableColumnInfo> TreeMap<Integer, T> columnMap(T[] columns) {
    return Arrays.stream(columns)
        .collect(
            Collectors.toMap(T::getIndex, UnaryOperator.identity(), (e1, e2) -> e1, TreeMap::new));
  }

  private TableUtils() {}
}
