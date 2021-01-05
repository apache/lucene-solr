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

package org.apache.lucene.luke.app.desktop.components.fragments.search;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.TableModelEvent;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;

/** Provider of the FieldValues pane */
public final class FieldValuesPaneProvider implements FieldValuesTabOperator {

  private final JCheckBox loadAllCB = new JCheckBox();

  private final JTable fieldsTable = new JTable();

  private ListenerFunctions listners = new ListenerFunctions();

  public FieldValuesPaneProvider() {
    ComponentOperatorRegistry.getInstance().register(FieldValuesTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    panel.add(initFieldsConfigPanel());

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initFieldsConfigPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel header = new JPanel(new GridLayout(1, 2));
    header.setOpaque(false);
    header.setBorder(BorderFactory.createEmptyBorder(0, 0, 10, 0));
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_values.label.description")));
    loadAllCB.setText(MessageUtils.getLocalizedMessage("search_values.checkbox.load_all"));
    loadAllCB.setSelected(true);
    loadAllCB.addActionListener(listners::loadAllFields);
    loadAllCB.setOpaque(false);
    header.add(loadAllCB);
    panel.add(header, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        fieldsTable,
        ListSelectionModel.SINGLE_SELECTION,
        new FieldsTableModel(),
        null,
        FieldsTableModel.Column.LOAD.getColumnWidth());
    fieldsTable.setShowGrid(true);
    fieldsTable.setPreferredScrollableViewportSize(fieldsTable.getPreferredSize());
    panel.add(new JScrollPane(fieldsTable), BorderLayout.CENTER);

    return panel;
  }

  @Override
  public void setFields(Collection<String> fields) {
    fieldsTable.setModel(new FieldsTableModel(fields));
    fieldsTable
        .getColumnModel()
        .getColumn(FieldsTableModel.Column.LOAD.getIndex())
        .setMinWidth(FieldsTableModel.Column.LOAD.getColumnWidth());
    fieldsTable
        .getColumnModel()
        .getColumn(FieldsTableModel.Column.LOAD.getIndex())
        .setMaxWidth(FieldsTableModel.Column.LOAD.getColumnWidth());
    fieldsTable.getModel().addTableModelListener(listners::tableDataChenged);
  }

  @Override
  public Set<String> getFieldsToLoad() {
    Set<String> fieldsToLoad = new HashSet<>();
    for (int row = 0; row < fieldsTable.getRowCount(); row++) {
      boolean loaded =
          (boolean) fieldsTable.getValueAt(row, FieldsTableModel.Column.LOAD.getIndex());
      if (loaded) {
        fieldsToLoad.add(
            (String) fieldsTable.getValueAt(row, FieldsTableModel.Column.FIELD.getIndex()));
      }
    }
    return fieldsToLoad;
  }

  class ListenerFunctions {

    void loadAllFields(ActionEvent e) {
      for (int i = 0; i < fieldsTable.getModel().getRowCount(); i++) {
        if (loadAllCB.isSelected()) {
          fieldsTable.setValueAt(true, i, FieldsTableModel.Column.LOAD.getIndex());
        } else {
          fieldsTable.setValueAt(false, i, FieldsTableModel.Column.LOAD.getIndex());
        }
      }
    }

    void tableDataChenged(TableModelEvent e) {
      int row = e.getFirstRow();
      int col = e.getColumn();
      if (col == FieldsTableModel.Column.LOAD.getIndex()) {
        boolean isLoad = (boolean) fieldsTable.getModel().getValueAt(row, col);
        if (!isLoad) {
          loadAllCB.setSelected(false);
        }
      }
    }
  }

  static final class FieldsTableModel extends TableModelBase<FieldsTableModel.Column> {

    enum Column implements TableColumnInfo {
      LOAD("Load", 0, Boolean.class, 50),
      FIELD("Field", 1, String.class, Integer.MAX_VALUE);

      private final String colName;
      private final int index;
      private final Class<?> type;
      private final int width;

      Column(String colName, int index, Class<?> type, int width) {
        this.colName = colName;
        this.index = index;
        this.type = type;
        this.width = width;
      }

      @Override
      public String getColName() {
        return colName;
      }

      @Override
      public int getIndex() {
        return index;
      }

      @Override
      public Class<?> getType() {
        return type;
      }

      @Override
      public int getColumnWidth() {
        return width;
      }
    }

    FieldsTableModel() {
      super();
    }

    FieldsTableModel(Collection<String> fields) {
      super(fields.size());
      int i = 0;
      for (String field : fields) {
        data[i][Column.LOAD.getIndex()] = true;
        data[i][Column.FIELD.getIndex()] = field;
        i++;
      }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return columnIndex == Column.LOAD.getIndex();
    }

    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
      data[rowIndex][columnIndex] = value;
      fireTableCellUpdated(rowIndex, columnIndex);
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }
}
