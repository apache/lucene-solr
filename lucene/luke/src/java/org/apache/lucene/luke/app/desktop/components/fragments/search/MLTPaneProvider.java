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
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.TableModelEvent;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TabSwitcherProxy;
import org.apache.lucene.luke.app.desktop.components.TabbedPaneProvider;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.components.fragments.search.FieldValuesPaneProvider.FieldsTableModel;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.search.MLTConfig;

/** Provider of the MLT pane */
public final class MLTPaneProvider implements MLTTabOperator {

  private final JLabel analyzerLbl = new JLabel(StandardAnalyzer.class.getName());

  private final JFormattedTextField maxDocFreqFTF = new JFormattedTextField();

  private final JFormattedTextField minDocFreqFTF = new JFormattedTextField();

  private final JFormattedTextField minTermFreqFTF = new JFormattedTextField();

  private final JCheckBox loadAllCB = new JCheckBox();

  private final JTable fieldsTable = new JTable();

  private final TabSwitcherProxy tabSwitcher;

  private final ListenerFunctions listeners = new ListenerFunctions();

  private MLTConfig config = new MLTConfig.Builder().build();

  public MLTPaneProvider() {
    this.tabSwitcher = TabSwitcherProxy.getInstance();

    ComponentOperatorRegistry.getInstance().register(MLTTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    panel.add(initMltParamsPanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initAnalyzerNamePanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initFieldsSettingsPanel());

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initMltParamsPanel() {
    JPanel panel = new JPanel(new GridLayout(3, 1));
    panel.setOpaque(false);

    JPanel maxDocFreq = new JPanel(new FlowLayout(FlowLayout.LEADING));
    maxDocFreq.setOpaque(false);
    maxDocFreq.add(new JLabel(MessageUtils.getLocalizedMessage("search_mlt.label.max_doc_freq")));
    maxDocFreqFTF.setColumns(10);
    maxDocFreqFTF.setValue(config.getMaxDocFreq());
    maxDocFreq.add(maxDocFreqFTF);
    maxDocFreq.add(new JLabel(MessageUtils.getLocalizedMessage("label.int_required")));
    panel.add(maxDocFreq);

    JPanel minDocFreq = new JPanel(new FlowLayout(FlowLayout.LEADING));
    minDocFreq.setOpaque(false);
    minDocFreq.add(new JLabel(MessageUtils.getLocalizedMessage("search_mlt.label.min_doc_freq")));
    minDocFreqFTF.setColumns(5);
    minDocFreqFTF.setValue(config.getMinDocFreq());
    minDocFreq.add(minDocFreqFTF);

    minDocFreq.add(new JLabel(MessageUtils.getLocalizedMessage("label.int_required")));
    panel.add(minDocFreq);

    JPanel minTermFreq = new JPanel(new FlowLayout(FlowLayout.LEADING));
    minTermFreq.setOpaque(false);
    minTermFreq.add(new JLabel(MessageUtils.getLocalizedMessage("serach_mlt.label.min_term_freq")));
    minTermFreqFTF.setColumns(5);
    minTermFreqFTF.setValue(config.getMinTermFreq());
    minTermFreq.add(minTermFreqFTF);
    minTermFreq.add(new JLabel(MessageUtils.getLocalizedMessage("label.int_required")));
    panel.add(minTermFreq);

    return panel;
  }

  private JPanel initAnalyzerNamePanel() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setOpaque(false);

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("search_mlt.label.analyzer")));

    panel.add(analyzerLbl);

    JLabel changeLbl = new JLabel(MessageUtils.getLocalizedMessage("search_mlt.hyperlink.change"));
    changeLbl.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            tabSwitcher.switchTab(TabbedPaneProvider.Tab.ANALYZER);
          }
        });
    panel.add(FontUtils.toLinkText(changeLbl));

    return panel;
  }

  private JPanel initFieldsSettingsPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setPreferredSize(new Dimension(500, 300));
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    JPanel header = new JPanel(new GridLayout(2, 1));
    header.setOpaque(false);
    header.setBorder(BorderFactory.createEmptyBorder(0, 0, 10, 0));
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_mlt.label.description")));
    loadAllCB.setText(MessageUtils.getLocalizedMessage("search_mlt.checkbox.select_all"));
    loadAllCB.setSelected(true);
    loadAllCB.addActionListener(listeners::loadAllFields);
    loadAllCB.setOpaque(false);
    header.add(loadAllCB);
    panel.add(header, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        fieldsTable,
        ListSelectionModel.SINGLE_SELECTION,
        new MLTFieldsTableModel(),
        null,
        MLTFieldsTableModel.Column.SELECT.getColumnWidth());
    fieldsTable.setPreferredScrollableViewportSize(fieldsTable.getPreferredSize());
    panel.add(new JScrollPane(fieldsTable), BorderLayout.CENTER);

    return panel;
  }

  @Override
  public void setAnalyzer(Analyzer analyzer) {
    analyzerLbl.setText(analyzer.getClass().getName());
  }

  @Override
  public void setFields(Collection<String> fields) {
    fieldsTable.setModel(new MLTFieldsTableModel(fields));
    fieldsTable
        .getColumnModel()
        .getColumn(MLTFieldsTableModel.Column.SELECT.getIndex())
        .setMinWidth(MLTFieldsTableModel.Column.SELECT.getColumnWidth());
    fieldsTable
        .getColumnModel()
        .getColumn(MLTFieldsTableModel.Column.SELECT.getIndex())
        .setMaxWidth(MLTFieldsTableModel.Column.SELECT.getColumnWidth());
    fieldsTable.getModel().addTableModelListener(listeners::tableDataChenged);
  }

  @Override
  public MLTConfig getConfig() {
    List<String> fields = new ArrayList<>();
    for (int row = 0; row < fieldsTable.getRowCount(); row++) {
      boolean selected =
          (boolean) fieldsTable.getValueAt(row, MLTFieldsTableModel.Column.SELECT.getIndex());
      if (selected) {
        fields.add(
            (String) fieldsTable.getValueAt(row, MLTFieldsTableModel.Column.FIELD.getIndex()));
      }
    }

    return new MLTConfig.Builder()
        .fields(fields)
        .maxDocFreq((int) maxDocFreqFTF.getValue())
        .minDocFreq((int) minDocFreqFTF.getValue())
        .minTermFreq((int) minTermFreqFTF.getValue())
        .build();
  }

  private class ListenerFunctions {

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
      if (col == MLTFieldsTableModel.Column.SELECT.getIndex()) {
        boolean isLoad = (boolean) fieldsTable.getModel().getValueAt(row, col);
        if (!isLoad) {
          loadAllCB.setSelected(false);
        }
      }
    }
  }

  static final class MLTFieldsTableModel extends TableModelBase<MLTFieldsTableModel.Column> {

    enum Column implements TableColumnInfo {
      SELECT("Select", 0, Boolean.class, 50),
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

    MLTFieldsTableModel() {
      super();
    }

    MLTFieldsTableModel(Collection<String> fields) {
      super(fields.size());
      int i = 0;
      for (String field : fields) {
        data[i][Column.SELECT.getIndex()] = true;
        data[i][Column.FIELD.getIndex()] = field;
        i++;
      }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return columnIndex == Column.SELECT.getIndex();
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
