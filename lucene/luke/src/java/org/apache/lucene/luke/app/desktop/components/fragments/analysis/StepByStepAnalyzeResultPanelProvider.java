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

package org.apache.lucene.luke.app.desktop.components.fragments.analysis;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.AbstractTableModel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.analysis.Analysis;

/** Provider of the Step by step analyze result panel */
public class StepByStepAnalyzeResultPanelProvider implements StepByStepAnalyzeResultPanelOperator {

  private final ComponentOperatorRegistry operatorRegistry;

  private final TokenAttributeDialogFactory tokenAttrDialogFactory;

  private final JTable charfilterTextsTable = new JTable();

  private final JTable charfilterTextsRowHeader = new JTable();

  private final JTable namedTokensTable = new JTable();

  private final JTable namedTokensRowHeader = new JTable();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private Analysis analysisModel;

  private Analysis.StepByStepResult result;

  public StepByStepAnalyzeResultPanelProvider(TokenAttributeDialogFactory tokenAttrDialogFactory) {
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    operatorRegistry.register(StepByStepAnalyzeResultPanelOperator.class, this);
    this.tokenAttrDialogFactory = tokenAttrDialogFactory;
  }

  public JPanel get() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel hint = new JPanel(new FlowLayout(FlowLayout.LEADING));
    hint.setOpaque(false);
    hint.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.hint.show_attributes_step_by_step")));
    panel.add(hint, BorderLayout.PAGE_START);

    TableUtils.setupTable(charfilterTextsRowHeader, ListSelectionModel.SINGLE_SELECTION, new RowHeaderTableModel(),
        null);
    TableUtils.setupTable(charfilterTextsTable, ListSelectionModel.SINGLE_SELECTION, new CharfilterTextTableModel(),
        null);

    TableUtils.setupTable(namedTokensRowHeader, ListSelectionModel.SINGLE_SELECTION, new RowHeaderTableModel(),
        null);
    TableUtils.setupTable(namedTokensTable, ListSelectionModel.SINGLE_SELECTION, new NamedTokensTableModel(),
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showAttributeValues(e);
          }
        });
    namedTokensTable.setColumnSelectionAllowed(true);
    JSplitPane inner = new JSplitPane(JSplitPane.VERTICAL_SPLIT, initResultScroll(charfilterTextsTable, charfilterTextsRowHeader), initResultScroll(namedTokensTable, namedTokensRowHeader));
    inner.setDividerLocation(60);

    panel.add(inner, BorderLayout.CENTER);
    return panel;
  }

  private JScrollPane initResultScroll(JTable table, JTable header) {
    JScrollPane scroll = new JScrollPane(table);
    scroll.setRowHeaderView(header);
    scroll.setCorner(JScrollPane.UPPER_LEFT_CORNER, header.getTableHeader());
    Dimension tsz = new Dimension(200, header.getPreferredSize().height);
    scroll.getRowHeader().setPreferredSize(tsz);
    return scroll;
  }


  @Override
  public void setAnalysisModel(Analysis analysisModel) {
    this.analysisModel = analysisModel;
  }

  @Override
  public void executeAnalysisStepByStep(String text) {
    result = analysisModel.analyzeStepByStep(text);
    RowHeaderTableModel charfilterTextsHeaderModel = new RowHeaderTableModel(result.getCharfilteredTexts());
    charfilterTextsRowHeader.setModel(charfilterTextsHeaderModel);
    charfilterTextsRowHeader.setShowGrid(true);

    CharfilterTextTableModel charfilterTextTableModel = new CharfilterTextTableModel(result.getCharfilteredTexts());
    charfilterTextsTable.setModel(charfilterTextTableModel);
    charfilterTextsTable.setShowGrid(true);

    RowHeaderTableModel namedTokensHeaderModel = new RowHeaderTableModel(result.getNamedTokens());
    namedTokensRowHeader.setModel(namedTokensHeaderModel);
    namedTokensRowHeader.setShowGrid(true);

    NamedTokensTableModel tableModel = new NamedTokensTableModel(result.getNamedTokens());
    namedTokensTable.setModel(tableModel);
    namedTokensTable.setShowGrid(true);
    for (int i = 0; i < tableModel.getColumnCount(); i++) {
      namedTokensTable.getColumnModel().getColumn(i).setPreferredWidth(tableModel.getColumnWidth(i));
    }
  }

  @Override
  public void clearTable() {
    TableUtils.setupTable(charfilterTextsRowHeader, ListSelectionModel.SINGLE_SELECTION, new RowHeaderTableModel(),
        null);
    TableUtils.setupTable(charfilterTextsTable, ListSelectionModel.SINGLE_SELECTION, new CharfilterTextTableModel(),
        null);

    TableUtils.setupTable(namedTokensRowHeader, ListSelectionModel.SINGLE_SELECTION, new RowHeaderTableModel(),
        null);
    TableUtils.setupTable(namedTokensTable, ListSelectionModel.SINGLE_SELECTION, new NamedTokensTableModel(),
        null);
  }

  private void showAttributeValues(int rowIndex, int columnIndex) {
    Analysis.NamedTokens namedTokens =
        this.result.getNamedTokens().get(rowIndex);
    List<Analysis.Token> tokens = namedTokens.getTokens();

    if (rowIndex <= tokens.size()) {
      String term = "\"" + tokens.get(columnIndex).getTerm() + "\" BY " + namedTokens.getName();
      List<Analysis.TokenAttribute> attributes = tokens.get(columnIndex).getAttributes();
      new DialogOpener<>(tokenAttrDialogFactory).open("Token Attributes", 650, 400,
          factory -> {
            factory.setTerm(term);
            factory.setAttributes(attributes);
          });
    }
  }

  private class ListenerFunctions {
    void showAttributeValues(MouseEvent e) {
      if (e.getClickCount() != 2 || e.isConsumed()) {
        return;
      }
      int rowIndex = namedTokensTable.rowAtPoint(e.getPoint());
      int columnIndex = namedTokensTable.columnAtPoint(e.getPoint());
      if (rowIndex < 0 || rowIndex >= namedTokensTable.getRowCount()) {
        return;
      } else if (columnIndex < 0 || columnIndex >= namedTokensTable.getColumnCount()) {
        return;
      }
      StepByStepAnalyzeResultPanelProvider.this.showAttributeValues(rowIndex, columnIndex);
    }
  }

  /** Table model for row header (display charfilter/tokenizer/filter name)  */
  private static class RowHeaderTableModel extends TableModelBase<RowHeaderTableModel.Column> {

    enum Column implements TableColumnInfo {
      NAME("Name", 0, String.class, 200);

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

    RowHeaderTableModel() {
      super();
    }

    RowHeaderTableModel(List<? extends Analysis.NamedObject> namedObjects) {
      super(namedObjects.size());
      for (int i = 0; i < namedObjects.size(); i++) {
        data[i][0] = shortenName(namedObjects.get(i).getName());
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  /** Table model for charfilter result */
  private static class CharfilterTextTableModel extends TableModelBase<CharfilterTextTableModel.Column> {

    enum Column implements TableColumnInfo {
      TEXT("Text", 0, String.class, 1000);

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

    CharfilterTextTableModel() {
      super();
    }

    CharfilterTextTableModel(List<Analysis.CharfilteredText> charfilteredTexts) {
      super(charfilteredTexts.size());
      for (int i = 0; i < charfilteredTexts.size(); i++) {
        data[i][Column.TEXT.getIndex()] = charfilteredTexts.get(i).getText();
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  /** Table model for tokenizer/filter result */
  private static class NamedTokensTableModel extends AbstractTableModel {

    class Column implements TableColumnInfo {

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

    private final Map<Integer, Column> columnMap = new TreeMap<>();

    private final Object[][] data;


    NamedTokensTableModel() {
      this.data = new Object[0][0];
    }

    // Currently this only show each tokenizer/filters result independently,
    // so the result doesn't show deletion/separation by next filter,
    // e.g. "library" by WordDelimiterFilter is different position between other output.
    NamedTokensTableModel(List<Analysis.NamedTokens> namedTokens) {
      int maxColumnSize = 0;
      Analysis.NamedTokens namedToken;
      for (Analysis.NamedTokens tokens : namedTokens) {
        namedToken = tokens;
        if (maxColumnSize < namedToken.getTokens().size()) {
          maxColumnSize = namedToken.getTokens().size();
        }
      }
      int rowSize = namedTokens.size();
      this.data = new Object[rowSize][maxColumnSize];

      for (int i = 0; i < namedTokens.size(); i++) {
        namedToken = namedTokens.get(i);
        data[i][0] = shortenName(namedToken.getName());
        for (int j = 0; j < namedToken.getTokens().size(); j++) {
          Analysis.Token token = namedToken.getTokens().get(j);
          data[i][j] = token.getTerm();
          if (maxColumnSize == namedToken.getTokens().size()) {
            columnMap.put(j, new Column(String.valueOf(j), j, String.class, 200));
          }
        }
      }
    }

    @Override
    public int getRowCount() {
      return data.length;
    }

    @Override
    public int getColumnCount() {
      return columnMap.size();
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

    public int getColumnWidth(int columnIndex) {
      return columnMap.get(columnIndex).getColumnWidth();
    }
  }

  private static String shortenName(String name) {
    return name.substring(name.lastIndexOf('.') + 1);
  }

}
