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
import javax.swing.JTable;
import javax.swing.ListSelectionModel;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.analysis.Analysis;

/** Provider of the simple analyze result panel */
public class SimpleAnalyzeResultPanelProvider implements SimpleAnalyzeResultPanelOperator {

  private final ComponentOperatorRegistry operatorRegistry;

  private final TokenAttributeDialogFactory tokenAttrDialogFactory;

  private final JTable tokensTable = new JTable();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private Analysis analysisModel;

  private List<Analysis.Token> tokens;

  public SimpleAnalyzeResultPanelProvider(TokenAttributeDialogFactory tokenAttrDialogFactory) {
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    operatorRegistry.register(SimpleAnalyzeResultPanelOperator.class, this);
    this.tokenAttrDialogFactory = tokenAttrDialogFactory;
  }

  public JPanel get() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel hint = new JPanel(new FlowLayout(FlowLayout.LEADING));
    hint.setOpaque(false);
    hint.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.hint.show_attributes")));
    panel.add(hint, BorderLayout.PAGE_START);

    TableUtils.setupTable(tokensTable, ListSelectionModel.SINGLE_SELECTION, new TokensTableModel(),
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showAttributeValues(e);
          }
        },
        TokensTableModel.Column.TERM.getColumnWidth(),
        TokensTableModel.Column.ATTR.getColumnWidth());
    panel.add(new JScrollPane(tokensTable), BorderLayout.CENTER);

    return panel;
  }

  @Override
  public void setAnalysisModel(Analysis analysisModel) {
    this.analysisModel = analysisModel;
  }

  @Override
  public void executeAnalysis(String text) {
    tokens = analysisModel.analyze(text);
    tokensTable.setModel(new TokensTableModel(tokens));
    tokensTable.setShowGrid(true);
    tokensTable.getColumnModel().getColumn(TokensTableModel.Column.TERM.getIndex())
        .setPreferredWidth(TokensTableModel.Column.TERM.getColumnWidth());
    tokensTable.getColumnModel().getColumn(TokensTableModel.Column.ATTR.getIndex())
        .setPreferredWidth(TokensTableModel.Column.ATTR.getColumnWidth());
  }

  @Override
  public void clearTable() {
    TableUtils.setupTable(tokensTable, ListSelectionModel.SINGLE_SELECTION, new TokensTableModel(),
        null,
        TokensTableModel.Column.TERM.getColumnWidth(),
        TokensTableModel.Column.ATTR.getColumnWidth());
  }

  private void showAttributeValues(int selectedIndex) {
    String term = tokens.get(selectedIndex).getTerm();
    List<Analysis.TokenAttribute> attributes = tokens.get(selectedIndex).getAttributes();
    new DialogOpener<>(tokenAttrDialogFactory).open("Token Attributes", 650, 400,
        factory -> {
          factory.setTerm(term);
          factory.setAttributes(attributes);
        });
  }

  private class ListenerFunctions {

    void showAttributeValues(MouseEvent e) {
      if (e.getClickCount() != 2 || e.isConsumed()) {
        return;
      }
      int selectedIndex = tokensTable.rowAtPoint(e.getPoint());
      if (selectedIndex < 0 || selectedIndex >= tokensTable.getRowCount()) {
        return;
      }
      SimpleAnalyzeResultPanelProvider.this.showAttributeValues(selectedIndex);
    }
  }

  /** Table model for simple result */
  private static class TokensTableModel extends TableModelBase<TokensTableModel.Column> {

    enum Column implements TableColumnInfo {
      TERM("Term", 0, String.class, 150),
      ATTR("Attributes", 1, String.class, 1000);

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

    TokensTableModel() {
      super();
    }

    TokensTableModel(List<Analysis.Token> tokens) {
      super(tokens.size());
      for (int i = 0; i < tokens.size(); i++) {
        Analysis.Token token = tokens.get(i);
        data[i][Column.TERM.getIndex()] = token.getTerm();
        List<String> attValues = token.getAttributes().stream()
            .flatMap(att -> att.getAttValues().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue()))
            .collect(Collectors.toList());
        data[i][Column.ATTR.getIndex()] = String.join(",", attValues);
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }
}
