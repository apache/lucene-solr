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

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.AnalysisChainDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.TokenAttributeDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.AddDocumentDialogOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.PresetAnalyzerPanelOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.PresetAnalyzerPanelProvider;
import org.apache.lucene.luke.app.desktop.components.fragments.search.AnalyzerTabOperator;
import org.apache.lucene.luke.app.desktop.components.fragments.search.MLTTabOperator;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.analysis.Analysis;
import org.apache.lucene.luke.models.analysis.AnalysisFactory;
import org.apache.lucene.luke.models.analysis.CustomAnalyzerConfig;
import org.apache.lucene.util.NamedThreadFactory;

/** Provider of the Analysis panel */
public final class AnalysisPanelProvider implements AnalysisTabOperator {

  private static final String TYPE_PRESET = "preset";

  private static final String TYPE_CUSTOM = "custom";

  private final ComponentOperatorRegistry operatorRegistry;

  private final AnalysisChainDialogFactory analysisChainDialogFactory;

  private final TokenAttributeDialogFactory tokenAttrDialogFactory;

  private final MessageBroker messageBroker;

  private final JPanel mainPanel = new JPanel();

  private final JPanel preset;

  private final JPanel custom;

  private final JRadioButton presetRB = new JRadioButton();

  private final JRadioButton customRB = new JRadioButton();

  private final JLabel analyzerNameLbl = new JLabel();

  private final JLabel showChainLbl = new JLabel();

  private final JTextArea inputArea = new JTextArea();

  private final JTable tokensTable = new JTable();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private List<Analysis.Token> tokens;

  private Analysis analysisModel;

  public AnalysisPanelProvider() throws IOException {
    this.preset = new PresetAnalyzerPanelProvider().get();
    this.custom = new CustomAnalyzerPanelProvider().get();

    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.analysisChainDialogFactory = AnalysisChainDialogFactory.getInstance();
    this.tokenAttrDialogFactory = TokenAttributeDialogFactory.getInstance();
    this.messageBroker = MessageBroker.getInstance();

    this.analysisModel = new AnalysisFactory().newInstance();
    analysisModel.createAnalyzerFromClassName(StandardAnalyzer.class.getName());

    operatorRegistry.register(AnalysisTabOperator.class, this);

    operatorRegistry.get(PresetAnalyzerPanelOperator.class).ifPresent(operator -> {
      // Scanning all Analyzer types will take time...
      ExecutorService executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory("load-preset-analyzer-types"));
      executorService.execute(() -> {
        operator.setPresetAnalyzers(analysisModel.getPresetAnalyzerTypes());
        operator.setSelectedAnalyzer(analysisModel.currentAnalyzer().getClass());
      });
      executorService.shutdown();
    });
  }

  public JPanel get() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createLineBorder(Color.gray));

    JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, initUpperPanel(), initLowerPanel());
    splitPane.setOpaque(false);
    splitPane.setDividerLocation(320);
    panel.add(splitPane);

    return panel;
  }

  private JPanel initUpperPanel() {
    mainPanel.setOpaque(false);
    mainPanel.setLayout(new BorderLayout());
    mainPanel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    mainPanel.add(initSwitcherPanel(), BorderLayout.PAGE_START);
    mainPanel.add(preset, BorderLayout.CENTER);

    return mainPanel;
  }

  private JPanel initSwitcherPanel() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setOpaque(false);

    presetRB.setText(MessageUtils.getLocalizedMessage("analysis.radio.preset"));
    presetRB.setActionCommand(TYPE_PRESET);
    presetRB.addActionListener(listeners::toggleMainPanel);
    presetRB.setOpaque(false);
    presetRB.setSelected(true);

    customRB.setText(MessageUtils.getLocalizedMessage("analysis.radio.custom"));
    customRB.setActionCommand(TYPE_CUSTOM);
    customRB.addActionListener(listeners::toggleMainPanel);
    customRB.setOpaque(false);
    customRB.setSelected(false);

    ButtonGroup group = new ButtonGroup();
    group.add(presetRB);
    group.add(customRB);

    panel.add(presetRB);
    panel.add(customRB);

    return panel;
  }

  private JPanel initLowerPanel() {
    JPanel inner1 = new JPanel(new BorderLayout());
    inner1.setOpaque(false);

    JPanel analyzerName = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    analyzerName.setOpaque(false);
    analyzerName.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.label.selected_analyzer")));
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    analyzerName.add(analyzerNameLbl);
    showChainLbl.setText(MessageUtils.getLocalizedMessage("analysis.label.show_chain"));
    showChainLbl.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(MouseEvent e) {
        listeners.showAnalysisChain(e);
      }
    });
    showChainLbl.setVisible(analysisModel.currentAnalyzer() instanceof CustomAnalyzer);
    analyzerName.add(FontUtils.toLinkText(showChainLbl));
    inner1.add(analyzerName, BorderLayout.PAGE_START);

    JPanel input = new JPanel(new FlowLayout(FlowLayout.LEADING, 5, 2));
    input.setOpaque(false);
    inputArea.setRows(3);
    inputArea.setColumns(50);
    inputArea.setLineWrap(true);
    inputArea.setWrapStyleWord(true);
    inputArea.setText(MessageUtils.getLocalizedMessage("analysis.textarea.prompt"));
    input.add(new JScrollPane(inputArea));

    JButton executeBtn = new JButton(FontUtils.elegantIconHtml("&#xe007;", MessageUtils.getLocalizedMessage("analysis.button.test")));
    executeBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    executeBtn.setMargin(new Insets(3, 3, 3, 3));
    executeBtn.addActionListener(listeners::executeAnalysis);
    input.add(executeBtn);

    JButton clearBtn = new JButton(MessageUtils.getLocalizedMessage("button.clear"));
    clearBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    clearBtn.setMargin(new Insets(5, 5, 5, 5));
    clearBtn.addActionListener(e -> {
      inputArea.setText("");
      TableUtils.setupTable(tokensTable, ListSelectionModel.SINGLE_SELECTION, new TokensTableModel(),
          null,
          TokensTableModel.Column.TERM.getColumnWidth(),
          TokensTableModel.Column.ATTR.getColumnWidth());
    });
    input.add(clearBtn);

    inner1.add(input, BorderLayout.CENTER);

    JPanel inner2 = new JPanel(new BorderLayout());
    inner2.setOpaque(false);

    JPanel hint = new JPanel(new FlowLayout(FlowLayout.LEADING));
    hint.setOpaque(false);
    hint.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.hint.show_attributes")));
    inner2.add(hint, BorderLayout.PAGE_START);


    TableUtils.setupTable(tokensTable, ListSelectionModel.SINGLE_SELECTION, new TokensTableModel(),
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showAttributeValues(e);
          }
        },
        TokensTableModel.Column.TERM.getColumnWidth(),
        TokensTableModel.Column.ATTR.getColumnWidth());
    inner2.add(new JScrollPane(tokensTable), BorderLayout.CENTER);

    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));
    panel.add(inner1, BorderLayout.PAGE_START);
    panel.add(inner2, BorderLayout.CENTER);

    return panel;
  }

  // control methods

  void toggleMainPanel(String command) {
    if (command.equalsIgnoreCase(TYPE_PRESET)) {
      mainPanel.remove(custom);
      mainPanel.add(preset, BorderLayout.CENTER);

      operatorRegistry.get(PresetAnalyzerPanelOperator.class).ifPresent(operator -> {
        operator.setPresetAnalyzers(analysisModel.getPresetAnalyzerTypes());
        operator.setSelectedAnalyzer(analysisModel.currentAnalyzer().getClass());
      });

    } else if (command.equalsIgnoreCase(TYPE_CUSTOM)) {
      mainPanel.remove(preset);
      mainPanel.add(custom, BorderLayout.CENTER);

      operatorRegistry.get(CustomAnalyzerPanelOperator.class).ifPresent(operator -> {
        operator.setAnalysisModel(analysisModel);
        operator.resetAnalysisComponents();
      });
    }
    mainPanel.setVisible(false);
    mainPanel.setVisible(true);
  }

  void executeAnalysis() {
    String text = inputArea.getText();
    if (Objects.isNull(text) || text.isEmpty()) {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("analysis.message.empry_input"));
    }

    tokens = analysisModel.analyze(text);
    tokensTable.setModel(new TokensTableModel(tokens));
    tokensTable.setShowGrid(true);
    tokensTable.getColumnModel().getColumn(TokensTableModel.Column.TERM.getIndex()).setPreferredWidth(TokensTableModel.Column.TERM.getColumnWidth());
    tokensTable.getColumnModel().getColumn(TokensTableModel.Column.ATTR.getIndex()).setPreferredWidth(TokensTableModel.Column.ATTR.getColumnWidth());
  }

  void showAnalysisChainDialog() {
    if (getCurrentAnalyzer() instanceof CustomAnalyzer) {
      CustomAnalyzer analyzer = (CustomAnalyzer) getCurrentAnalyzer();
      new DialogOpener<>(analysisChainDialogFactory).open("Analysis chain", 600, 320,
          (factory) -> {
            factory.setAnalyzer(analyzer);
          });
    }
  }

  void showAttributeValues(int selectedIndex) {
    String term = tokens.get(selectedIndex).getTerm();
    List<Analysis.TokenAttribute> attributes = tokens.get(selectedIndex).getAttributes();
    new DialogOpener<>(tokenAttrDialogFactory).open("Token Attributes", 650, 400,
        factory -> {
          factory.setTerm(term);
          factory.setAttributes(attributes);
        });
  }


  @Override
  public void setAnalyzerByType(String analyzerType) {
    analysisModel.createAnalyzerFromClassName(analyzerType);
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    showChainLbl.setVisible(false);
    operatorRegistry.get(AnalyzerTabOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry.get(MLTTabOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry.get(AddDocumentDialogOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
  }

  @Override
  public void setAnalyzerByCustomConfiguration(CustomAnalyzerConfig config) {
    analysisModel.buildCustomAnalyzer(config);
    analyzerNameLbl.setText(analysisModel.currentAnalyzer().getClass().getName());
    showChainLbl.setVisible(true);
    operatorRegistry.get(AnalyzerTabOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry.get(MLTTabOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
    operatorRegistry.get(AddDocumentDialogOperator.class).ifPresent(operator ->
        operator.setAnalyzer(analysisModel.currentAnalyzer()));
  }

  @Override
  public Analyzer getCurrentAnalyzer() {
    return analysisModel.currentAnalyzer();
  }

  private class ListenerFunctions {

    void toggleMainPanel(ActionEvent e) {
      AnalysisPanelProvider.this.toggleMainPanel(e.getActionCommand());
    }

    void showAnalysisChain(MouseEvent e) {
      AnalysisPanelProvider.this.showAnalysisChainDialog();
    }

    void executeAnalysis(ActionEvent e) {
      AnalysisPanelProvider.this.executeAnalysis();
    }

    void showAttributeValues(MouseEvent e) {
      if (e.getClickCount() != 2 || e.isConsumed()) {
        return;
      }
      int selectedIndex = tokensTable.rowAtPoint(e.getPoint());
      if (selectedIndex < 0 || selectedIndex >= tokensTable.getRowCount()) {
        return;
      }
      AnalysisPanelProvider.this.showAttributeValues(selectedIndex);
    }

  }

  static final class TokensTableModel extends TableModelBase<TokensTableModel.Column> {

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

