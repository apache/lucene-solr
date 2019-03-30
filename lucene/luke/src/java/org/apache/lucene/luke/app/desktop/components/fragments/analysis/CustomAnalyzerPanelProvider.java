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

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.components.AnalysisTabOperator;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditFiltersDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditFiltersMode;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditParamsDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.analysis.EditParamsMode;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.ListUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.lang.Callable;
import org.apache.lucene.luke.models.analysis.Analysis;
import org.apache.lucene.luke.models.analysis.CustomAnalyzerConfig;
import org.apache.lucene.util.SuppressForbidden;

/** Provider of the custom analyzer panel */
public final class CustomAnalyzerPanelProvider implements CustomAnalyzerPanelOperator {

  private final ComponentOperatorRegistry operatorRegistry;

  private final EditParamsDialogFactory editParamsDialogFactory;

  private final EditFiltersDialogFactory editFiltersDialogFactory;

  private final MessageBroker messageBroker;

  private final JTextField confDirTF = new JTextField();

  private final JFileChooser fileChooser = new JFileChooser();

  private final JButton confDirBtn = new JButton();

  private final JButton buildBtn = new JButton();

  private final JLabel loadJarLbl = new JLabel();

  private final JList<String> selectedCfList = new JList<>(new String[]{});

  private final JButton cfEditBtn = new JButton();

  private final JComboBox<String> cfFactoryCombo = new JComboBox<>();

  private final JTextField selectedTokTF = new JTextField();

  private final JButton tokEditBtn = new JButton();

  private final JComboBox<String> tokFactoryCombo = new JComboBox<>();

  private final JList<String> selectedTfList = new JList<>(new String[]{});

  private final JButton tfEditBtn = new JButton();

  private final JComboBox<String> tfFactoryCombo = new JComboBox<>();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private final List<Map<String, String>> cfParamsList = new ArrayList<>();

  private final Map<String, String> tokParams = new HashMap<>();

  private final List<Map<String, String>> tfParamsList = new ArrayList<>();

  private JPanel containerPanel;

  private Analysis analysisModel;

  public CustomAnalyzerPanelProvider() throws IOException {
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.editParamsDialogFactory = EditParamsDialogFactory.getInstance();
    this.editFiltersDialogFactory = EditFiltersDialogFactory.getInstance();
    this.messageBroker = MessageBroker.getInstance();

    operatorRegistry.register(CustomAnalyzerPanelOperator.class, this);

    cfFactoryCombo.addActionListener(listeners::addCharFilter);
    tokFactoryCombo.addActionListener(listeners::setTokenizer);
    tfFactoryCombo.addActionListener(listeners::addTokenFilter);
  }

  public JPanel get() {
    if (containerPanel == null) {
      containerPanel = new JPanel();
      containerPanel.setOpaque(false);
      containerPanel.setLayout(new BorderLayout());
      containerPanel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

      containerPanel.add(initCustomAnalyzerHeaderPanel(), BorderLayout.PAGE_START);
      containerPanel.add(initCustomAnalyzerChainPanel(), BorderLayout.CENTER);
    }

    return containerPanel;
  }

  private JPanel initCustomAnalyzerHeaderPanel() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setOpaque(false);

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.label.config_dir")));
    confDirTF.setColumns(30);
    confDirTF.setPreferredSize(new Dimension(200, 30));
    panel.add(confDirTF);
    confDirBtn.setText(FontUtils.elegantIconHtml("&#x6e;", MessageUtils.getLocalizedMessage("analysis.button.browse")));
    confDirBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    confDirBtn.setMargin(new Insets(3, 3, 3, 3));
    confDirBtn.addActionListener(listeners::chooseConfigDir);
    panel.add(confDirBtn);
    buildBtn.setText(FontUtils.elegantIconHtml("&#xe102;", MessageUtils.getLocalizedMessage("analysis.button.build_analyzser")));
    buildBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    buildBtn.setMargin(new Insets(3, 3, 3, 3));
    buildBtn.addActionListener(listeners::buildAnalyzer);
    panel.add(buildBtn);
    loadJarLbl.setText(MessageUtils.getLocalizedMessage("analysis.hyperlink.load_jars"));
    loadJarLbl.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(MouseEvent e) {
        listeners.loadExternalJars(e);
      }
    });
    panel.add(FontUtils.toLinkText(loadJarLbl));

    return panel;
  }

  private JPanel initCustomAnalyzerChainPanel() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    panel.add(initCustomChainConfigPanel());

    return panel;
  }

  private JPanel initCustomChainConfigPanel() {
    JPanel panel = new JPanel(new GridBagLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createLineBorder(Color.black));

    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.HORIZONTAL;

    GridBagConstraints sepc = new GridBagConstraints();
    sepc.fill = GridBagConstraints.HORIZONTAL;
    sepc.weightx = 1.0;
    sepc.gridwidth = GridBagConstraints.REMAINDER;

    // char filters
    JLabel cfLbl = new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.charfilters"));
    cfLbl.setBorder(BorderFactory.createEmptyBorder(3, 10, 3, 3));
    c.gridx = 0;
    c.gridy = 0;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(cfLbl, c);

    c.gridx = 1;
    c.gridy = 0;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.selected")), c);

    selectedCfList.setVisibleRowCount(1);
    selectedCfList.setFont(new Font(selectedCfList.getFont().getFontName(), Font.PLAIN, 15));
    JScrollPane selectedPanel = new JScrollPane(selectedCfList);
    c.gridx = 2;
    c.gridy = 0;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(selectedPanel, c);

    cfEditBtn.setText(FontUtils.elegantIconHtml("&#x6a;", MessageUtils.getLocalizedMessage("analysis_custom.label.edit")));
    cfEditBtn.setMargin(new Insets(2, 4, 2, 4));
    cfEditBtn.setEnabled(false);
    cfEditBtn.addActionListener(listeners::editCharFilters);
    c.fill = GridBagConstraints.NONE;
    c.gridx = 7;
    c.gridy = 0;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(cfEditBtn, c);

    JLabel cfAddLabel = new JLabel(FontUtils.elegantIconHtml("&#x4c;", MessageUtils.getLocalizedMessage("analysis_custom.label.add")));
    cfAddLabel.setHorizontalAlignment(JLabel.LEFT);
    c.fill = GridBagConstraints.HORIZONTAL;
    c.gridx = 1;
    c.gridy = 2;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(cfAddLabel, c);

    c.gridx = 2;
    c.gridy = 2;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(cfFactoryCombo, c);

    // separator
    sepc.gridx = 0;
    sepc.gridy = 3;
    sepc.anchor = GridBagConstraints.LINE_START;
    panel.add(new JSeparator(JSeparator.HORIZONTAL), sepc);

    // tokenizer
    JLabel tokLabel = new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.tokenizer"));
    tokLabel.setBorder(BorderFactory.createEmptyBorder(3, 10, 3, 3));
    c.gridx = 0;
    c.gridy = 4;
    c.gridwidth = 1;
    c.gridheight = 2;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(tokLabel, c);

    c.gridx = 1;
    c.gridy = 4;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.selected")), c);

    selectedTokTF.setColumns(15);
    selectedTokTF.setFont(new Font(selectedTokTF.getFont().getFontName(), Font.PLAIN, 15));
    selectedTokTF.setBorder(BorderFactory.createLineBorder(Color.gray));
    selectedTokTF.setText("standard");
    selectedTokTF.setEditable(false);
    c.gridx = 2;
    c.gridy = 4;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(selectedTokTF, c);

    tokEditBtn.setText(FontUtils.elegantIconHtml("&#x6a;", MessageUtils.getLocalizedMessage("analysis_custom.label.edit")));
    tokEditBtn.setMargin(new Insets(2, 4, 2, 4));
    tokEditBtn.addActionListener(listeners::editTokenizer);
    c.fill = GridBagConstraints.NONE;
    c.gridx = 7;
    c.gridy = 4;
    c.gridwidth = 2;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(tokEditBtn, c);

    JLabel setTokLabel = new JLabel(FontUtils.elegantIconHtml("&#xe01e;", MessageUtils.getLocalizedMessage("analysis_custom.label.set")));
    setTokLabel.setHorizontalAlignment(JLabel.LEFT);
    c.fill = GridBagConstraints.HORIZONTAL;
    c.gridx = 1;
    c.gridy = 6;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(setTokLabel, c);

    c.gridx = 2;
    c.gridy = 6;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(tokFactoryCombo, c);

    // separator
    sepc.gridx = 0;
    sepc.gridy = 7;
    sepc.anchor = GridBagConstraints.LINE_START;
    panel.add(new JSeparator(JSeparator.HORIZONTAL), sepc);

    // token filters
    JLabel tfLbl = new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.tokenfilters"));
    tfLbl.setBorder(BorderFactory.createEmptyBorder(3, 10, 3, 3));
    c.gridx = 0;
    c.gridy = 8;
    c.gridwidth = 1;
    c.gridheight = 2;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(tfLbl, c);

    c.gridx = 1;
    c.gridy = 8;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis_custom.label.selected")), c);

    selectedTfList.setVisibleRowCount(1);
    selectedTfList.setFont(new Font(selectedTfList.getFont().getFontName(), Font.PLAIN, 15));
    JScrollPane selectedTfPanel = new JScrollPane(selectedTfList);
    c.gridx = 2;
    c.gridy = 8;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(selectedTfPanel, c);

    tfEditBtn.setText(FontUtils.elegantIconHtml("&#x6a;", MessageUtils.getLocalizedMessage("analysis_custom.label.edit")));
    tfEditBtn.setMargin(new Insets(2, 4, 2, 4));
    tfEditBtn.setEnabled(false);
    tfEditBtn.addActionListener(listeners::editTokenFilters);
    c.fill = GridBagConstraints.NONE;
    c.gridx = 7;
    c.gridy = 8;
    c.gridwidth = 2;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.CENTER;
    panel.add(tfEditBtn, c);

    JLabel tfAddLabel = new JLabel(FontUtils.elegantIconHtml("&#x4c;", MessageUtils.getLocalizedMessage("analysis_custom.label.add")));
    tfAddLabel.setHorizontalAlignment(JLabel.LEFT);
    c.fill = GridBagConstraints.HORIZONTAL;
    c.gridx = 1;
    c.gridy = 10;
    c.gridwidth = 1;
    c.gridheight = 1;
    c.weightx = 0.1;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(tfAddLabel, c);

    c.gridx = 2;
    c.gridy = 10;
    c.gridwidth = 5;
    c.gridheight = 1;
    c.weightx = 0.5;
    c.weighty = 0.5;
    c.anchor = GridBagConstraints.LINE_END;
    panel.add(tfFactoryCombo, c);

    return panel;
  }

  // control methods

  @SuppressForbidden(reason = "JFilechooser#getSelectedFile() returns java.io.File")
  private void chooseConfigDir() {
    fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

    int ret = fileChooser.showOpenDialog(containerPanel);
    if (ret == JFileChooser.APPROVE_OPTION) {
      File dir = fileChooser.getSelectedFile();
      confDirTF.setText(dir.getAbsolutePath());
    }
  }

  @SuppressForbidden(reason = "JFilechooser#getSelectedFiles() returns java.io.File[]")
  private void loadExternalJars() {
    fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
    fileChooser.setMultiSelectionEnabled(true);

    int ret = fileChooser.showOpenDialog(containerPanel);
    if (ret == JFileChooser.APPROVE_OPTION) {
      File[] files = fileChooser.getSelectedFiles();
      analysisModel.addExternalJars(Arrays.stream(files).map(File::getAbsolutePath).collect(Collectors.toList()));
      operatorRegistry.get(CustomAnalyzerPanelOperator.class).ifPresent(operator ->
          operator.resetAnalysisComponents()
      );
      messageBroker.showStatusMessage("External jars were added.");
    }
  }


  private void buildAnalyzer() {
    List<String> charFilters = ListUtils.getAllItems(selectedCfList);
    assert charFilters.size() == cfParamsList.size();

    List<String> tokenFilters = ListUtils.getAllItems(selectedTfList);
    assert tokenFilters.size() == tfParamsList.size();

    String tokenizerName = selectedTokTF.getText();
    CustomAnalyzerConfig.Builder builder =
        new CustomAnalyzerConfig.Builder(tokenizerName, tokParams).configDir(confDirTF.getText());
    IntStream.range(0, charFilters.size()).forEach(i ->
        builder.addCharFilterConfig(charFilters.get(i), cfParamsList.get(i))
    );
    IntStream.range(0, tokenFilters.size()).forEach(i ->
        builder.addTokenFilterConfig(tokenFilters.get(i), tfParamsList.get(i))
    );
    CustomAnalyzerConfig config = builder.build();

    operatorRegistry.get(AnalysisTabOperator.class).ifPresent(operator -> {
      operator.setAnalyzerByCustomConfiguration(config);
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("analysis.message.build_success"));
      buildBtn.setEnabled(false);
    });

  }

  private void addCharFilter() {
    if (Objects.isNull(cfFactoryCombo.getSelectedItem()) || cfFactoryCombo.getSelectedItem() == "") {
      return;
    }

    int targetIndex = selectedCfList.getModel().getSize();
    String selectedItem = (String) cfFactoryCombo.getSelectedItem();
    List<String> updatedList = ListUtils.getAllItems(selectedCfList);
    updatedList.add(selectedItem);
    cfParamsList.add(new HashMap<>());

    assert selectedCfList.getModel().getSize() == cfParamsList.size();

    showEditParamsDialog(MessageUtils.getLocalizedMessage("analysis.dialog.title.char_filter_params"),
        EditParamsMode.CHARFILTER, targetIndex, selectedItem, cfParamsList.get(cfParamsList.size() - 1),
        () -> {
          selectedCfList.setModel(new DefaultComboBoxModel<>(updatedList.toArray(new String[0])));
          cfFactoryCombo.setSelectedItem("");
          cfEditBtn.setEnabled(true);
          buildBtn.setEnabled(true);
        });
  }

  private void setTokenizer() {
    if (Objects.isNull(tokFactoryCombo.getSelectedItem()) || tokFactoryCombo.getSelectedItem() == "") {
      return;
    }

    String selectedItem = (String) tokFactoryCombo.getSelectedItem();
    showEditParamsDialog(MessageUtils.getLocalizedMessage("analysis.dialog.title.tokenizer_params"),
        EditParamsMode.TOKENIZER, -1, selectedItem, Collections.emptyMap(),
        () -> {
          selectedTokTF.setText(selectedItem);
          tokFactoryCombo.setSelectedItem("");
          buildBtn.setEnabled(true);
        });
  }

  private void addTokenFilter() {
    if (Objects.isNull(tfFactoryCombo.getSelectedItem()) || tfFactoryCombo.getSelectedItem() == "") {
      return;
    }

    int targetIndex = selectedTfList.getModel().getSize();
    String selectedItem = (String) tfFactoryCombo.getSelectedItem();
    List<String> updatedList = ListUtils.getAllItems(selectedTfList);
    updatedList.add(selectedItem);
    tfParamsList.add(new HashMap<>());

    assert selectedTfList.getModel().getSize() == tfParamsList.size();

    showEditParamsDialog(MessageUtils.getLocalizedMessage("analysis.dialog.title.token_filter_params"),
        EditParamsMode.TOKENFILTER, targetIndex, selectedItem, tfParamsList.get(tfParamsList.size() - 1),
        () -> {
          selectedTfList.setModel(new DefaultComboBoxModel<>(updatedList.toArray(new String[updatedList.size()])));
          tfFactoryCombo.setSelectedItem("");
          tfEditBtn.setEnabled(true);
          buildBtn.setEnabled(true);
        });
  }

  private void showEditParamsDialog(String title, EditParamsMode mode, int targetIndex, String selectedItem, Map<String, String> params, Callable callback) {
    new DialogOpener<>(editParamsDialogFactory).open(title, 400, 300,
        (factory) -> {
          factory.setMode(mode);
          factory.setTargetIndex(targetIndex);
          factory.setTarget(selectedItem);
          factory.setParams(params);
          factory.setCallback(callback);
        });
  }

  private void editCharFilters() {
    List<String> filters = ListUtils.getAllItems(selectedCfList);
    showEditFiltersDialog(EditFiltersMode.CHARFILTER, filters,
        () -> {
          cfEditBtn.setEnabled(selectedCfList.getModel().getSize() > 0);
          buildBtn.setEnabled(true);
        });
  }

  private void editTokenizer() {
    String selectedItem = selectedTokTF.getText();
    showEditParamsDialog(MessageUtils.getLocalizedMessage("analysis.dialog.title.tokenizer_params"),
        EditParamsMode.TOKENIZER, -1, selectedItem, tokParams, () -> {
          buildBtn.setEnabled(true);
        });
  }

  private void editTokenFilters() {
    List<String> filters = ListUtils.getAllItems(selectedTfList);
    showEditFiltersDialog(EditFiltersMode.TOKENFILTER, filters,
        () -> {
          tfEditBtn.setEnabled(selectedTfList.getModel().getSize() > 0);
          buildBtn.setEnabled(true);
        });
  }

  private void showEditFiltersDialog(EditFiltersMode mode, List<String> selectedFilters, Callable callback) {
    String title = (mode == EditFiltersMode.CHARFILTER) ?
        MessageUtils.getLocalizedMessage("analysis.dialog.title.selected_char_filter") :
        MessageUtils.getLocalizedMessage("analysis.dialog.title.selected_token_filter");
    new DialogOpener<>(editFiltersDialogFactory).open(title, 400, 300,
        (factory) -> {
          factory.setMode(mode);
          factory.setSelectedFilters(selectedFilters);
          factory.setCallback(callback);
        });
  }

  @Override
  public void setAnalysisModel(Analysis model) {
    analysisModel = model;
  }

  @Override
  public void resetAnalysisComponents() {
    setAvailableCharFilterFactories();
    setAvailableTokenizerFactories();
    setAvailableTokenFilterFactories();
    buildBtn.setEnabled(true);
  }

  private void setAvailableCharFilterFactories() {
    Collection<String> charFilters = analysisModel.getAvailableCharFilters();
    String[] charFilterNames = new String[charFilters.size() + 1];
    charFilterNames[0] = "";
    System.arraycopy(charFilters.toArray(new String[0]), 0, charFilterNames, 1, charFilters.size());
    cfFactoryCombo.setModel(new DefaultComboBoxModel<>(charFilterNames));
  }

  private void setAvailableTokenizerFactories() {
    Collection<String> tokenizers = analysisModel.getAvailableTokenizers();
    String[] tokenizerNames = new String[tokenizers.size() + 1];
    tokenizerNames[0] = "";
    System.arraycopy(tokenizers.toArray(new String[0]), 0, tokenizerNames, 1, tokenizers.size());
    tokFactoryCombo.setModel(new DefaultComboBoxModel<>(tokenizerNames));
  }

  private void setAvailableTokenFilterFactories() {
    Collection<String> tokenFilters = analysisModel.getAvailableTokenFilters();
    String[] tokenFilterNames = new String[tokenFilters.size() + 1];
    tokenFilterNames[0] = "";
    System.arraycopy(tokenFilters.toArray(new String[0]), 0, tokenFilterNames, 1, tokenFilters.size());
    tfFactoryCombo.setModel(new DefaultComboBoxModel<>(tokenFilterNames));
  }

  @Override
  public void updateCharFilters(List<Integer> deletedIndexes) {
    // update filters
    List<String> filters = ListUtils.getAllItems(selectedCfList);
    String[] updatedFilters = IntStream.range(0, filters.size())
        .filter(i -> !deletedIndexes.contains(i))
        .mapToObj(filters::get)
        .toArray(String[]::new);
    selectedCfList.setModel(new DefaultComboBoxModel<>(updatedFilters));
    // update parameters map for each filter
    List<Map<String, String>> updatedParamList = IntStream.range(0, cfParamsList.size())
        .filter(i -> !deletedIndexes.contains(i))
        .mapToObj(cfParamsList::get)
        .collect(Collectors.toList());
    cfParamsList.clear();
    cfParamsList.addAll(updatedParamList);
    assert selectedCfList.getModel().getSize() == cfParamsList.size();
  }

  @Override
  public void updateTokenFilters(List<Integer> deletedIndexes) {
    // update filters
    List<String> filters = ListUtils.getAllItems(selectedTfList);
    String[] updatedFilters = IntStream.range(0, filters.size())
        .filter(i -> !deletedIndexes.contains(i))
        .mapToObj(filters::get)
        .toArray(String[]::new);
    selectedTfList.setModel(new DefaultComboBoxModel<>(updatedFilters));
    // update parameters map for each filter
    List<Map<String, String>> updatedParamList = IntStream.range(0, tfParamsList.size())
        .filter(i -> !deletedIndexes.contains(i))
        .mapToObj(tfParamsList::get)
        .collect(Collectors.toList());
    tfParamsList.clear();
    tfParamsList.addAll(updatedParamList);
    assert selectedTfList.getModel().getSize() == tfParamsList.size();
  }

  @Override
  public Map<String, String> getCharFilterParams(int index) {
    if (index < 0 || index > cfParamsList.size()) {
      throw new IllegalArgumentException();
    }
    return Collections.unmodifiableMap(cfParamsList.get(index));
  }

  @Override
  public void updateCharFilterParams(int index, Map<String, String> updatedParams) {
    if (index < 0 || index > cfParamsList.size()) {
      throw new IllegalArgumentException();
    }
    if (index == cfParamsList.size()) {
      cfParamsList.add(new HashMap<>());
    }
    cfParamsList.get(index).clear();
    cfParamsList.get(index).putAll(updatedParams);
  }

  @Override
  public void updateTokenizerParams(Map<String, String> updatedParams) {
    tokParams.clear();
    tokParams.putAll(updatedParams);
  }

  @Override
  public Map<String, String> getTokenFilterParams(int index) {
    if (index < 0 || index > tfParamsList.size()) {
      throw new IllegalArgumentException();
    }
    return Collections.unmodifiableMap(tfParamsList.get(index));
  }

  @Override
  public void updateTokenFilterParams(int index, Map<String, String> updatedParams) {
    if (index < 0 || index > tfParamsList.size()) {
      throw new IllegalArgumentException();
    }
    if (index == tfParamsList.size()) {
      tfParamsList.add(new HashMap<>());
    }
    tfParamsList.get(index).clear();
    tfParamsList.get(index).putAll(updatedParams);
  }

  private class ListenerFunctions {

    void chooseConfigDir(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.chooseConfigDir();
    }

    void loadExternalJars(MouseEvent e) {
      CustomAnalyzerPanelProvider.this.loadExternalJars();
    }

    void buildAnalyzer(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.buildAnalyzer();
    }

    void addCharFilter(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.addCharFilter();
    }

    void setTokenizer(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.setTokenizer();
    }

    void addTokenFilter(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.addTokenFilter();
    }

    void editCharFilters(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.editCharFilters();
    }

    void editTokenizer(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.editTokenizer();
    }

    void editTokenFilters(ActionEvent e) {
      CustomAnalyzerPanelProvider.this.editTokenFilters();
    }

  }

}
