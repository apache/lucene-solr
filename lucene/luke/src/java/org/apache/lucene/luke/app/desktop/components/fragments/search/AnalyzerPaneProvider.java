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

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
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
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TabSwitcherProxy;
import org.apache.lucene.luke.app.desktop.components.TabbedPaneProvider;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

/** Provider of the Analyzer pane */
public final class AnalyzerPaneProvider implements AnalyzerTabOperator {

  private final TabSwitcherProxy tabSwitcher;

  private final JLabel analyzerNameLbl = new JLabel(StandardAnalyzer.class.getName());

  private final JList<String> charFilterList = new JList<>();

  private final JTextField tokenizerTF = new JTextField();

  private final JList<String> tokenFilterList = new JList<>();

  public AnalyzerPaneProvider() {
    this.tabSwitcher = TabSwitcherProxy.getInstance();

    ComponentOperatorRegistry.getInstance().register(AnalyzerTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    panel.add(initAnalyzerNamePanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initAnalysisChainPanel());

    tokenizerTF.setEditable(false);

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initAnalyzerNamePanel() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setOpaque(false);

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.label.name")));

    panel.add(analyzerNameLbl);

    JLabel changeLbl = new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.hyperlink.change"));
    changeLbl.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(MouseEvent e) {
        tabSwitcher.switchTab(TabbedPaneProvider.Tab.ANALYZER);
      }
    });
    panel.add(FontUtils.toLinkText(changeLbl));

    return panel;
  }

  private JPanel initAnalysisChainPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel top = new JPanel(new FlowLayout(FlowLayout.LEADING));
    top.setOpaque(false);
    top.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
    top.add(new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.label.chain")));
    panel.add(top, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new GridBagLayout());
    center.setOpaque(false);

    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.BOTH;
    c.insets = new Insets(5, 5, 5, 5);

    c.gridx = 0;
    c.gridy = 0;
    c.weightx = 0.1;
    center.add(new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.label.charfilters")), c);

    charFilterList.setVisibleRowCount(3);
    JScrollPane charFilterSP = new JScrollPane(charFilterList);
    c.gridx = 1;
    c.gridy = 0;
    c.weightx = 0.5;
    center.add(charFilterSP, c);

    c.gridx = 0;
    c.gridy = 1;
    c.weightx = 0.1;
    center.add(new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.label.tokenizer")), c);

    tokenizerTF.setColumns(30);
    tokenizerTF.setPreferredSize(new Dimension(400, 25));
    tokenizerTF.setBorder(BorderFactory.createLineBorder(Color.gray));
    c.gridx = 1;
    c.gridy = 1;
    c.weightx = 0.5;
    center.add(tokenizerTF, c);

    c.gridx = 0;
    c.gridy = 2;
    c.weightx = 0.1;
    center.add(new JLabel(MessageUtils.getLocalizedMessage("search_analyzer.label.tokenfilters")), c);

    tokenFilterList.setVisibleRowCount(3);
    JScrollPane tokenFilterSP = new JScrollPane(tokenFilterList);
    c.gridx = 1;
    c.gridy = 2;
    c.weightx = 0.5;
    center.add(tokenFilterSP, c);

    panel.add(center, BorderLayout.CENTER);

    return panel;
  }

  @Override
  public void setAnalyzer(Analyzer analyzer) {
    analyzerNameLbl.setText(analyzer.getClass().getName());

    if (analyzer instanceof CustomAnalyzer) {
      CustomAnalyzer customAnalyzer = (CustomAnalyzer) analyzer;

      DefaultListModel<String> charFilterListModel = new DefaultListModel<>();
      customAnalyzer.getCharFilterFactories().stream()
          .map(f -> f.getClass().getSimpleName())
          .forEach(charFilterListModel::addElement);
      charFilterList.setModel(charFilterListModel);

      tokenizerTF.setText(customAnalyzer.getTokenizerFactory().getClass().getSimpleName());

      DefaultListModel<String> tokenFilterListModel = new DefaultListModel<>();
      customAnalyzer.getTokenFilterFactories().stream()
          .map(f -> f.getClass().getSimpleName())
          .forEach(tokenFilterListModel::addElement);
      tokenFilterList.setModel(tokenFilterListModel);

      charFilterList.setBackground(Color.white);
      tokenizerTF.setBackground(Color.white);
      tokenFilterList.setBackground(Color.white);
    } else {
      charFilterList.setModel(new DefaultListModel<>());
      tokenizerTF.setText("");
      tokenFilterList.setModel(new DefaultListModel<>());

      charFilterList.setBackground(Color.lightGray);
      tokenizerTF.setBackground(Color.lightGray);
      tokenFilterList.setBackground(Color.lightGray);
    }
  }


}
