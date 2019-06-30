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

package org.apache.lucene.luke.app.desktop.components.dialog.analysis;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;

import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

/** Factory of analysis chain dialog */
public class AnalysisChainDialogFactory implements DialogOpener.DialogFactory {

  private static AnalysisChainDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  private CustomAnalyzer analyzer;

  public synchronized static AnalysisChainDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new AnalysisChainDialogFactory();
    }
    return instance;
  }

  private AnalysisChainDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setAnalyzer(CustomAnalyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    dialog = new JDialog(owner, title, Dialog.ModalityType.APPLICATION_MODAL);
    dialog.add(content());
    dialog.setSize(new Dimension(width, height));
    dialog.setLocationRelativeTo(owner);
    dialog.getContentPane().setBackground(prefs.getColorTheme().getBackgroundColor());
    return dialog;
  }

  private JPanel content() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    panel.add(analysisChain(), BorderLayout.PAGE_START);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 10, 5));
    footer.setOpaque(false);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  private JPanel analysisChain() {
    JPanel panel = new JPanel(new GridBagLayout());
    panel.setOpaque(false);

    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.HORIZONTAL;
    c.insets = new Insets(5, 5, 5, 5);

    c.gridx = 0;
    c.gridy = 0;
    c.weightx = 0.1;
    c.weighty = 0.5;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.dialog.chain.label.charfilters")), c);

    String[] charFilters = analyzer.getCharFilterFactories().stream().map(f -> CharFilterFactory.findSPIName(f.getClass())).toArray(String[]::new);
    JList<String> charFilterList = new JList<>(charFilters);
    charFilterList.setVisibleRowCount(charFilters.length == 0 ? 1 : Math.min(charFilters.length, 5));
    c.gridx = 1;
    c.gridy = 0;
    c.weightx = 0.5;
    c.weighty = 0.5;
    panel.add(new JScrollPane(charFilterList), c);

    c.gridx = 0;
    c.gridy = 1;
    c.weightx = 0.1;
    c.weighty = 0.1;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.dialog.chain.label.tokenizer")), c);

    String tokenizer = TokenizerFactory.findSPIName(analyzer.getTokenizerFactory().getClass());
    JTextField tokenizerTF = new JTextField(tokenizer);
    tokenizerTF.setColumns(30);
    tokenizerTF.setEditable(false);
    tokenizerTF.setPreferredSize(new Dimension(300, 25));
    tokenizerTF.setBorder(BorderFactory.createLineBorder(Color.gray));
    c.gridx = 1;
    c.gridy = 1;
    c.weightx = 0.5;
    c.weighty = 0.1;
    panel.add(tokenizerTF, c);

    c.gridx = 0;
    c.gridy = 2;
    c.weightx = 0.1;
    c.weighty = 0.5;
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.dialog.chain.label.tokenfilters")), c);

    String[] tokenFilters = analyzer.getTokenFilterFactories().stream().map(f -> TokenFilterFactory.findSPIName(f.getClass())).toArray(String[]::new);
    JList<String> tokenFilterList = new JList<>(tokenFilters);
    tokenFilterList.setVisibleRowCount(tokenFilters.length == 0 ? 1 : Math.min(tokenFilters.length, 5));
    tokenFilterList.setMinimumSize(new Dimension(300, 25));
    c.gridx = 1;
    c.gridy = 2;
    c.weightx = 0.5;
    c.weighty = 0.5;
    panel.add(new JScrollPane(tokenFilterList), c);

    return panel;
  }

}
