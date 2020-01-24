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

package org.apache.lucene.luke.app.desktop.components.dialog.search;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.search.Explanation;

/** Factory of explain dialog */
public final class ExplainDialogFactory implements DialogOpener.DialogFactory {

  private static ExplainDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  private int docid = -1;

  private Explanation explanation;

  public synchronized static ExplainDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new ExplainDialogFactory();
    }
    return  instance;
  }

  private ExplainDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setDocid(int docid) {
    this.docid = docid;
  }

  public void setExplanation(Explanation explanation) {
    this.explanation = explanation;
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    if (docid < 0 || Objects.isNull(explanation)) {
      throw new IllegalStateException("docid and/or explanation is not set.");
    }

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
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING, 5, 10));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search.explanation.description")));
    header.add(new JLabel(String.valueOf(docid)));
    panel.add(header, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new GridLayout(1, 1));
    center.setOpaque(false);
    center.add(new JScrollPane(createExplanationTree()));
    panel.add(center, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 5, 5));
    footer.setOpaque(false);

    JButton copyBtn = new JButton(FontUtils.elegantIconHtml("&#xe0e6;", MessageUtils.getLocalizedMessage("button.copy")));
    copyBtn.setMargin(new Insets(3, 3, 3, 3));
    copyBtn.addActionListener(e -> {
      Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
      StringSelection selection = new StringSelection(explanationToString());
      clipboard.setContents(selection, null);
    });
    footer.add(copyBtn);

    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setMargin(new Insets(3, 3, 3, 3));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  private JTree createExplanationTree() {
    DefaultMutableTreeNode top = createNode(explanation);
    traverse(top, explanation.getDetails());

    JTree tree = new JTree(top);
    tree.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
    DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
    renderer.setOpenIcon(null);
    renderer.setClosedIcon(null);
    renderer.setLeafIcon(null);
    tree.setCellRenderer(renderer);
    // expand all nodes
    for (int row = 0; row < tree.getRowCount(); row++) {
      tree.expandRow(row);
    }
    return tree;
  }

  private void traverse(DefaultMutableTreeNode parent, Explanation[] explanations) {
    for (Explanation explanation : explanations) {
      DefaultMutableTreeNode node = createNode(explanation);
      parent.add(node);
      traverse(node, explanation.getDetails());
    }
  }

  private DefaultMutableTreeNode createNode(Explanation explanation) {
    return new DefaultMutableTreeNode(format(explanation));
  }

  private String explanationToString() {
    StringBuilder sb = new StringBuilder(format(explanation));
    sb.append(System.lineSeparator());
    traverseToCopy(sb, 1, explanation.getDetails());
    return sb.toString();
  }

  private void traverseToCopy(StringBuilder sb, int depth, Explanation[] explanations) {
    for (Explanation explanation : explanations) {
      IntStream.range(0, depth).forEach(i -> sb.append("  "));
      sb.append(format(explanation));
      sb.append("\n");
      traverseToCopy(sb, depth + 1, explanation.getDetails());
    }
  }

  private String format(Explanation explanation) {
    return explanation.getValue() + " " + explanation.getDescription();
  }
}
