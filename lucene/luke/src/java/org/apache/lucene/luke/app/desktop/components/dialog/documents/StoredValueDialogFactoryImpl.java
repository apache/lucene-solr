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

package org.apache.lucene.luke.app.desktop.components.dialog.documents;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.util.Objects;

import com.google.inject.Inject;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

public final class StoredValueDialogFactoryImpl implements StoredValueDialogFactory {

  private final Preferences prefs;

  private JDialog dialog;

  private String field;

  private String value;

  @Override
  public void setField(String field) {
    this.field = field;
  }

  @Override
  public void setValue(String value) {
    this.value = value;
  }

  @Inject
  public StoredValueDialogFactoryImpl(Preferences prefs) {
    this.prefs = prefs;
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    if (Objects.isNull(field) || Objects.isNull(value)) {
      throw new IllegalStateException("field name and/or stored value is not set.");
    }

    dialog = new JDialog(owner, "Term Vector", Dialog.ModalityType.APPLICATION_MODAL);
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

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING, 5, 5));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("documents.stored.label.stored_value")));
    header.add(new JLabel(field));
    panel.add(header, BorderLayout.PAGE_START);

    JTextArea valueTA = new JTextArea(value);
    valueTA.setLineWrap(true);
    valueTA.setEditable(false);
    valueTA.setBackground(Color.white);
    JScrollPane scrollPane = new JScrollPane(valueTA);
    panel.add(scrollPane, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 5, 5));
    footer.setOpaque(false);

    JButton copyBtn = new JButton(FontUtils.elegantIconHtml("&#xe0e6;", MessageUtils.getLocalizedMessage("button.copy")));
    copyBtn.setMargin(new Insets(3, 3, 3, 3));
    copyBtn.addActionListener(e -> {
      Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
      StringSelection selection = new StringSelection(value);
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


}
