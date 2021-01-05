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

package org.apache.lucene.luke.app.desktop.components.dialog;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Window;
import java.io.IOException;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.lang.Callable;

/** Factory of confirm dialog */
public final class ConfirmDialogFactory implements DialogOpener.DialogFactory {

  private static ConfirmDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  private String message;

  private Callable callback;

  public static synchronized ConfirmDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new ConfirmDialogFactory();
    }
    return instance;
  }

  private ConfirmDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void setCallback(Callable callback) {
    this.callback = callback;
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
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    JLabel alertIconLbl = new JLabel(FontUtils.elegantIconHtml("&#x71;"));
    alertIconLbl.setHorizontalAlignment(JLabel.CENTER);
    alertIconLbl.setFont(new Font(alertIconLbl.getFont().getFontName(), Font.PLAIN, 25));
    header.add(alertIconLbl);
    panel.add(header, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new GridLayout(1, 1));
    center.setOpaque(false);
    center.setBorder(BorderFactory.createLineBorder(Color.gray, 3));
    center.add(new JLabel(message, JLabel.CENTER));
    panel.add(center, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    footer.setOpaque(false);
    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.addActionListener(
        e -> {
          callback.call();
          dialog.dispose();
        });
    footer.add(okBtn);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }
}
