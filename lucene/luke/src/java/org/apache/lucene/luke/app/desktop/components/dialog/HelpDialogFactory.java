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

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Window;
import java.io.IOException;

import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

/** Factory of help dialog */
public final class HelpDialogFactory implements DialogOpener.DialogFactory {

  private static HelpDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  private String desc;

  private JComponent helpContent;

  public synchronized static HelpDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new HelpDialogFactory();
    }
    return instance;
  }

  private HelpDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  public void setContent(JComponent helpContent) {
    this.helpContent = helpContent;
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
    header.add(new JLabel(desc));
    panel.add(header, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new GridLayout(1, 1));
    center.setOpaque(false);
    center.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
    center.add(helpContent);
    panel.add(center, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    footer.setOpaque(false);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }
}
