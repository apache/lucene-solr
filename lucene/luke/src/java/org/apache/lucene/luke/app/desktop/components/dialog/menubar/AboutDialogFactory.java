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

package org.apache.lucene.luke.app.desktop.components.dialog.menubar;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingUtilities;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Desktop;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.lucene.LucenePackage;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.ImageUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.URLLabel;
import org.apache.lucene.luke.models.LukeException;

/** Factory of about dialog */
public final class AboutDialogFactory implements DialogOpener.DialogFactory {

  private static AboutDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  public synchronized static AboutDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new AboutDialogFactory();
    }
    return instance;
  }

  private AboutDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
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
    panel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));

    panel.add(header(), BorderLayout.PAGE_START);
    panel.add(center(), BorderLayout.CENTER);
    panel.add(footer(), BorderLayout.PAGE_END);

    return panel;
  }

  private JPanel header() {
    JPanel panel = new JPanel(new GridLayout(3, 1));
    panel.setOpaque(false);

    JPanel logo = new JPanel(new FlowLayout(FlowLayout.CENTER));
    logo.setOpaque(false);
    logo.add(new JLabel(ImageUtils.createImageIcon("luke-logo.gif", 200, 40)));
    panel.add(logo);

    JPanel project = new JPanel(new FlowLayout(FlowLayout.CENTER));
    project.setOpaque(false);
    JLabel projectLbl = new JLabel("Lucene Toolbox Project");
    projectLbl.setFont(new Font(projectLbl.getFont().getFontName(), Font.BOLD, 32));
    projectLbl.setForeground(Color.decode("#5aaa88"));
    project.add(projectLbl);
    panel.add(project);

    JPanel desc = new JPanel();
    desc.setOpaque(false);
    desc.setLayout(new BoxLayout(desc, BoxLayout.PAGE_AXIS));

    JPanel subTitle = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 5));
    subTitle.setOpaque(false);
    JLabel subTitleLbl = new JLabel("GUI client of the best Java search library Apache Lucene");
    subTitleLbl.setFont(new Font(subTitleLbl.getFont().getFontName(), Font.PLAIN, 20));
    subTitle.add(subTitleLbl);
    subTitle.add(new JLabel(ImageUtils.createImageIcon("lucene-logo.gif", 100, 15)));
    desc.add(subTitle);

    JPanel link = new JPanel(new FlowLayout(FlowLayout.CENTER, 5, 5));
    link.setOpaque(false);
    JLabel linkLbl = FontUtils.toLinkText(new URLLabel("https://lucene.apache.org/"));
    link.add(linkLbl);
    desc.add(link);

    panel.add(desc);

    return panel;
  }

  private JScrollPane center() {
    JEditorPane editorPane = new JEditorPane();
    editorPane.setOpaque(false);
    editorPane.setMargin(new Insets(0, 5, 2, 5));
    editorPane.setContentType("text/html");
    editorPane.setText(LICENSE_NOTICE);
    editorPane.setEditable(false);
    editorPane.addHyperlinkListener(hyperlinkListener);
    JScrollPane scrollPane = new JScrollPane(editorPane, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
    scrollPane.setBorder(BorderFactory.createLineBorder(Color.gray));
    SwingUtilities.invokeLater(() -> {
      // Set the scroll bar position to top
      scrollPane.getVerticalScrollBar().setValue(0);
    });
    return scrollPane;
  }

  private JPanel footer() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    panel.setOpaque(false);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setMargin(new Insets(5, 5, 5, 5));
    if (closeBtn.getActionListeners().length == 0) {
      closeBtn.addActionListener(e -> dialog.dispose());
    }
    panel.add(closeBtn);
    return panel;
  }

  private static final String LUCENE_IMPLEMENTATION_VERSION = LucenePackage.get().getImplementationVersion();

  private static final String LICENSE_NOTICE =
      "<p>[Implementation Version]</p>" +
          "<p>" + (Objects.nonNull(LUCENE_IMPLEMENTATION_VERSION) ? LUCENE_IMPLEMENTATION_VERSION : "") + "</p>" +
          "<p>[License]</p>" +
          "<p>Luke is distributed under <a href=\"http://www.apache.org/licenses/LICENSE-2.0\">Apache License Version 2.0</a> (http://www.apache.org/licenses/LICENSE-2.0) " +
          "and includes <a href=\"https://www.elegantthemes.com/blog/resources/elegant-icon-font\">The Elegant Icon Font</a> (https://www.elegantthemes.com/blog/resources/elegant-icon-font) " +
          "licensed under <a href=\"https://opensource.org/licenses/MIT\">MIT</a> (https://opensource.org/licenses/MIT)</p>" +
          "<p>[Brief history]</p>" +
          "<ul>" +
          "<li>The original author is Andrzej Bialecki</li>" +
          "<li>The project has been mavenized by Neil Ireson</li>" +
          "<li>The project has been ported to Lucene trunk (marked as 5.0 at the time) by Dmitry Kan\n</li>" +
          "<li>The project has been back-ported to Lucene 4.3 by sonarname</li>" +
          "<li>There are updates to the (non-mavenized) project done by tarzanek</li>" +
          "<li>The UI and core components has been re-implemented on top of Swing by Tomoko Uchida</li>" +
          "</ul>"
      ;


  private static final HyperlinkListener hyperlinkListener = e -> {
    if (e.getEventType() == HyperlinkEvent.EventType.ACTIVATED)
      if (Desktop.isDesktopSupported()) {
        try {
          Desktop.getDesktop().browse(e.getURL().toURI());
        } catch (IOException | URISyntaxException ex) {
          throw new LukeException(ex.getMessage(), ex);
        }
      }
  };


}
