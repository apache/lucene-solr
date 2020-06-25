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
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import java.awt.Color;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.ImageUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Factory of export terms dialog
 */
public final class ExportTermsDialogFactory implements DialogOpener.DialogFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static ExportTermsDialogFactory instance;

  private final IndexToolsFactory indexToolsFactory = new IndexToolsFactory();

  private final Preferences prefs;

  private final IndexHandler indexHandler;

  private final JComboBox<String> fieldCombo = new JComboBox<String>();

  private final JComboBox<String> delimiterCombo = new JComboBox<String>();

  private final JTextField destDir = new JTextField();

  private final JLabel statusLbl = new JLabel();

  private final JLabel indicatorLbl = new JLabel();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private JDialog dialog;

  private IndexTools toolsModel;

  private String selectedDelimiter;

  public synchronized static ExportTermsDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new ExportTermsDialogFactory();
    }
    return instance;
  }

  private ExportTermsDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    indexHandler.addObserver(new Observer());
    Stream.of(Delimiter.values()).forEachOrdered(delimiterVal -> delimiterCombo.addItem(delimiterVal.getDescription()));
    delimiterCombo.setSelectedItem(Delimiter.COMMA.getDescription());//Set default delimiter
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
    JPanel panel = new JPanel(new GridLayout(5, 1));
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    panel.add(currentOpenIndexPanel());
    panel.add(fieldComboPanel());
    panel.add(destinationDirPanel());
    panel.add(delimiterComboPanel());
    panel.add(statusPanel());
    panel.add(actionButtonsPanel());

    return panel;
  }

  private JPanel currentOpenIndexPanel() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setBorder(BorderFactory.createEmptyBorder());
    panel.setOpaque(false);
    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("export.terms.label.index_path"));
    JLabel value = new JLabel(indexHandler.getState().getIndexPath());
    value.setToolTipText(indexHandler.getState().getIndexPath());
    panel.add(label);
    panel.add(value);
    return panel;
  }

  private JPanel delimiterComboPanel() {
    JPanel panel = new JPanel(new GridLayout(2, 1));
    panel.setOpaque(false);
    panel.add(new JLabel("Select Delimiter: "));
    panel.add(delimiterCombo);
    return panel;
  }

  private JPanel fieldComboPanel() {
    JPanel panel = new JPanel(new GridLayout(2, 1));
    panel.setOpaque(false);
    panel.add(new JLabel(MessageUtils.getLocalizedMessage("export.terms.field")));
    panel.add(fieldCombo);
    return panel;
  }

  private JPanel destinationDirPanel() {
    JPanel panel = new JPanel(new GridLayout(2, 1));
    panel.setOpaque(false);

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("export.terms.label.output_path")));

    JPanel inputPanel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    inputPanel.setBorder(BorderFactory.createEmptyBorder());
    inputPanel.setOpaque(false);
    destDir.setText(System.getProperty("user.home"));
    destDir.setColumns(60);
    destDir.setPreferredSize(new Dimension(200, 30));
    destDir.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    destDir.setEditable(false);
    destDir.setBackground(Color.white);
    inputPanel.add(destDir);

    JButton browseBtn = new JButton(MessageUtils.getLocalizedMessage("export.terms.button.browse"));
    browseBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    browseBtn.setMargin(new Insets(3, 0, 3, 0));
    browseBtn.addActionListener(listeners::browseDirectory);
    inputPanel.add(browseBtn);

    panel.add(inputPanel);
    return panel;
  }

  private JPanel actionButtonsPanel() {
    // Buttons
    JPanel execButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    execButtons.setOpaque(false);
    JButton exportBtn = new JButton(MessageUtils.getLocalizedMessage("export.terms.button.export"));
    exportBtn.setMargin(new Insets(3, 0, 3, 0));
    exportBtn.addActionListener(listeners::export);
    execButtons.add(exportBtn);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setMargin(new Insets(3, 0, 3, 0));
    closeBtn.addActionListener(e -> dialog.dispose());
    execButtons.add(closeBtn);
    return execButtons;
  }

  private JPanel statusPanel() {
    JPanel status = new JPanel(new FlowLayout(FlowLayout.LEADING));
    status.setOpaque(false);
    indicatorLbl.setIcon(ImageUtils.createImageIcon("indicator.gif", 20, 20));
    indicatorLbl.setVisible(false);
    status.add(statusLbl);
    status.add(indicatorLbl);
    return status;
  }

  private class ListenerFunctions {

    @SuppressForbidden(reason = "JFilechooser#getSelectedFile() returns java.io.File")
    void browseDirectory(ActionEvent e) {
      JFileChooser fileChooser = new JFileChooser();
      fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
      fileChooser.setFileHidingEnabled(false);
      int retVal = fileChooser.showOpenDialog(dialog);
      if (retVal == JFileChooser.APPROVE_OPTION) {
        File f = fileChooser.getSelectedFile();
        destDir.setText(f.getAbsolutePath());
      }
    }

    void export(ActionEvent e) {
      ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("export-terms-dialog"));

      SwingWorker<Void, Void> task = new SwingWorker<Void, Void>() {

        String filename;

        @Override
        protected Void doInBackground() {
          setProgress(0);
          statusLbl.setText("Exporting...");
          indicatorLbl.setVisible(true);
          String field = (String) fieldCombo.getSelectedItem();
          selectedDelimiter = Delimiter.getSelectedDelimiterValue((String) delimiterCombo.getSelectedItem());

          String directory = destDir.getText();
          try {
            filename = toolsModel.exportTerms(directory, field, selectedDelimiter);
          } catch (LukeException e) {
            log.error("Error while exporting terms from field {}", field, e);
            statusLbl.setText(MessageUtils.getLocalizedMessage("export.terms.label.error", e.getMessage()));
          } catch (Exception e) {
            log.error("Error while exporting terms from field {}", field, e);
            statusLbl.setText(MessageUtils.getLocalizedMessage("message.error.unknown"));
            throw e;
          } finally {
            setProgress(100);
          }
          return null;
        }

        @Override
        protected void done() {
          indicatorLbl.setVisible(false);
          if (filename != null) {
            statusLbl.setText(MessageUtils.getLocalizedMessage("export.terms.label.success", filename, "[term]" + selectedDelimiter + "[doc frequency]"));
          }
        }
      };

      executor.submit(task);
      executor.shutdown();
    }

  }

  private class Observer implements IndexObserver {

    @Override
    public void openIndex(LukeState state) {
      toolsModel = indexToolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
      IndexUtils.getFieldNames(state.getIndexReader()).stream().sorted().forEach(fieldCombo::addItem);
    }

    @Override
    public void closeIndex() {
      fieldCombo.removeAllItems();
      toolsModel = null;
    }

  }

  /**
   * Delimiters that can be selected
   */
  private enum Delimiter {
    COMMA("Comma", ","), WHITESPACE("Whitespace", " "), TAB("Tab", "\t");

    private final String description;
    private final String separator;

    private Delimiter(final String description, final String separator) {
      this.description = description;
      this.separator = separator;
    }

    String getDescription() {
      return this.description;
    }

    String getSeparator() {
      return this.separator;
    }

    static String getSelectedDelimiterValue(String delimiter) {
      return Arrays.stream(Delimiter.values())
          .filter(e -> e.description.equals(delimiter))
          .findFirst()
          .orElse(COMMA)
          .getSeparator();
    }
  }

}
