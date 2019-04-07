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
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.ImageUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.URLLabel;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;

/** Factory of create index dialog */
public class CreateIndexDialogFactory implements DialogOpener.DialogFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CreateIndexDialogFactory instance;

  private final Preferences prefs;

  private final IndexHandler indexHandler;

  private final JTextField locationTF = new JTextField();

  private final JButton browseBtn = new JButton();

  private final JTextField dirnameTF = new JTextField();

  private final JTextField dataDirTF = new JTextField();

  private final JButton dataBrowseBtn = new JButton();

  private final JButton clearBtn = new JButton();

  private final JLabel indicatorLbl = new JLabel();

  private final JButton createBtn = new JButton();

  private final JButton cancelBtn = new JButton();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private JDialog dialog;

  public synchronized static CreateIndexDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new CreateIndexDialogFactory();
    }
    return instance;
  }

  private  CreateIndexDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    initialize();
  }

  private void initialize() {
    locationTF.setPreferredSize(new Dimension(360, 30));
    locationTF.setText(System.getProperty("user.home"));
    locationTF.setEditable(false);

    browseBtn.setText(FontUtils.elegantIconHtml("&#x6e;", MessageUtils.getLocalizedMessage("button.browse")));
    browseBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    browseBtn.setPreferredSize(new Dimension(120, 30));
    browseBtn.addActionListener(listeners::browseLocationDirectory);

    dirnameTF.setPreferredSize(new Dimension(200, 30));

    dataDirTF.setPreferredSize(new Dimension(250, 30));
    dataDirTF.setEditable(false);

    clearBtn.setText(MessageUtils.getLocalizedMessage("button.clear"));
    clearBtn.setPreferredSize(new Dimension(70, 30));
    clearBtn.addActionListener(listeners::clearDataDir);

    dataBrowseBtn.setText(FontUtils.elegantIconHtml("&#x6e;", MessageUtils.getLocalizedMessage("button.browse")));
    dataBrowseBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    dataBrowseBtn.setPreferredSize(new Dimension(100, 30));
    dataBrowseBtn.addActionListener(listeners::browseDataDirectory);

    indicatorLbl.setIcon(ImageUtils.createImageIcon("indicator.gif", 20, 20));
    indicatorLbl.setVisible(false);

    createBtn.setText(MessageUtils.getLocalizedMessage("button.create"));
    createBtn.addActionListener(listeners::createIndex);

    cancelBtn.setText(MessageUtils.getLocalizedMessage("button.cancel"));
    cancelBtn.addActionListener(e -> dialog.dispose());
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
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    panel.add(basicSettings());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(optionalSettings());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(buttons());

    return panel;
  }

  private JPanel basicSettings() {
    JPanel panel = new JPanel(new GridLayout(2, 1));
    panel.setOpaque(false);

    JPanel locPath = new JPanel(new FlowLayout(FlowLayout.LEADING));
    locPath.setOpaque(false);
    locPath.add(new JLabel(MessageUtils.getLocalizedMessage("createindex.label.location")));
    locPath.add(locationTF);
    locPath.add(browseBtn);
    panel.add(locPath);

    JPanel dirName = new JPanel(new FlowLayout(FlowLayout.LEADING));
    dirName.setOpaque(false);
    dirName.add(new JLabel(MessageUtils.getLocalizedMessage("createindex.label.dirname")));
    dirName.add(dirnameTF);
    panel.add(dirName);

    return panel;
  }

  private JPanel optionalSettings() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JPanel description = new JPanel();
    description.setLayout(new BoxLayout(description, BoxLayout.Y_AXIS));
    description.setOpaque(false);

    JPanel name = new JPanel(new FlowLayout(FlowLayout.LEADING));
    name.setOpaque(false);
    JLabel nameLbl = new JLabel(MessageUtils.getLocalizedMessage("createindex.label.option"));
    name.add(nameLbl);
    description.add(name);

    JTextArea descTA1 = new JTextArea(MessageUtils.getLocalizedMessage("createindex.textarea.data_help1"));
    descTA1.setPreferredSize(new Dimension(550, 20));
    descTA1.setBorder(BorderFactory.createEmptyBorder(2, 10, 10, 5));
    descTA1.setOpaque(false);
    descTA1.setLineWrap(true);
    descTA1.setEditable(false);
    description.add(descTA1);

    JPanel link = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 1));
    link.setOpaque(false);
    JLabel linkLbl = FontUtils.toLinkText(new URLLabel(MessageUtils.getLocalizedMessage("createindex.label.data_link")));
    link.add(linkLbl);
    description.add(link);

    JTextArea descTA2 = new JTextArea(MessageUtils.getLocalizedMessage("createindex.textarea.data_help2"));
    descTA2.setPreferredSize(new Dimension(550, 50));
    descTA2.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 5));
    descTA2.setOpaque(false);
    descTA2.setLineWrap(true);
    descTA2.setEditable(false);
    description.add(descTA2);

    panel.add(description, BorderLayout.PAGE_START);

    JPanel dataDirPath = new JPanel(new FlowLayout(FlowLayout.LEADING));
    dataDirPath.setOpaque(false);
    dataDirPath.add(new JLabel(MessageUtils.getLocalizedMessage("createindex.label.datadir")));
    dataDirPath.add(dataDirTF);
    dataDirPath.add(dataBrowseBtn);

    dataDirPath.add(clearBtn);
    panel.add(dataDirPath, BorderLayout.CENTER);

    return panel;
  }

  private JPanel buttons() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 10, 20));

    panel.add(indicatorLbl);
    panel.add(createBtn);
    panel.add(cancelBtn);

    return panel;
  }

  private class ListenerFunctions {

    void browseLocationDirectory(ActionEvent e) {
      browseDirectory(locationTF);
    }

    void browseDataDirectory(ActionEvent e) {
      browseDirectory(dataDirTF);
    }

    @SuppressForbidden(reason = "JFilechooser#getSelectedFile() returns java.io.File")
    private void browseDirectory(JTextField tf) {
      JFileChooser fc = new JFileChooser(new File(tf.getText()));
      fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
      fc.setFileHidingEnabled(false);
      int retVal = fc.showOpenDialog(dialog);
      if (retVal == JFileChooser.APPROVE_OPTION) {
        File dir = fc.getSelectedFile();
        tf.setText(dir.getAbsolutePath());
      }
    }

    void createIndex(ActionEvent e) {
      Path path = Paths.get(locationTF.getText(), dirnameTF.getText());
      if (Files.exists(path)) {
        String message = "The directory " + path.toAbsolutePath().toString() + " already exists.";
        JOptionPane.showMessageDialog(dialog, message, "Empty index path", JOptionPane.ERROR_MESSAGE);
      } else {
        // create new index asynchronously
        ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("create-index-dialog"));

        SwingWorker<Void, Void> task = new SwingWorker<Void, Void>() {

          @Override
          protected Void doInBackground() throws Exception {
            setProgress(0);
            indicatorLbl.setVisible(true);
            createBtn.setEnabled(false);

            try {
              Directory dir = FSDirectory.open(path);
              IndexTools toolsModel = new IndexToolsFactory().newInstance(dir);

              if (dataDirTF.getText().isEmpty()) {
                // without sample documents
                toolsModel.createNewIndex();
              } else {
                // with sample documents
                Path dataPath = Paths.get(dataDirTF.getText());
                toolsModel.createNewIndex(dataPath.toAbsolutePath().toString());
              }

              indexHandler.open(path.toAbsolutePath().toString(), null, false, false, false);
              prefs.addHistory(path.toAbsolutePath().toString());

              dirnameTF.setText("");
              closeDialog();
            } catch (Exception ex) {
              // cleanup
              try {
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                  @Override
                  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                  }
                });
                Files.deleteIfExists(path);
              } catch (IOException ex2) {
              }

              log.error("Cannot create index", ex);
              String message = "See Logs tab or log file for more details.";
              JOptionPane.showMessageDialog(dialog, message, "Cannot create index", JOptionPane.ERROR_MESSAGE);
            } finally {
              setProgress(100);
            }
            return null;
          }

          @Override
          protected void done() {
            indicatorLbl.setVisible(false);
            createBtn.setEnabled(true);
          }
        };

        executor.submit(task);
        executor.shutdown();
      }
    }

    private void clearDataDir(ActionEvent e) {
      dataDirTF.setText("");
    }

    private void closeDialog() {
      dialog.dispose();
    }
  }
}
