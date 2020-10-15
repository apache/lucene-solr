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
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSeparator;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.luke.util.reflection.ClassScanner;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;

/** Factory of open index dialog */
public final class OpenIndexDialogFactory implements DialogOpener.DialogFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static OpenIndexDialogFactory instance;

  private final Preferences prefs;

  private final DirectoryHandler directoryHandler;

  private final IndexHandler indexHandler;

  private final JComboBox<String> idxPathCombo = new JComboBox<>();

  private final JButton browseBtn = new JButton();

  private final JCheckBox readOnlyCB = new JCheckBox();

  private final JComboBox<String> dirImplCombo = new JComboBox<>();

  private final JCheckBox noReaderCB = new JCheckBox();

  private final JCheckBox useCompoundCB = new JCheckBox();

  private final JRadioButton keepLastCommitRB = new JRadioButton();

  private final JRadioButton keepAllCommitsRB = new JRadioButton();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private JDialog dialog;

  public synchronized static OpenIndexDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new OpenIndexDialogFactory();
    }
    return instance;
  }

  private OpenIndexDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.directoryHandler = DirectoryHandler.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    initialize();
  }

  private void initialize() {
    idxPathCombo.setPreferredSize(new Dimension(360, 40));

    browseBtn.setText(FontUtils.elegantIconHtml("&#x6e;", MessageUtils.getLocalizedMessage("button.browse")));
    browseBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    browseBtn.setPreferredSize(new Dimension(120, 40));
    browseBtn.addActionListener(listeners::browseDirectory);

    readOnlyCB.setText(MessageUtils.getLocalizedMessage("openindex.checkbox.readonly"));
    readOnlyCB.setSelected(prefs.isReadOnly());
    readOnlyCB.addActionListener(listeners::toggleReadOnly);
    readOnlyCB.setOpaque(false);

    // Scanning all Directory types will take time...
    ExecutorService executorService = Executors.newFixedThreadPool(1, new NamedThreadFactory("load-directory-types"));
    executorService.execute(() -> {
      for (String clazzName : supportedDirImpls()) {
        dirImplCombo.addItem(clazzName);
      }
    });
    executorService.shutdown();
    dirImplCombo.setPreferredSize(new Dimension(350, 30));
    dirImplCombo.setSelectedItem(prefs.getDirImpl());

    noReaderCB.setText(MessageUtils.getLocalizedMessage("openindex.checkbox.no_reader"));
    noReaderCB.setSelected(prefs.isNoReader());
    noReaderCB.setOpaque(false);

    useCompoundCB.setText(MessageUtils.getLocalizedMessage("openindex.checkbox.use_compound"));
    useCompoundCB.setSelected(prefs.isUseCompound());
    useCompoundCB.setOpaque(false);

    keepLastCommitRB.setText(MessageUtils.getLocalizedMessage("openindex.radio.keep_only_last_commit"));
    keepLastCommitRB.setSelected(!prefs.isKeepAllCommits());
    keepLastCommitRB.setOpaque(false);

    keepAllCommitsRB.setText(MessageUtils.getLocalizedMessage("openindex.radio.keep_all_commits"));
    keepAllCommitsRB.setSelected(prefs.isKeepAllCommits());
    keepAllCommitsRB.setOpaque(false);

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
    panel.add(expertSettings());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(buttons());

    return panel;
  }

  private JPanel basicSettings() {
    JPanel panel = new JPanel(new GridLayout(2, 1));
    panel.setOpaque(false);

    JPanel idxPath = new JPanel(new FlowLayout(FlowLayout.LEADING));
    idxPath.setOpaque(false);
    idxPath.add(new JLabel(MessageUtils.getLocalizedMessage("openindex.label.index_path")));

    idxPathCombo.removeAllItems();
    for (String path : prefs.getHistory()) {
      idxPathCombo.addItem(path);
    }
    idxPath.add(idxPathCombo);

    idxPath.add(browseBtn);

    panel.add(idxPath);

    JPanel readOnly = new JPanel(new FlowLayout(FlowLayout.LEADING));
    readOnly.setOpaque(false);
    readOnly.add(readOnlyCB);
    JLabel roIconLB = new JLabel(FontUtils.elegantIconHtml("&#xe06c;"));
    readOnly.add(roIconLB);
    panel.add(readOnly);

    return panel;
  }

  private JPanel expertSettings() {
    JPanel panel = new JPanel(new GridLayout(6, 1));
    panel.setOpaque(false);

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("openindex.label.expert")));
    panel.add(header);

    JPanel dirImpl = new JPanel(new FlowLayout(FlowLayout.LEADING));
    dirImpl.setOpaque(false);
    dirImpl.add(new JLabel(MessageUtils.getLocalizedMessage("openindex.label.dir_impl")));
    dirImpl.add(dirImplCombo);
    panel.add(dirImpl);

    JPanel noReader = new JPanel(new FlowLayout(FlowLayout.LEADING));
    noReader.setOpaque(false);
    noReader.add(noReaderCB);
    JLabel noReaderIcon = new JLabel(FontUtils.elegantIconHtml("&#xe077;"));
    noReader.add(noReaderIcon);
    panel.add(noReader);

    JPanel iwConfig = new JPanel(new FlowLayout(FlowLayout.LEADING));
    iwConfig.setOpaque(false);
    iwConfig.add(new JLabel(MessageUtils.getLocalizedMessage("openindex.label.iw_config")));
    panel.add(iwConfig);

    JPanel compound = new JPanel(new FlowLayout(FlowLayout.LEADING));
    compound.setOpaque(false);
    compound.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    compound.add(useCompoundCB);
    panel.add(compound);

    JPanel keepCommits = new JPanel(new FlowLayout(FlowLayout.LEADING));
    keepCommits.setOpaque(false);
    keepCommits.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    keepCommits.add(keepLastCommitRB);
    keepCommits.add(keepAllCommitsRB);

    ButtonGroup group = new ButtonGroup();
    group.add(keepLastCommitRB);
    group.add(keepAllCommitsRB);

    panel.add(keepCommits);

    return panel;
  }

  private String[] supportedDirImpls() {
    // supports FS-based built-in implementations
    ClassScanner scanner = new ClassScanner("org.apache.lucene.store", getClass().getClassLoader());
    Set<Class<? extends FSDirectory>> clazzSet = scanner.scanSubTypes(FSDirectory.class);

    List<String> clazzNames = new ArrayList<>();
    clazzNames.add(FSDirectory.class.getName());
    clazzNames.addAll(clazzSet.stream().map(Class::getName).collect(Collectors.toList()));

    String[] result = new String[clazzNames.size()];
    return clazzNames.toArray(result);
  }

  private JPanel buttons() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 10, 20));

    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.addActionListener(listeners::openIndexOrDirectory);
    panel.add(okBtn);

    JButton cancelBtn = new JButton(MessageUtils.getLocalizedMessage("button.cancel"));
    cancelBtn.addActionListener(e -> dialog.dispose());
    panel.add(cancelBtn);

    return panel;
  }

  private class ListenerFunctions {

    @SuppressForbidden(reason = "FileChooser#getSelectedFile() returns java.io.File")
    void browseDirectory(ActionEvent e) {
      File currentDir = getLastOpenedDirectory();
      JFileChooser fc = currentDir == null ? new JFileChooser() : new JFileChooser(currentDir);
      fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
      fc.setFileHidingEnabled(false);
      int retVal = fc.showOpenDialog(dialog);
      if (retVal == JFileChooser.APPROVE_OPTION) {
        File dir = fc.getSelectedFile();
        idxPathCombo.insertItemAt(dir.getAbsolutePath(), 0);
        idxPathCombo.setSelectedIndex(0);
      }
    }

    @SuppressForbidden(reason = "JFileChooser constructor takes java.io.File")
    private File getLastOpenedDirectory() {
      List<String> history = prefs.getHistory();
      if (!history.isEmpty()) {
        Path path = Paths.get(history.get(0));
        if (Files.exists(path)) {
          return path.getParent().toAbsolutePath().toFile();
        }
      }
      return null;
    }

    void toggleReadOnly(ActionEvent e) {
      setWriterConfigEnabled(!isReadOnly());
    }

    private void setWriterConfigEnabled(boolean enable) {
      useCompoundCB.setEnabled(enable);
      keepLastCommitRB.setEnabled(enable);
      keepAllCommitsRB.setEnabled(enable);
    }

    void openIndexOrDirectory(ActionEvent e) {
      try {
        if (directoryHandler.directoryOpened()) {
          directoryHandler.close();
        }
        if (indexHandler.indexOpened()) {
          indexHandler.close();
        }

        String selectedPath = (String) idxPathCombo.getSelectedItem();
        String dirImplClazz = (String) dirImplCombo.getSelectedItem();
        if (selectedPath == null || selectedPath.length() == 0) {
          String message = MessageUtils.getLocalizedMessage("openindex.message.index_path_not_selected");
          JOptionPane.showMessageDialog(dialog, message, "Empty index path", JOptionPane.ERROR_MESSAGE);
        } else if (isNoReader()) {
          directoryHandler.open(selectedPath, dirImplClazz);
          addHistory(selectedPath);
        } else {
          indexHandler.open(selectedPath, dirImplClazz, isReadOnly(), useCompound(), keepAllCommits());
          addHistory(selectedPath);
        }
        prefs.setIndexOpenerPrefs(
            isReadOnly(), dirImplClazz,
            isNoReader(), useCompound(), keepAllCommits());
        closeDialog();
      } catch (LukeException ex) {
        String message = ex.getMessage() + System.lineSeparator() + "See Logs tab or log file for more details.";
        JOptionPane.showMessageDialog(dialog, message, "Invalid index path", JOptionPane.ERROR_MESSAGE);
      } catch (Throwable cause) {
        JOptionPane.showMessageDialog(dialog, MessageUtils.getLocalizedMessage("message.error.unknown"), "Unknown Error", JOptionPane.ERROR_MESSAGE);
        log.error("Error opening index or directory", cause);
      }
    }

    private boolean isNoReader() {
      return noReaderCB.isSelected();
    }

    private boolean isReadOnly() {
      return readOnlyCB.isSelected();
    }

    private boolean useCompound() {
      return useCompoundCB.isSelected();
    }

    private boolean keepAllCommits() {
      return keepAllCommitsRB.isSelected();
    }

    private void closeDialog() {
      dialog.dispose();
    }

    private void addHistory(String indexPath) throws IOException {
      prefs.addHistory(indexPath);
    }

  }

}
