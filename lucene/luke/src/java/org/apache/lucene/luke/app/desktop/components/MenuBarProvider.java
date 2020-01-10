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

package org.apache.lucene.luke.app.desktop.components;

import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import java.awt.event.ActionEvent;
import java.io.IOException;

import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.AboutDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.CheckIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.CreateIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.ExportTermsDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OptimizeIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.util.Version;

/** Provider of the MenuBar */
public final class MenuBarProvider {

  private final Preferences prefs;

  private final ComponentOperatorRegistry operatorRegistry;

  private final DirectoryHandler directoryHandler;

  private final IndexHandler indexHandler;

  private final OpenIndexDialogFactory openIndexDialogFactory;

  private final CreateIndexDialogFactory createIndexDialogFactory;

  private final OptimizeIndexDialogFactory optimizeIndexDialogFactory;

  private final ExportTermsDialogFactory exportTermsDialogFactory;

  private final CheckIndexDialogFactory checkIndexDialogFactory;

  private final AboutDialogFactory aboutDialogFactory;

  private final JMenuItem openIndexMItem = new JMenuItem();

  private final JMenuItem reopenIndexMItem = new JMenuItem();

  private final JMenuItem createIndexMItem = new JMenuItem();

  private final JMenuItem closeIndexMItem = new JMenuItem();

  private final JMenuItem grayThemeMItem = new JMenuItem();

  private final JMenuItem classicThemeMItem = new JMenuItem();

  private final JMenuItem sandstoneThemeMItem = new JMenuItem();

  private final JMenuItem navyThemeMItem = new JMenuItem();

  private final JMenuItem exitMItem = new JMenuItem();

  private final JMenuItem optimizeIndexMItem = new JMenuItem();

  private final JMenuItem exportTermsMItem = new JMenuItem();

  private final JMenuItem checkIndexMItem = new JMenuItem();

  private final JMenuItem aboutMItem = new JMenuItem();

  private final ListenerFunctions listeners = new ListenerFunctions();

  public MenuBarProvider() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.directoryHandler = DirectoryHandler.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.openIndexDialogFactory = OpenIndexDialogFactory.getInstance();
    this.createIndexDialogFactory = CreateIndexDialogFactory.getInstance();
    this.optimizeIndexDialogFactory = OptimizeIndexDialogFactory.getInstance();
    this.exportTermsDialogFactory = ExportTermsDialogFactory.getInstance();
    this.checkIndexDialogFactory = CheckIndexDialogFactory.getInstance();
    this.aboutDialogFactory = AboutDialogFactory.getInstance();

    Observer observer = new Observer();
    directoryHandler.addObserver(observer);
    indexHandler.addObserver(observer);
  }

  public JMenuBar get() {
    JMenuBar menuBar = new JMenuBar();

    menuBar.add(createFileMenu());
    menuBar.add(createToolsMenu());
    menuBar.add(createHelpMenu());

    return menuBar;
  }

  private JMenu createFileMenu() {
    JMenu fileMenu = new JMenu(MessageUtils.getLocalizedMessage("menu.file"));

    openIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.open_index"));
    openIndexMItem.addActionListener(listeners::showOpenIndexDialog);
    fileMenu.add(openIndexMItem);

    reopenIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.reopen_index"));
    reopenIndexMItem.setEnabled(false);
    reopenIndexMItem.addActionListener(listeners::reopenIndex);
    fileMenu.add(reopenIndexMItem);

    createIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.create_index"));
    createIndexMItem.addActionListener(listeners::showCreateIndexDialog);
    fileMenu.add(createIndexMItem);


    closeIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.close_index"));
    closeIndexMItem.setEnabled(false);
    closeIndexMItem.addActionListener(listeners::closeIndex);
    fileMenu.add(closeIndexMItem);

    fileMenu.addSeparator();

    JMenu settingsMenu = new JMenu(MessageUtils.getLocalizedMessage("menu.settings"));
    JMenu themeMenu = new JMenu(MessageUtils.getLocalizedMessage("menu.color"));
    grayThemeMItem.setText(MessageUtils.getLocalizedMessage("menu.item.theme_gray"));
    grayThemeMItem.addActionListener(listeners::changeThemeToGray);
    themeMenu.add(grayThemeMItem);
    classicThemeMItem.setText(MessageUtils.getLocalizedMessage("menu.item.theme_classic"));
    classicThemeMItem.addActionListener(listeners::changeThemeToClassic);
    themeMenu.add(classicThemeMItem);
    sandstoneThemeMItem.setText(MessageUtils.getLocalizedMessage("menu.item.theme_sandstone"));
    sandstoneThemeMItem.addActionListener(listeners::changeThemeToSandstone);
    themeMenu.add(sandstoneThemeMItem);
    navyThemeMItem.setText(MessageUtils.getLocalizedMessage("menu.item.theme_navy"));
    navyThemeMItem.addActionListener(listeners::changeThemeToNavy);
    themeMenu.add(navyThemeMItem);
    settingsMenu.add(themeMenu);
    fileMenu.add(settingsMenu);

    fileMenu.addSeparator();

    exitMItem.setText(MessageUtils.getLocalizedMessage("menu.item.exit"));
    exitMItem.addActionListener(listeners::exit);
    fileMenu.add(exitMItem);

    return fileMenu;
  }

  private JMenu createToolsMenu() {
    JMenu toolsMenu = new JMenu(MessageUtils.getLocalizedMessage("menu.tools"));
    optimizeIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.optimize"));
    optimizeIndexMItem.setEnabled(false);
    optimizeIndexMItem.addActionListener(listeners::showOptimizeIndexDialog);
    toolsMenu.add(optimizeIndexMItem);
    checkIndexMItem.setText(MessageUtils.getLocalizedMessage("menu.item.check_index"));
    checkIndexMItem.setEnabled(false);
    checkIndexMItem.addActionListener(listeners::showCheckIndexDialog);
    toolsMenu.add(checkIndexMItem);
    exportTermsMItem.setText(MessageUtils.getLocalizedMessage("menu.item.export.terms"));
    exportTermsMItem.setEnabled(false);
    exportTermsMItem.addActionListener(listeners::showExportTermsDialog);
    toolsMenu.add(exportTermsMItem);
    return toolsMenu;
  }

  private JMenu createHelpMenu() {
    JMenu helpMenu = new JMenu(MessageUtils.getLocalizedMessage("menu.help"));
    aboutMItem.setText(MessageUtils.getLocalizedMessage("menu.item.about"));
    aboutMItem.addActionListener(listeners::showAboutDialog);
    helpMenu.add(aboutMItem);
    return helpMenu;
  }

  private class ListenerFunctions {

    void showOpenIndexDialog(ActionEvent e) {
      new DialogOpener<>(openIndexDialogFactory).open(MessageUtils.getLocalizedMessage("openindex.dialog.title"), 600, 420,
          (factory) -> {});
    }

    void showCreateIndexDialog(ActionEvent e) {
      new DialogOpener<>(createIndexDialogFactory).open(MessageUtils.getLocalizedMessage("createindex.dialog.title"), 600, 360,
          (factory) -> {});
    }

    void reopenIndex(ActionEvent e) {
      indexHandler.reOpen();
    }

    void closeIndex(ActionEvent e) {
      close();
    }

    void changeThemeToGray(ActionEvent e) {
      changeTheme(Preferences.ColorTheme.GRAY);
    }

    void changeThemeToClassic(ActionEvent e) {
      changeTheme(Preferences.ColorTheme.CLASSIC);
    }

    void changeThemeToSandstone(ActionEvent e) {
      changeTheme(Preferences.ColorTheme.SANDSTONE);
    }

    void changeThemeToNavy(ActionEvent e) {
      changeTheme(Preferences.ColorTheme.NAVY);
    }

    private void changeTheme(Preferences.ColorTheme theme) {
      try {
        prefs.setColorTheme(theme);
        operatorRegistry.get(LukeWindowOperator.class).ifPresent(operator -> operator.setColorTheme(theme));
      } catch (IOException e) {
        throw new LukeException("Failed to set color theme : " + theme.name(), e);
      }
    }

    void exit(ActionEvent e) {
      close();
      System.exit(0);
    }

    private void close() {
      directoryHandler.close();
      indexHandler.close();
    }

    void showOptimizeIndexDialog(ActionEvent e) {
      new DialogOpener<>(optimizeIndexDialogFactory).open("Optimize index", 600, 600,
          factory -> {
          });
    }

    void showCheckIndexDialog(ActionEvent e) {
      new DialogOpener<>(checkIndexDialogFactory).open("Check index", 600, 600,
          factory -> {
          });
    }

    void showAboutDialog(ActionEvent e) {
      final String title = "About Luke v" + Version.LATEST.toString();
      new DialogOpener<>(aboutDialogFactory).open(title, 800, 480,
          factory -> {
          });
    }

    void showExportTermsDialog(ActionEvent e) {
      new DialogOpener<>(exportTermsDialogFactory).open("Export terms", 600, 450,
          factory -> {
          });
    }

  }

  private class Observer implements IndexObserver, DirectoryObserver {

    @Override
    public void openDirectory(LukeState state) {
      reopenIndexMItem.setEnabled(false);
      closeIndexMItem.setEnabled(false);
      optimizeIndexMItem.setEnabled(false);
      exportTermsMItem.setEnabled(false);
      checkIndexMItem.setEnabled(true);
    }

    @Override
    public void closeDirectory() {
      close();
    }

    @Override
    public void openIndex(LukeState state) {
      reopenIndexMItem.setEnabled(true);
      closeIndexMItem.setEnabled(true);
      exportTermsMItem.setEnabled(true);
      if (!state.readOnly() && state.hasDirectoryReader()) {
        optimizeIndexMItem.setEnabled(true);
      }
      if (state.hasDirectoryReader()) {
        checkIndexMItem.setEnabled(true);
      }
    }

    @Override
    public void closeIndex() {
      close();
    }

    private void close() {
      reopenIndexMItem.setEnabled(false);
      closeIndexMItem.setEnabled(false);
      optimizeIndexMItem.setEnabled(false);
      checkIndexMItem.setEnabled(false);
      exportTermsMItem.setEnabled(false);
    }

  }
}
