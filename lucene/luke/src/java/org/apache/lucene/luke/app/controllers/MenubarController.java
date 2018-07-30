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

package org.apache.lucene.luke.app.controllers;

import com.google.inject.Inject;
import javafx.fxml.FXML;
import javafx.scene.control.MenuItem;
import javafx.stage.Stage;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.dialog.menubar.AboutController;
import org.apache.lucene.luke.app.controllers.dialog.menubar.CheckIndexController;
import org.apache.lucene.luke.app.controllers.dialog.menubar.OpenIndexController;
import org.apache.lucene.luke.app.controllers.dialog.menubar.OptimizeController;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.util.DialogOpener;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.util.Version;

import java.io.IOException;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class MenubarController implements IndexObserver, DirectoryObserver {

  private final Preferences prefs;

  private final DirectoryHandler directoryHandler;

  private final IndexHandler indexHandler;

  private LukeController mainController;

  @FXML
  private MenuItem menuOpenIndex;

  @FXML
  private MenuItem menuReopenIndex;

  @FXML
  private MenuItem menuCloseIndex;

  @FXML
  private MenuItem menuExit;

  @FXML
  private MenuItem menuOptimizeIndex;

  @FXML
  private MenuItem menuCheckIndex;

  @FXML
  private MenuItem menuThemeGray;

  @FXML
  private MenuItem menuThemeClassic;

  @FXML
  private MenuItem menuThemeSandstone;

  @FXML
  private MenuItem menuThemeNavy;

  @FXML
  private MenuItem menuAbout;

  @Inject
  public MenubarController(Preferences prefs, DirectoryHandler directoryHandler, IndexHandler indexHandler) {
    this.prefs = prefs;
    this.directoryHandler = directoryHandler;
    this.indexHandler = indexHandler;
  }

  @FXML
  private void initialize() {
    menuOpenIndex.setOnAction(e -> runnableWrapper(this::showOpenIndexDialog));

    menuReopenIndex.setOnAction(e -> runnableWrapper(() -> {
      indexHandler.reOpen();
    }));
    menuReopenIndex.setDisable(true);

    menuCloseIndex.setOnAction(e -> runnableWrapper(() -> {
      directoryHandler.close();
      indexHandler.close();
    }));
    menuCloseIndex.setDisable(true);

    menuExit.setOnAction(e -> exit());

    menuOptimizeIndex.setOnAction(e -> runnableWrapper(this::showOptimizeDialog));
    menuOptimizeIndex.setDisable(true);

    menuCheckIndex.setOnAction(e -> runnableWrapper(this::showCheckIndexDialog));
    menuCheckIndex.setDisable(true);

    menuThemeGray.setOnAction(e -> runnableWrapper(() -> changeTheme(LukeController.ColorTheme.GRAY)));
    menuThemeClassic.setOnAction(e -> runnableWrapper(() -> changeTheme(LukeController.ColorTheme.CLASSIC)));
    menuThemeSandstone.setOnAction(e -> runnableWrapper(() -> changeTheme(LukeController.ColorTheme.SANDSTONE)));
    menuThemeNavy.setOnAction(e -> runnableWrapper(() -> changeTheme(LukeController.ColorTheme.NAVY)));

    menuAbout.setOnAction(e -> runnableWrapper(this::showAboutDialog));
  }

  private Stage openIndexDialog = null;

  public void showOpenIndexDialog() throws Exception {
    openIndexDialog = new DialogOpener<OpenIndexController>(mainController).show(
        openIndexDialog,
        "Choose index directory path",
        "/fxml/dialog/menubar/openindex.fxml",
        600, 400,
        (controller) -> {
          controller.setMainController(mainController);
        }
    );
  }

  private Stage optimizeDialog = null;

  private void showOptimizeDialog() throws Exception {
    optimizeDialog = new DialogOpener<OptimizeController>(mainController).show(
        optimizeDialog,
        "Optimize index",
        "/fxml/dialog/menubar/optimize.fxml",
        600, 600,
        (controller) -> {}
    );
  }

  private Stage checkIndexDialog = null;

  private void showCheckIndexDialog() throws Exception {
    checkIndexDialog = new DialogOpener<CheckIndexController>(mainController).show(
        checkIndexDialog,
        "Check index",
        "/fxml/dialog/menubar/checkindex.fxml",
        600, 600,
        (controller) -> {}
    );
  }

  private void changeTheme(LukeController.ColorTheme theme) throws IOException {
    prefs.setTheme(theme);
    mainController.setColorTheme(theme);
  }

  private Stage aboutDialog = null;

  private void showAboutDialog() throws Exception {
    String version = Version.LATEST.toString();
    aboutDialog = new DialogOpener<AboutController>(mainController).show(
        aboutDialog,
        "About Luke v" + version,
        "/fxml/dialog/menubar/about.fxml",
        1000, 480,
        (controller) -> {},
        "/styles/about.css"
    );

  }

  private void exit() {
    directoryHandler.close();
    indexHandler.close();
    ((Stage) mainController.getPrimaryWindow()).close();
  }

  @Override
  public void openDirectory(LukeState state) {
    menuReopenIndex.setDisable(true);
    menuCloseIndex.setDisable(false);
    menuOptimizeIndex.setDisable(true);
    menuCheckIndex.setDisable(false);
  }

  @Override
  public void openIndex(LukeState state) throws LukeException {
    menuReopenIndex.setDisable(false);
    menuCloseIndex.setDisable(false);
    if (!state.readOnly() && state.hasDirectoryReader()) {
      menuOptimizeIndex.setDisable(false);
    }
    if (state.hasDirectoryReader()) {
      menuCheckIndex.setDisable(false);
    }
  }

  @Override
  public void closeDirectory() {
    close();
  }

  @Override
  public void closeIndex() {
    close();
  }

  private void close() {
    menuReopenIndex.setDisable(true);
    menuCloseIndex.setDisable(true);
    menuOptimizeIndex.setDisable(true);
    menuCheckIndex.setDisable(true);
  }

  void setMainController(LukeController mainController) {
    this.mainController = mainController;
  }

}
