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
import javafx.scene.control.Label;
import javafx.scene.control.MenuBar;
import javafx.scene.control.TabPane;
import javafx.scene.control.Tooltip;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Window;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.util.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class LukeController implements IndexObserver, DirectoryObserver {

  private static final Logger logger = LoggerFactory.getLogger(LukeController.class);

  @FXML
  private AnchorPane primary;

  @FXML
  private MenuBar menuBar;

  @FXML
  private TabPane tabPane;

  @FXML
  private AnchorPane overview;

  @FXML
  private MenubarController menubarController;

  @FXML
  private OverviewController overviewController;

  @FXML
  private DocumentsController documentsController;

  @FXML
  private SearchController searchController;

  @FXML
  private AnalysisController analysisController;

  @FXML
  private CommitsController commitsController;

  @FXML
  private Label statusMessage;

  @FXML
  private ImageView multiIcon;

  @FXML
  private ImageView roIcon;

  @FXML
  private ImageView noReaderIcon;

  private Preferences prefs;

  private DirectoryHandler directoryHandler;

  private IndexHandler indexHandler;

  @FXML
  private void initialize() {
    menubarController.setMainController(this);
    overviewController.setParent(this);
    documentsController.setParent(this);
    searchController.setParent(this);
    analysisController.setParent(this);

    directoryHandler.addObserver(menubarController);
    directoryHandler.addObserver(commitsController);
    directoryHandler.addObserver(this);

    indexHandler.addObserver(menubarController);
    indexHandler.addObserver(overviewController);
    indexHandler.addObserver(documentsController);
    indexHandler.addObserver(searchController);
    indexHandler.addObserver(commitsController);
    indexHandler.addObserver(this);

    // disable tabs until an index opened.
    tabPane.getTabs().get(Tab.OVERVIEW.index()).setDisable(true);
    tabPane.getTabs().get(Tab.DOCUMENTS.index()).setDisable(true);
    tabPane.getTabs().get(Tab.SEARCH.index()).setDisable(true);
    tabPane.getTabs().get(Tab.COMMITS.index()).setDisable(true);

    tabPane.getSelectionModel().selectedIndexProperty().addListener((obs, oldV, newV) ->
        runnableWrapper(() -> {
          clearStatusMessage();
          if (newV.equals(Tab.DOCUMENTS.index())) {
            documentsController.setCurrentAnalyzer(analysisController.getCurrentAnalyzer());
          }
          if (newV.equals(Tab.SEARCH.index())) {
            searchController.setCurrentAnalyzer(analysisController.getCurrentAnalyzer());
          }
        }));

    multiIcon.setVisible(false);
    Tooltip.install(multiIcon, new Tooltip(MessageUtils.getLocalizedMessage("tooltip.multi_reader")));

    roIcon.setVisible(false);
    Tooltip.install(roIcon, new Tooltip(MessageUtils.getLocalizedMessage("tooltip.read_only")));

    noReaderIcon.setVisible(false);
    Tooltip.install(noReaderIcon, new Tooltip(MessageUtils.getLocalizedMessage("tooltip.no_reader")));
  }

  public void showOpenIndexDialog() throws Exception {
    menubarController.showOpenIndexDialog();
  }

  // -------------------------------------------------
  // methods for interaction with other controllers
  // -------------------------------------------------

  public Window getPrimaryWindow() {
    return primary.getScene().getWindow();
  }

  OverviewController getOverviewController() {
    return overviewController;
  }

  DocumentsController getDocumentsController() {
    return documentsController;
  }

  SearchController getSearchController() {
    return searchController;
  }

  @Override
  public void openDirectory(LukeState state) {
    roIcon.setVisible(false);
    noReaderIcon.setVisible(true);

    tabPane.getTabs().get(Tab.COMMITS.index()).setDisable(false);
    showStatusMessage(MessageUtils.getLocalizedMessage("message.directory_opened"));
  }

  @Override
  public void closeDirectory() {
    roIcon.setVisible(false);
    noReaderIcon.setVisible(false);

    switchTab(LukeController.Tab.OVERVIEW);
    showStatusMessage(MessageUtils.getLocalizedMessage("message.directory_closed"));
  }

  @Override
  public void openIndex(LukeState state) {

    if (state.hasDirectoryReader()) {
      multiIcon.setVisible(false);
    } else {
      multiIcon.setVisible(true);
    }

    if (state.readOnly()) {
      roIcon.setVisible(true);
    } else {
      roIcon.setVisible(false);
    }

    noReaderIcon.setVisible(false);

    // enable tabs
    tabPane.getTabs().get(Tab.OVERVIEW.index()).setDisable(false);
    tabPane.getTabs().get(Tab.DOCUMENTS.index()).setDisable(false);
    tabPane.getTabs().get(Tab.SEARCH.index()).setDisable(false);
    if (state.hasDirectoryReader()) {
      tabPane.getTabs().get(Tab.COMMITS.index()).setDisable(false);
    }

    // show Overview tab
    switchTab(LukeController.Tab.OVERVIEW);

    if (state.readOnly()) {
      showStatusMessage(MessageUtils.getLocalizedMessage("message.index_opened_ro"));
    } else if (!state.hasDirectoryReader()) {
      showStatusMessage(MessageUtils.getLocalizedMessage("message.index_opened_multi"));
    } else {
      showStatusMessage(MessageUtils.getLocalizedMessage("message.index_opened"));
    }

    documentsController.setCurrentAnalyzer(analysisController.getCurrentAnalyzer());
    searchController.setCurrentAnalyzer(analysisController.getCurrentAnalyzer());
  }

  @Override
  public void closeIndex() {
    multiIcon.setVisible(false);
    roIcon.setVisible(false);
    noReaderIcon.setVisible(false);

    // disable tabs until index re-opened.
    tabPane.getTabs().get(Tab.OVERVIEW.index()).setDisable(true);
    tabPane.getTabs().get(Tab.DOCUMENTS.index()).setDisable(true);
    tabPane.getTabs().get(Tab.SEARCH.index()).setDisable(true);
    tabPane.getTabs().get(Tab.COMMITS.index()).setDisable(true);

    switchTab(LukeController.Tab.OVERVIEW);
    showStatusMessage(MessageUtils.getLocalizedMessage("message.index_closed"));
  }

  public void switchTab(Tab tab) {
    tabPane.getSelectionModel().select(tab.index());
  }

  @Inject
  public LukeController(Preferences prefs, DirectoryHandler directoryHandler, IndexHandler indexHandler) {
    this.prefs = prefs;
    this.directoryHandler = directoryHandler;
    this.indexHandler = indexHandler;
  }

  void setColorTheme(ColorTheme colorTheme) {
    try {
      prefs.setTheme(colorTheme);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    resetStyles();
  }

  public void resetStyles() {
    getPrimaryWindow().getScene().getStylesheets().clear();
    getPrimaryWindow().getScene().getStylesheets().addAll(
        getClass().getResource("/styles/luke.css").toExternalForm(),
        getClass().getResource(prefs.getTheme().resourceName()).toExternalForm()
    );
  }

  public String getStyleResourceName() {
    return prefs.getTheme().resourceName();
  }

  public void showStatusMessage(String message) {
    statusMessage.setText(message);
  }

  public void showUnknownErrorMessage() {
    statusMessage.setText(MessageUtils.getLocalizedMessage("message.error.unknown"));
  }

  public void clearStatusMessage() {
    statusMessage.setText("");
  }

  public enum Tab {
    OVERVIEW(0), DOCUMENTS(1), SEARCH(2), ANALYZER(3), COMMITS(4);

    private int tabIdx;

    Tab(int tabIdx) {
      this.tabIdx = tabIdx;
    }

    int index() {
      return tabIdx;
    }
  }

  public enum ColorTheme {
    GRAY, CLASSIC, SANDSTONE, NAVY;

    String resourceName() {
      return String.format("/styles/theme_%s.css", name().toLowerCase());
    }
  }
}
