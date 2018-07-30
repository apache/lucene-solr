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
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.FlowPane;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.luke.app.controllers.dialog.analysis.TokenAttributeController;
import org.apache.lucene.luke.app.controllers.dto.analysis.Token;
import org.apache.lucene.luke.app.controllers.fragments.analysis.AnalyzerController;
import org.apache.lucene.luke.app.controllers.fragments.analysis.CustomAnalyzerController;
import org.apache.lucene.luke.app.controllers.fragments.analysis.PresetAnalyzerController;
import org.apache.lucene.luke.app.util.DialogOpener;
import org.apache.lucene.luke.models.analysis.Analysis;
import org.apache.lucene.luke.models.analysis.AnalysisFactory;
import org.apache.lucene.luke.models.analysis.CustomAnalyzerConfig;
import org.apache.lucene.luke.app.util.MessageUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class AnalysisController extends ChildTabController {

  private static final String ANALYZER_PRESET = "preset";
  private static final String ANALYZER_CUSTOM = "custom";

  private Analysis analysisModel;

  @FXML
  private ToggleGroup analyzerGroup;

  @FXML
  private FlowPane customSettings;

  @FXML
  private TextField config;

  private String configDir;

  @FXML
  private Button browse;

  @FXML
  private Button build;

  @FXML
  private Hyperlink loadJars;

  @FXML
  private AnchorPane analyzerPane;

  @FXML
  private Label selectedAnalyzer;

  @FXML
  private TextArea inputText;

  @FXML
  private Button test;

  @FXML
  private TableView<Token> tokenTable;

  private ObservableList<Token> tokenList;

  @FXML
  private TableColumn<Token, String> termColumn;

  @FXML
  private TableColumn<Token, String> attColumn;

  @FXML
  private PresetAnalyzerController presetAnalyzerController;

  @FXML
  private CustomAnalyzerController customAnalyzerController;

  @Inject
  public AnalysisController(AnalysisFactory analysisFactory) {
    this.analysisModel = analysisFactory.newInstance();
  }

  @FXML
  private void initialize() throws Exception {
    displayAnalyzerPane(ANALYZER_PRESET);
    customSettings.setDisable(true);
    analyzerGroup.selectedToggleProperty().addListener((obs, oldV, newV) -> runnableWrapper(() -> {
      displayAnalyzerPane(newV.getUserData().toString());
      customSettings.setDisable(!customSettings.isDisabled());
    }));
    browse.setOnAction(e -> showBrowseDialog());

    build.setOnAction(e -> runnableWrapper(() -> {
      buildCustomAnalyzer();
      build.setDisable(true);
    }));

    loadJars.setOnAction(e -> runnableWrapper(this::showBrowseExternalJarsDialog));

    // initialize tokens table
    termColumn.setCellValueFactory(new PropertyValueFactory<>("term"));
    attColumn.setCellValueFactory(new PropertyValueFactory<>("attributes"));
    tokenList = FXCollections.observableArrayList();
    tokenTable.setItems(tokenList);
    tokenTable.setOnMouseClicked(e -> runnableWrapper(() -> {
      if (e.getButton().equals(MouseButton.PRIMARY) &&
          e.getClickCount() == 2) {
        showTokenAttributes();
      }
    }));


    test.setOnAction(e -> runnableWrapper(this::testAnalyze));
  }

  private void showBrowseDialog() {
    DirectoryChooser directoryChooser = new DirectoryChooser();
    File homeDir = new File(System.getProperty("user.home"));
    directoryChooser.setInitialDirectory(homeDir);
    Window window = config.getScene().getWindow();
    File file = directoryChooser.showDialog(window);
    if (file != null) {
      configDir = file.getAbsolutePath();
      String[] path = file.getAbsolutePath().split(File.separator);
      config.setText(path[path.length - 1]);
    }
  }

  private void showBrowseExternalJarsDialog() throws Exception {
    FileChooser fileChooser = new FileChooser();
    File homeDir = new File(System.getProperty("user.home"));
    fileChooser.setInitialDirectory(homeDir);
    Window window = loadJars.getScene().getWindow();
    List<File> files = fileChooser.showOpenMultipleDialog(window);
    if (files != null) {
      analysisModel.addExternalJars(
          files.stream().map(File::getAbsolutePath).collect(Collectors.toList())
      );
      customAnalyzerController.populate(analysisModel);
      showStatusMessage("External jars added.");
    }
  }

  private Map<String, Parent> analyzerNodeMap = new HashMap<>();

  private void displayAnalyzerPane(@Nonnull String name) throws Exception {
    Parent root = analyzerNodeMap.get(name);
    if (root == null) {
      String resourceName = String.format("/fxml/fragments/analysis/analysis_%s.fxml", name);
      FXMLLoader loader = new FXMLLoader(getClass().getResource(resourceName), MessageUtils.getBundle());
      root = loader.load();
      AnalyzerController controller = loader.getController();
      controller.setParent(this, getParent());
      controller.populate(analysisModel);

      if (controller instanceof PresetAnalyzerController) {
        this.presetAnalyzerController = (PresetAnalyzerController) controller;
      } else if (controller instanceof CustomAnalyzerController) {
        this.customAnalyzerController = (CustomAnalyzerController) controller;
      }

      analyzerNodeMap.put(name, root);
    }

    analyzerPane.getChildren().clear();
    analyzerPane.getChildren().add(root);

    switch (name) {
      case ANALYZER_PRESET:
        presetAnalyzerController.resetSelectedAnalyzer();
        break;
      case ANALYZER_CUSTOM:
        customAnalyzerController.resetSelectedAnalyzer();
        break;
      default:
        break;
    }
  }

  private void testAnalyze() {
    String text = inputText.getText();
    if (text == null || text.length() == 0) {
      showStatusMessage(MessageUtils.getLocalizedMessage("analysis.message.empry_input"));
      return;
    }
    List<Analysis.Token> tokens = analysisModel.analyze(text);
    tokenList.clear();
    tokenList.addAll(tokens.stream().map(Token::of).collect(Collectors.toList()));
    clearStatusMessage();
  }

  private Stage tokenAttDialog;

  private void showTokenAttributes() throws Exception {
    Analysis.Token token = tokenList.get(tokenTable.getSelectionModel().getFocusedIndex()).getOriginalToken();
    String termText = token.getTerm();
    tokenAttDialog = new DialogOpener<TokenAttributeController>(getParent()).show(
        tokenAttDialog,
        "Token Attributes",
        "/fxml/dialog/analysis/tokenattribute.fxml",
        600, 400,
        (controller) -> {
          controller.populate(termText, token.getAttributes());
        }
    );
    clearStatusMessage();
  }

  Analyzer getCurrentAnalyzer() {
    return analysisModel.currentAnalyzer();
  }

  public void createPresetAnalyzer(String analyzerType) {
    Analyzer analyzer = analysisModel.createAnalyzerFromClassName(analyzerType);
    selectedAnalyzer.setText(analyzer.getClass().getName());
  }

  public void buildCustomAnalyzer() {
    CustomAnalyzerConfig config = customAnalyzerController.getAnalyzerConfig(configDir);
    Analyzer analyzer = analysisModel.buildCustomAnalyzer(config);
    selectedAnalyzer.setText(analyzer.getClass().getName());
    showStatusMessage(MessageUtils.getLocalizedMessage("analysis.message.build_success"));
  }

  public void enableBuildButton() {
    build.setDisable(false);
  }
}
