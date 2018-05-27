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

package org.apache.lucene.luke.app.controllers.fragments.analysis;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import org.apache.lucene.luke.app.controllers.AnalysisController;
import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.app.controllers.dialog.analysis.EditFiltersController;
import org.apache.lucene.luke.app.controllers.dialog.analysis.EditParamsController;
import org.apache.lucene.luke.app.util.DialogOpener;
import org.apache.lucene.luke.models.analysis.Analysis;
import org.apache.lucene.luke.models.analysis.CustomAnalyzerConfig;
import org.apache.lucene.luke.app.util.MessageUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class CustomAnalyzerController implements AnalyzerController {

  private static final String DEFAULT_TOKENIZER = "org.apache.lucene.analysis.standard.StandardTokenizerFactory";

  private AnalysisController analysisController;

  private LukeController parent;

  @FXML
  private ChoiceBox<String> charFilter;

  private ObservableList<String> availableCharFilters;

  @FXML
  private Button editCF;

  @FXML
  private ChoiceBox<String> tokenizer;

  private ObservableList<String> availableTokenizers;

  @FXML
  private Button editTkn;

  @FXML
  private ChoiceBox<String> tokenFilter;

  private ObservableList<String> availableTokenFilters;

  @FXML
  private Button editTF;

  @FXML
  private ListView<String> selectedCFs;

  private ObservableList<String> selectedCharFilterList;

  private List<Map<String, String>> paramListCFs = new ArrayList<>();

  @FXML
  private TextField selectedTkn;

  private Map<String, String> paramMapTkn;

  @FXML
  private ListView<String> selectedTFs;

  private ObservableList<String> selectedTokenFilterList;

  private List<Map<String, String>> paramListTFs = new ArrayList<>();

  @FXML
  private void initialize() {
    availableCharFilters = FXCollections.observableArrayList();
    charFilter.setItems(availableCharFilters);
    charFilter.setOnAction(e -> runnableWrapper(this::addCharFilter));
    selectedCharFilterList = FXCollections.observableArrayList();
    selectedCFs.setItems(selectedCharFilterList);
    editCF.setOnAction(e -> runnableWrapper(this::editCharFilters));
    editCF.setDisable(true);

    availableTokenizers = FXCollections.observableArrayList();
    tokenizer.setItems(availableTokenizers);
    tokenizer.setOnAction(e -> runnableWrapper(this::setTokenizer));
    editTkn.setOnAction(e -> runnableWrapper(this::editTokenizerParams));

    availableTokenFilters = FXCollections.observableArrayList();
    tokenFilter.setItems(availableTokenFilters);
    tokenFilter.setOnAction(e -> runnableWrapper(this::addTokenFilter));
    selectedTokenFilterList = FXCollections.observableArrayList();
    selectedTFs.setItems(selectedTokenFilterList);
    editTF.setOnAction(e -> runnableWrapper(this::editTokenFilters));
    editTF.setDisable(true);
  }

  private Stage editParamCFDialog = null;

  private void addCharFilter() throws Exception {
    String filterClazz = charFilter.getValue();
    editParamCFDialog = addFilter(
        editParamCFDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.char_filter_params"),
        filterClazz,
        selectedCharFilterList, paramListCFs, charFilter);
  }

  public void editCharFilterParams(int idx) throws Exception {
    editParamCFDialog = editFilterParams(
        editParamCFDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.char_filter_params"),
        idx,
        selectedCharFilterList, paramListCFs);
  }

  private Stage editCharFiltersDialog = null;

  private void editCharFilters() throws Exception {
    editCharFiltersDialog = editFilterList(
        editCharFiltersDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.selected_char_filter"),
        EditFiltersController.Mode.CHAR_FILTER,
        selectedCharFilterList, paramListCFs);
  }

  private Stage editParamTknDialog = null;
  private boolean openEditParamTknDialog = false;

  private void setTokenizer() throws Exception {
    if (!openEditParamTknDialog) {
      // will be called in populate() at the initializing stage.
      // do not show the dialog in that case.
      openEditParamTknDialog = true;
      return;
    }
    String[] tmp = tokenizer.getValue().split("\\.");
    String name = tmp[tmp.length - 1];
    editParamTknDialog = showEditParametersDialog(
        editParamTknDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.tokenizer_params"),
        (controller) -> {
          controller.setTarget(name);
          controller.populate(null);
          controller.setCallback(paramList -> {
            Map<String, String> paramMap = new HashMap<>();
            paramList.stream()
                .filter(p -> !p.isDeleted())
                .filter(p -> p.isValid())
                .forEach(p -> paramMap.put(p.getName(), p.getValue()));
            paramMapTkn = paramMap;
            selectedTkn.setText(tokenizer.getValue());
            toggleButtons();
          });
        });
  }

  private void editTokenizerParams() throws Exception {
    String[] tmp = tokenizer.getValue().split("\\.");
    String name = tmp[tmp.length - 1];
    editParamTknDialog = showEditParametersDialog(
        editParamTknDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.tokenizer_params"),
        (controller) -> {
          controller.setTarget(name);
          controller.populate(new LinkedHashMap<>(paramMapTkn));
          controller.setCallback(paramList -> {
            Map<String, String> paramMap = new HashMap<>();
            paramList.stream()
                .filter(p -> !p.isDeleted())
                .filter(p -> p.isValid())
                .forEach(p -> paramMap.put(p.getName(), p.getValue()));
            paramMapTkn = paramMap;
            toggleButtons();
          });
        });
  }

  private Stage editParamTFDialog = null;

  private void addTokenFilter() throws Exception {
    String filterClazz = tokenFilter.getValue();
    editParamTFDialog = addFilter(
        editParamTFDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.token_filter_params"),
        filterClazz,
        selectedTokenFilterList, paramListTFs, tokenFilter);
  }

  public void editTokenFilterParams(int idx) throws Exception {
    editParamTFDialog = editFilterParams(
        editParamTFDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.token_filter_params"),
        idx,
        selectedTokenFilterList, paramListTFs);
  }

  private Stage editTokenFiltersDialog = null;

  private void editTokenFilters() throws Exception {
    editTokenFiltersDialog = editFilterList(
        editTokenFiltersDialog,
        MessageUtils.getLocalizedMessage("analysis.dialog.title.selected_token_filter"),
        EditFiltersController.Mode.TOKEN_FILTER,
        selectedTokenFilterList, paramListTFs);
  }

  private Stage addFilter(Stage stage, String title,
                          String filterClazz,
                          ObservableList<String> filterList,
                          List<Map<String, String>> filterParamList,
                          ChoiceBox<String> choiceBox) throws Exception {
    if (filterClazz == null) {
      return null;
    }
    String[] tmp = filterClazz.split("\\.");
    String name = tmp[tmp.length - 1];
    return showEditParametersDialog(stage, title,
        (controller) -> {
          controller.setTarget(name);
          controller.populate(null);
          controller.setCallback(paramList -> {
            Map<String, String> paramMap = new HashMap<>();
            paramList.stream()
                .filter(p -> !p.isDeleted())
                .filter(p -> p.isValid())
                .forEach(p -> paramMap.put(p.getName(), p.getValue()));
            filterParamList.add(paramMap);
            filterList.add(filterClazz);
            choiceBox.getSelectionModel().clearSelection();
            toggleButtons();
          });
        });
  }

  private Stage editFilterParams(Stage dialog, String title, int idx,
                                 ObservableList<String> filterList,
                                 List<Map<String, String>> filterParamList) throws Exception {
    if (idx >= filterList.size()) {
      throw new IllegalArgumentException("Invalid index: " + idx);
    }
    String[] tmp = filterList.get(idx).split("\\.");
    String name = tmp[tmp.length - 1];
    return showEditParametersDialog(dialog, title,
        (controller) -> {
          controller.setTarget(name);
          controller.populate(new LinkedHashMap<>(filterParamList.get(idx)));
          controller.setCallback(paramList -> {
            Map<String, String> paramMap = new HashMap<>();
            paramList.stream()
                .filter(p -> !p.isDeleted())
                .filter(p -> p.isValid())
                .forEach(p -> paramMap.put(p.getName(), p.getValue()));
            filterParamList.remove(idx);
            filterParamList.add(idx, paramMap);
            toggleButtons();
          });
        });
  }

  private Stage editFilterList(Stage dialog, String title, EditFiltersController.Mode mode,
                               ObservableList<String> filterList,
                               List<Map<String, String>> filterParamList) throws Exception {
    return new DialogOpener<EditFiltersController>(parent).show(
        dialog,
        title,
        "/fxml/dialog/analysis/edit_filters.fxml",
        400, 300,
        (controller) -> {
          controller.setParent(this, mode);
          controller.populate(
              filterList.stream().map(f -> {
                String tmp[] = f.split("\\.");
                return tmp[tmp.length - 1];
              }).collect(Collectors.toList()));
          controller.setCallback(list -> {
            List<Map<String, String>> newParamList = new ArrayList<>();
            List<String> newTFList = new ArrayList<>();
            IntStream.range(0, list.size()).filter(i -> !list.get(i).isDeleted())
                .forEach(i -> {
                  newParamList.add(filterParamList.get(i));
                  newTFList.add(filterList.get(i));
                });
            filterParamList.clear();
            filterParamList.addAll(newParamList);
            filterList.clear();
            filterList.addAll(newTFList);
            toggleButtons();
          });
        }
    );
  }

  private Stage showEditParametersDialog(Stage dialog, String title, Consumer<EditParamsController> initializer)
      throws Exception {
    return new DialogOpener<EditParamsController>(parent).show(
        dialog,
        title,
        "/fxml/dialog/analysis/edit_params.fxml",
        400, 300,
        initializer
    );
  }

  private void toggleButtons() {
    editCF.setDisable(selectedCharFilterList.isEmpty());
    editTF.setDisable(selectedTokenFilterList.isEmpty());
    analysisController.enableBuildButton();
  }



  @Override
  public void setParent(AnalysisController analysisController, LukeController parent) {
    this.analysisController = analysisController;
    this.parent = parent;
  }

  @Override
  public void populate(Analysis modelAnalysis) {
    availableCharFilters.clear();
    availableCharFilters.addAll(modelAnalysis.getAvailableCharFilterFactories().stream()
        .map(Class::getName)
        .collect(Collectors.toList()));

    availableTokenFilters.clear();
    availableTokenizers.addAll(modelAnalysis.getAvailableTokenizerFactories().stream()
        .map(Class::getName)
        .collect(Collectors.toList()));
    tokenizer.setValue(DEFAULT_TOKENIZER);
    selectedTkn.setText(DEFAULT_TOKENIZER);
    paramMapTkn = new HashMap<>();

    availableTokenFilters.clear();
    availableTokenFilters.addAll(modelAnalysis.getAvailableTokenFilterFactories().stream()
        .map(Class::getName)
        .collect(Collectors.toList()));
  }

  @Override
  public void resetSelectedAnalyzer() {
    analysisController.buildCustomAnalyzer();
  }

  public CustomAnalyzerConfig getAnalyzerConfig(String configDir) {
    CustomAnalyzerConfig.Builder builder =
        new CustomAnalyzerConfig.Builder(selectedTkn.getText(), paramMapTkn).configDir(configDir);

    IntStream.range(0, Math.min(selectedCharFilterList.size(), paramListCFs.size()))
        .forEach(i -> builder.addCharFilterConfig(selectedCharFilterList.get(i), paramListCFs.get(i)));
    IntStream.range(0, Math.min(selectedTokenFilterList.size(), paramListTFs.size()))
        .forEach(i -> builder.addTokenFilterConfig(selectedTokenFilterList.get(i), paramListTFs.get(i)));

    return builder.build();
  }
}
