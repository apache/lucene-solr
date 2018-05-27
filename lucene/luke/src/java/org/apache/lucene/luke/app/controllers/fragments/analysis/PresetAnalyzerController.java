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
import javafx.scene.control.ChoiceBox;
import org.apache.lucene.luke.app.controllers.AnalysisController;
import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.analysis.Analysis;

import java.util.stream.Collectors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class PresetAnalyzerController implements AnalyzerController {

  private static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  private AnalysisController analysisController;

  private LukeController parent;

  @FXML
  private ChoiceBox<String> presetAnalyzers;

  private ObservableList<String> presetAnalyzerList;

  @FXML
  private void initialize() {
    presetAnalyzerList = FXCollections.observableArrayList();
    presetAnalyzers.setItems(presetAnalyzerList);
    presetAnalyzers.setOnAction(e -> runnableWrapper(this::selectAnalyzer));
  }

  private void selectAnalyzer() throws LukeException {
    String analyzserType = presetAnalyzers.getValue();
    analysisController.createPresetAnalyzer(analyzserType);
  }

  @Override
  public void setParent(AnalysisController analysisController, LukeController parent) {
    this.analysisController = analysisController;
    this.parent = parent;
  }

  @Override
  public void populate(Analysis modelAnalysis) {
    presetAnalyzerList.addAll(modelAnalysis.getPresetAnalyzerTypes()
        .stream()
        .map(Class::getName)
        .collect(Collectors.toList()));
    presetAnalyzers.setValue(DEFAULT_ANALYZER);

  }

  @Override
  public void resetSelectedAnalyzer() throws Exception {
    selectAnalyzer();
  }


}
