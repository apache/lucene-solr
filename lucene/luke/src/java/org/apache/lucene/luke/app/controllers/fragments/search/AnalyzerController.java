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

package org.apache.lucene.luke.app.controllers.fragments.search;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.layout.VBox;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.luke.app.controllers.LukeController;

import java.util.stream.Collectors;

public class AnalyzerController {

  private static final int LISTVIEW_ROW_HEIGHT = 25;

  private LukeController parent;

  @FXML
  private Label analyzerName;

  @FXML
  private Hyperlink change;

  @FXML
  private VBox analysisChain;

  @FXML
  private ListView<String> charFilters;

  private ObservableList<String> charFilterList;

  @FXML
  private TextField tokenizer;

  @FXML
  private ListView<String> tokenFilters;

  private ObservableList<String> tokenFilterList;

  @FXML
  private void initialize() {
    change.setOnAction(e -> parent.switchTab(LukeController.Tab.ANALYZER));

    analysisChain.setDisable(true);
    charFilterList = FXCollections.observableArrayList();
    charFilters.setItems(charFilterList);
    tokenFilterList = FXCollections.observableArrayList();
    tokenFilters.setItems(tokenFilterList);
  }

  public void setParent(LukeController parent) {
    this.parent = parent;
  }

  public void setCurrentAnalyzer(Analyzer analyzer) {
    analyzerName.setText(analyzer.getClass().getSimpleName());
    charFilterList.clear();
    tokenizer.clear();
    tokenFilterList.clear();

    if (analyzer instanceof CustomAnalyzer) {
      CustomAnalyzer customAnalyzer = (CustomAnalyzer) analyzer;

      charFilterList.addAll(customAnalyzer.getCharFilterFactories().stream()
          .map(f -> f.getClass().getSimpleName()).collect(Collectors.toList()));
      tokenizer.setText(customAnalyzer.getTokenizerFactory().getClass().getSimpleName());
      tokenFilterList.clear();
      tokenFilterList.addAll(customAnalyzer.getTokenFilterFactories().stream()
          .map(f -> f.getClass().getSimpleName()).collect(Collectors.toList()));
      analysisChain.setDisable(false);
    } else {
      analysisChain.setDisable(true);
    }

    charFilters.setPrefHeight(Math.max(LISTVIEW_ROW_HEIGHT * charFilterList.size(), LISTVIEW_ROW_HEIGHT));
    tokenFilters.setPrefHeight(Math.max(LISTVIEW_ROW_HEIGHT * tokenFilterList.size(), LISTVIEW_ROW_HEIGHT));
  }

}
