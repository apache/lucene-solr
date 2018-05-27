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

package org.apache.lucene.luke.app.controllers.dialog.analysis;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dto.analysis.TokenAttValue;
import org.apache.lucene.luke.models.analysis.Analysis;

import java.util.List;
import java.util.stream.Collectors;

public class TokenAttributeController implements DialogWindowController {

  @FXML
  private Label term;

  @FXML
  private TableView<TokenAttValue> attTable;

  @FXML
  private TableColumn<TokenAttValue, String> attClassColumn;

  @FXML
  private TableColumn<TokenAttValue, String> nameColumn;

  @FXML
  private TableColumn<TokenAttValue, String> valueColumn;

  private ObservableList<TokenAttValue> attList;

  @FXML
  private Button ok;

  @FXML
  private void initialize() {
    attClassColumn.setCellValueFactory(new PropertyValueFactory<>("attClass"));
    nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
    valueColumn.setCellValueFactory(new PropertyValueFactory<>("value"));
    attList = FXCollections.observableArrayList();
    attTable.setItems(attList);

    ok.setOnAction(e -> closeWindow(ok));
  }

  public void populate(String termText, List<Analysis.TokenAttribute> attributes) {
    term.setText(termText);
    attList.addAll(attributes.stream()
        .flatMap(att -> att.getAttValues().entrySet().stream()
            .map(e -> TokenAttValue.of(att.getAttClass(), e.getKey(), e.getValue())
            )).collect(Collectors.toList()));
  }
}
