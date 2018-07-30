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

package org.apache.lucene.luke.app.controllers.dialog.documents;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dto.documents.TermVector;
import org.apache.lucene.luke.models.documents.TermVectorEntry;

import java.util.List;
import java.util.stream.Collectors;

public class TermVectorController implements DialogWindowController {

  @FXML
  private Label field;

  @FXML
  private TableView<TermVector> termVectorTable;

  @FXML
  private TableColumn<TermVector, String> termColumn;

  @FXML
  private TableColumn<TermVector, Integer> freqColumn;

  @FXML
  private TableColumn<TermVector, String> positionsColumn;

  @FXML
  private TableColumn<TermVector, String> offsetsColumn;

  private ObservableList<TermVector> termVectorList;

  @FXML
  private Button close;

  @FXML
  private void initialize() {
    // initialize term vector table
    termColumn.setCellValueFactory(new PropertyValueFactory<>("termText"));
    freqColumn.setCellValueFactory(new PropertyValueFactory<>("freq"));
    positionsColumn.setCellValueFactory(new PropertyValueFactory<>("positions"));
    offsetsColumn.setCellValueFactory(new PropertyValueFactory<>("offsets"));
    termVectorList = FXCollections.observableArrayList();
    termVectorTable.setItems(termVectorList);

    close.setOnAction(e -> closeWindow(close));
  }

  public void setTermVector(String fieldName, List<TermVectorEntry> tvEntries) {
    field.setText(fieldName);
    termVectorList.clear();
    termVectorList.addAll(tvEntries.stream().map(TermVector::of).collect(Collectors.toList()));
  }
}
