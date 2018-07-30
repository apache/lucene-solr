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

import javafx.beans.property.BooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseButton;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dto.analysis.FilterFactory;
import org.apache.lucene.luke.app.controllers.fragments.analysis.CustomAnalyzerController;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class EditFiltersController implements DialogWindowController {

  private CustomAnalyzerController customAnalyzerController;

  private Mode mode;

  private Consumer<List<FilterFactory>> callback;

  @FXML
  private TableView<FilterFactory> filtersTable;

  @FXML
  private TableColumn<FilterFactory, Boolean> delColumn;

  @FXML
  private TableColumn<FilterFactory, Integer> orderColumn;

  @FXML
  private TableColumn<FilterFactory, String> factoryColumn;

  private ObservableList<FilterFactory> filterList;

  @FXML
  private Button ok;

  @FXML
  private Button cancel;

  @FXML
  private void initialize() {
    delColumn.setCellValueFactory(data -> {
      BooleanProperty prop = data.getValue().getDeletedProperty();
      prop.addListener((obs, oldV, newV) ->
          filtersTable.getSelectionModel().getSelectedItem().setDeleted(newV)
      );
      return prop;
    });
    delColumn.setCellFactory(col -> {
      CheckBoxTableCell<FilterFactory, Boolean> cell = new CheckBoxTableCell<>();
      cell.setAlignment(Pos.CENTER);
      return cell;
    });
    orderColumn.setCellValueFactory(new PropertyValueFactory<>("order"));
    factoryColumn.setCellValueFactory(new PropertyValueFactory<>("factory"));
    filterList = FXCollections.observableArrayList();
    filtersTable.setItems(filterList);
    filtersTable.setOnMouseClicked(e -> runnableWrapper(() -> {
      if (e.getButton().equals(MouseButton.PRIMARY) &&
          e.getClickCount() == 2) {
        editParams();
      }
    }));

    ok.setOnAction(e -> onOk());
    cancel.setOnAction(e -> closeWindow(cancel));
  }

  private void editParams() throws Exception {
    int selectedIdx = filtersTable.getSelectionModel().getFocusedIndex();
    switch (mode) {
      case CHAR_FILTER:
        customAnalyzerController.editCharFilterParams(selectedIdx);
        break;
      case TOKEN_FILTER:
        customAnalyzerController.editTokenFilterParams(selectedIdx);
        break;
      default:
        break;
    }
  }

  private void onOk() {
    callback.accept(filterList);
    closeWindow(ok);
  }

  public void populate(List<String> filters) {
    List<FilterFactory> li = IntStream.range(0, filters.size())
        .mapToObj(i -> FilterFactory.of(i + 1, filters.get(i)))
        .collect(Collectors.toList());
    filterList.addAll(li);
  }

  public void setCallback(Consumer<List<FilterFactory>> callback) {
    this.callback = callback;
  }

  public void setParent(CustomAnalyzerController customAnalyzerController, Mode mode) {
    this.customAnalyzerController = customAnalyzerController;
    this.mode = mode;
  }

  public enum Mode {
    CHAR_FILTER, TOKEN_FILTER
  }
}
