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

import javafx.beans.property.BooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.CheckBox;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.lucene.luke.app.controllers.dto.search.SelectedField;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldValuesController {

  @FXML
  private CheckBox loadAll;

  @FXML
  private TableView<SelectedField> fieldsTable;

  @FXML
  private TableColumn<SelectedField, Boolean> selectColumn;

  @FXML
  private TableColumn<SelectedField, String> fieldColumn;

  private ObservableList<SelectedField> fieldList;

  @FXML
  private void initialize() {
    loadAll.setSelected(true);
    loadAll.setOnAction(e ->
        fieldList.forEach(f -> f.setSelected(loadAll.isSelected()))
    );

    selectColumn.setCellValueFactory(data -> data.getValue().selectedProperty());
    selectColumn.setCellFactory(col -> {
      CheckBoxTableCell<SelectedField, Boolean> cell = new CheckBoxTableCell<>();
      cell.setSelectedStateCallback(idx -> {
        BooleanProperty prop = fieldsTable.getItems().get(idx).selectedProperty();
        if (!prop.get()) {
          loadAll.setSelected(false);
        }
        return prop;
      });
      cell.setAlignment(Pos.CENTER);
      return cell;
    });

    fieldColumn.setCellValueFactory(new PropertyValueFactory<>("field"));

    fieldList = FXCollections.observableArrayList();
    fieldsTable.setItems(fieldList);

  }

  public void populateFields(Collection<String> fieldNames) {
    fieldList.clear();
    fieldList.addAll(fieldNames.stream()
        .map(SelectedField::of).collect(Collectors.toList()));
  }

  public Set<String> getFieldsToLoad() {
    return fieldList.stream()
        .filter(SelectedField::isSelected)
        .map(SelectedField::getField).collect(Collectors.toSet());
  }

  public void setFieldsToLoad(Set<String> fields) {
    loadAll.setSelected(false);
    fieldList.forEach(f -> f.setSelected(fields.contains(f.getField())));
  }
}
