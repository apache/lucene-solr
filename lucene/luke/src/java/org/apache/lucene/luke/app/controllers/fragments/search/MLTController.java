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

import com.google.common.base.Strings;
import javafx.beans.property.BooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.util.converter.IntegerStringConverter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.app.controllers.dto.search.SelectedField;
import org.apache.lucene.luke.app.util.IntegerTextFormatter;
import org.apache.lucene.luke.models.search.MLTConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MLTController {

  private LukeController parent;

  private MLTConfig config = new MLTConfig.Builder().build();

  @FXML
  private TextField maxDocFreq;

  @FXML
  private TextField minDocFreq;

  @FXML
  private TextField minTermFreq;

  @FXML
  private Label analyzerName;

  @FXML
  private Hyperlink change;

  @FXML
  private CheckBox selectAll;

  @FXML
  private TableView<SelectedField> fieldsTable;

  @FXML
  private TableColumn<SelectedField, Boolean> selectColumn;

  @FXML
  private TableColumn<SelectedField, String> fieldColumn;

  private ObservableList<SelectedField> fieldList;

  @FXML
  private void initialize() {
    maxDocFreq.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), config.getMaxDocFreq()));
    minDocFreq.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), config.getMinDocFreq()));
    minTermFreq.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), config.getMinTermFreq()));

    selectAll.setSelected(true);
    selectAll.setOnAction(e ->
        fieldList.forEach(f -> f.setSelected(selectAll.isSelected()))
    );

    change.setOnAction(e -> parent.switchTab(LukeController.Tab.ANALYZER));

    selectColumn.setCellValueFactory(data -> data.getValue().selectedProperty());
    selectColumn.setCellFactory(col -> {
      CheckBoxTableCell<SelectedField, Boolean> cell = new CheckBoxTableCell<>();
      cell.setSelectedStateCallback(idx -> {
        BooleanProperty prop = fieldsTable.getItems().get(idx).selectedProperty();
        if (!prop.get()) {
          selectAll.setSelected(false);
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

  public void setParent(LukeController parent) {
    this.parent = parent;
  }

  public void setCurrentAnalyzer(Analyzer analyzer) {
    this.analyzerName.setText(analyzer.getClass().getSimpleName());
  }

  public void populateFields(Collection<String> fieldNames) {
    fieldList.clear();
    fieldList.addAll(fieldNames.stream()
        .map(SelectedField::of).collect(Collectors.toList()));
  }

  public MLTConfig getMLTConfig() {
    List<String> fields = fieldList.stream()
        .filter(SelectedField::isSelected)
        .map(SelectedField::getField)
        .collect(Collectors.toList());

    return new MLTConfig.Builder()
        .fields(fields)
        .maxDocFreq(textFieldToInt(maxDocFreq))
        .minDocFreq(textFieldToInt(minDocFreq))
        .minTermFreq(textFieldToInt(minTermFreq))
        .build();
  }

  private int textFieldToInt(TextField textField) {
    if (Strings.isNullOrEmpty(textField.getText())) {
      return 0;
    } else {
      return Integer.parseInt(textField.getText());
    }
  }

}
