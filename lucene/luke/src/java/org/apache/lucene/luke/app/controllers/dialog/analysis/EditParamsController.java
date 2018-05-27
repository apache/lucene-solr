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
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldTableCell;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dto.analysis.Param;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EditParamsController implements DialogWindowController {

  private Consumer<List<Param>> callback;

  @FXML
  private Label target;

  @FXML
  private TableView<Param> paramsTable;

  @FXML
  private TableColumn<Param, Boolean> delColumn;

  @FXML
  private TableColumn<Param, String> nameColumn;

  @FXML
  private TableColumn<Param, String> valueColumn;

  private ObservableList<Param> paramList;

  @FXML
  private Button ok;

  @FXML
  private Button cancel;

  @FXML
  private void initialize() {
    delColumn.setCellValueFactory(data -> {
      BooleanProperty prop = data.getValue().getDeletedProperty();
      prop.addListener((obs, oldV, newV) ->
          paramsTable.getSelectionModel().getSelectedItem().setDeleted(newV)
      );
      return prop;
    });
    delColumn.setCellFactory(col -> {
      CheckBoxTableCell<Param, Boolean> cell = new CheckBoxTableCell<>();
      cell.setAlignment(Pos.CENTER);
      return cell;
    });

    nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
    nameColumn.setCellFactory(TextFieldTableCell.forTableColumn());
    nameColumn.setOnEditCommit(e -> {
      int rowIdx = paramsTable.getSelectionModel().getFocusedIndex();
      paramList.get(rowIdx).setName(e.getNewValue());
    });

    valueColumn.setCellValueFactory(new PropertyValueFactory<>("value"));
    valueColumn.setCellFactory(TextFieldTableCell.forTableColumn());
    valueColumn.setOnEditCommit(e -> {
      int rowIdx = paramsTable.getSelectionModel().getFocusedIndex();
      paramList.get(rowIdx).setValue(e.getNewValue());
    });

    paramList = FXCollections.observableArrayList();
    paramsTable.setItems(paramList);

    ok.setOnAction(e -> onOk());
    cancel.setOnAction(e -> closeWindow(cancel));
  }

  private void onOk() {
    callback.accept(paramList);
    closeWindow(ok);
  }

  public void setTarget(String targetName) {
    this.target.setText(targetName);
  }

  public void setCallback(Consumer<List<Param>> callback) {
    this.callback = callback;
  }

  public void populate(@Nullable Map<String, String> paramMap) {
    List<Param> li;
    if (paramMap != null) {
      li = new ArrayList<>();
      for (Map.Entry<String, String> entry : paramMap.entrySet()) {
        li.add(Param.of(entry.getKey(), entry.getValue()));
      }
      li.addAll(IntStream.range(0, Math.max(10 - paramMap.size(), 0))
          .mapToObj(i -> Param.newInstance())
          .collect(Collectors.toList()));
    } else {
      li = IntStream.range(0, 10).mapToObj(i -> Param.newInstance()).collect(Collectors.toList());
    }
    paramList.addAll(li);
  }
}
