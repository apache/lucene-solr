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
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextField;
import javafx.util.converter.IntegerStringConverter;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dto.documents.NewField;
import org.apache.lucene.luke.app.util.IntegerTextFormatter;

import java.util.Arrays;
import java.util.stream.Collectors;

public class IndexOptionsController implements DialogWindowController {

  private NewField nf;

  @FXML
  private CheckBox stored;

  @FXML
  private CheckBox tokenized;

  @FXML
  private CheckBox omitNorms;

  @FXML
  private ChoiceBox<String> idxOptions;

  private ObservableList<String> idxOptionList;

  @FXML
  private CheckBox storeTv;

  @FXML
  private CheckBox storeTvPos;

  @FXML
  private CheckBox storeTvOff;

  @FXML
  private CheckBox storeTvPay;

  @FXML
  private ChoiceBox<String> dvType;

  private ObservableList<String> dvTypeList;

  @FXML
  private TextField pvDimCount;

  @FXML
  private TextField pvDimNumB;

  @FXML
  private Button ok;

  @FXML
  private Button cancel;

  @FXML
  private void initialize() {
    idxOptionList = FXCollections.observableArrayList(
        Arrays.stream(IndexOptions.values()).map(IndexOptions::name).collect(Collectors.toList())
    );
    idxOptions.setItems(idxOptionList);

    dvTypeList = FXCollections.observableArrayList(
        Arrays.stream(DocValuesType.values()).map(DocValuesType::name).collect(Collectors.toList())
    );
    dvType.setItems(dvTypeList);

    pvDimCount.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), 0));

    pvDimNumB.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), 0));

    ok.setOnAction(e -> saveOptions());
    cancel.setOnAction(e -> closeWindow(cancel));
  }

  private void saveOptions() {
    nf.setStored(stored.isSelected());
    if (nf.getType().equals(Field.class)) {
      FieldType ftype = (FieldType) nf.getFieldType();
      ftype.setStored(stored.isSelected());
      ftype.setTokenized(tokenized.isSelected());
      ftype.setOmitNorms(omitNorms.isSelected());
      ftype.setIndexOptions(IndexOptions.valueOf(idxOptions.getValue()));
      ftype.setStoreTermVectors(storeTv.isSelected());
      ftype.setStoreTermVectorPositions(storeTvPay.isSelected());
      ftype.setStoreTermVectorOffsets(storeTvOff.isSelected());
      ftype.setStoreTermVectorPayloads(storeTvPay.isSelected());
    }
    closeWindow(ok);
  }

  void setNewField(NewField nf) {
    this.nf = nf;

    stored.setSelected(nf.isStored());

    IndexableFieldType fieldType = nf.getFieldType();
    tokenized.setSelected(fieldType.tokenized());
    omitNorms.setSelected(fieldType.omitNorms());
    idxOptions.setValue(fieldType.indexOptions().name());
    storeTv.setSelected(fieldType.storeTermVectors());
    storeTvPos.setSelected(fieldType.storeTermVectorPositions());
    storeTvOff.setSelected(fieldType.storeTermVectorOffsets());
    storeTvPay.setSelected(fieldType.storeTermVectorPayloads());
    dvType.setValue(fieldType.docValuesType().name());
    pvDimCount.setText(String.valueOf(fieldType.pointDimensionCount()));
    pvDimNumB.setText(String.valueOf(fieldType.pointNumBytes()));

    if (nf.getType().equals(org.apache.lucene.document.TextField.class) ||
        nf.getType().equals(StringField.class) ||
        nf.getType().equals(Field.class)) {
      stored.setDisable(false);
    } else {
      stored.setDisable(true);
      //stored.setStyle("-fx-opacity: 0.8");
    }

    if (nf.getType().equals(Field.class)) {
      tokenized.setDisable(false);
      omitNorms.setDisable(false);
      idxOptions.setDisable(false);
      storeTv.setDisable(false);
      storeTvPos.setDisable(false);
      storeTvOff.setDisable(false);
      storeTvPay.setDisable(false);
    } else {
      tokenized.setDisable(true);
      //tokenized.setStyle("-fx-opacity: 0.8");
      omitNorms.setDisable(true);
      //omitNorms.setStyle("-fx-opacity: 0.8");
      idxOptions.setDisable(true);
      //idxOptions.setStyle("-fx-opacity: 0.8");
      storeTv.setDisable(true);
      //storeTv.setStyle("-fx-opacity: 0.8");
      storeTvPos.setDisable(true);
      //storeTvPos.setStyle("-fx-opacity: 0.8");
      storeTvOff.setDisable(true);
      //storeTvOff.setStyle("-fx-opacity: 0.8");
      storeTvPay.setDisable(true);
      //storeTvPay.setStyle("-fx-opacity: 0.8");
    }

    // TODO
    dvType.setDisable(true);
    //dvType.setStyle("-fx-opacity: 0.8");
    pvDimCount.setDisable(true);
    //pvDimCount.setStyle("-fx-opacity: 0.8");
    pvDimNumB.setDisable(true);
    //pvDimNumB.setStyle("-fx-opacity: 0.8");
  }
}
