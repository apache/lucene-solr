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
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.cell.ChoiceBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.util.StringConverter;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.luke.app.controllers.dto.search.PVField;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.search.QueryParserConfig;
import org.apache.lucene.queryparser.classic.QueryParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class QueryParserController {

  private static final String PARSER_STANDARD = "standard";
  private static final String PARSER_CLASSIC = "classic";

  private static final int ROW_HEIGHT = 30;

  private QueryParserConfig config = new QueryParserConfig.Builder().build();

  @FXML
  private ToggleGroup parserGroup;

  @FXML
  private ChoiceBox<String> defField;

  private ObservableList<String> defFieldList;

  @FXML
  private ChoiceBox<String> defOp;

  private ObservableList<String> defOpList;

  @FXML
  private CheckBox posIncr;

  @FXML
  private CheckBox leadWildcard;

  @FXML
  private CheckBox splitWs;

  @FXML
  private CheckBox genPq;

  @FXML
  private CheckBox genMTS;

  @FXML
  private TextField slop;

  @FXML
  private TextField fuzzyMinSim;

  @FXML
  private TextField fuzzyPrefLen;

  @FXML
  private ChoiceBox<String> dateRes;

  private ObservableList<String> dateResList;

  @FXML
  private TextField locale;

  @FXML
  private TextField timeZone;

  @FXML
  private TableView<PVField> pvFieldsTable;

  @FXML
  private TableColumn<PVField, String> fieldColumn;

  @FXML
  private TableColumn<PVField, PVField.Type> typeColumn;

  private ObservableList<PVField> pvFieldList;

  @FXML
  private void initialize() {
    parserGroup.selectedToggleProperty().addListener((obs, oldV, newV) -> {
      if (newV.getUserData().equals(PARSER_CLASSIC)) {
        splitWs.setDisable(false);
        genPq.setDisable(false);
        genMTS.setDisable(false);
        pvFieldsTable.setDisable(true);
      } else {
        splitWs.setDisable(true);
        genPq.setDisable(true);
        genMTS.setDisable(true);
        pvFieldsTable.setDisable(false);
      }
    });
    defFieldList = FXCollections.observableArrayList();
    defField.setItems(defFieldList);

    defOpList = FXCollections.observableArrayList(
        Arrays.stream(QueryParser.Operator.values())
            .map(QueryParser.Operator::name).collect(Collectors.toList()));
    defOp.setItems(defOpList);
    defOp.setValue(config.getDefaultOperator().name());

    posIncr.setSelected(config.isEnablePositionIncrements());
    leadWildcard.setSelected(config.isAllowLeadingWildcard());
    splitWs.setSelected(config.isSplitOnWhitespace());
    splitWs.setDisable(true);

    genPq.setSelected(config.isAutoGeneratePhraseQueries());
    genPq.setDisable(true);
    genMTS.setSelected(config.isAutoGenerateMultiTermSynonymsPhraseQuery());
    genMTS.setDisable(true);
    slop.setText(String.valueOf(config.getPhraseSlop()));

    fuzzyMinSim.setText(String.valueOf(config.getFuzzyMinSim()));
    fuzzyPrefLen.setText(String.valueOf(config.getFuzzyPrefixLength()));

    dateResList = FXCollections.observableArrayList(
        Arrays.stream(DateTools.Resolution.values())
            .map(DateTools.Resolution::name).collect(Collectors.toList())
    );
    dateRes.setItems(dateResList);
    dateRes.setValue(config.getDateResolution().name());

    locale.setText(config.getLocale().toString());
    timeZone.setText(config.getTimeZone().getID());

    fieldColumn.setCellValueFactory(new PropertyValueFactory<>("field"));
    typeColumn.setCellValueFactory(data -> data.getValue().getTypeProperty());
    typeColumn.setCellFactory(col -> {
      ChoiceBoxTableCell<PVField, PVField.Type> cell = new ChoiceBoxTableCell<>();
      cell.setConverter(new StringConverter<PVField.Type>() {
        @Override
        public String toString(PVField.Type type) {
          return type.name();
        }

        @Override
        public PVField.Type fromString(String name) {
          return PVField.Type.valueOf(name);
        }
      });
      cell.getItems().addAll(PVField.Type.values());
      return cell;
    });
    pvFieldList = FXCollections.observableArrayList();
    pvFieldsTable.setItems(pvFieldList);

  }

  public QueryParserConfig getConfig() throws LukeException {
    int phraseSlop;
    try {
      phraseSlop = Integer.parseInt(slop.getText());
    } catch (NumberFormatException e) {
      throw new LukeException("Invalid input for phrase slop: " + slop.getText(), e);
    }

    float fuzzyMinSimFloat;
    try {
      fuzzyMinSimFloat = Float.parseFloat(fuzzyMinSim.getText());
    } catch (NumberFormatException e) {
      throw new LukeException("Invalid input for fuzzy minimal similarity: " + fuzzyMinSim.getText(), e);
    }

    int fuzzyPrefLenInt;
    try {
      fuzzyPrefLenInt = Integer.parseInt(fuzzyPrefLen.getText());
    } catch (NumberFormatException e) {
      throw new LukeException("Invalid input for fuzzy prefix length: " + fuzzyPrefLen.getText(), e);
    }

    Map<String, Class<? extends Number>> typeMap = new HashMap<>();
    for (PVField pvField : pvFieldList) {
      switch (pvField.getType()) {
        case INT:
          typeMap.put(pvField.getField(), Integer.class);
          break;
        case LONG:
          typeMap.put(pvField.getField(), Long.class);
          break;
        case FLOAT:
          typeMap.put(pvField.getField(), Float.class);
          break;
        case DOUBLE:
          typeMap.put(pvField.getField(), Double.class);
          break;
        default:
          break;
      }
    }

    return new QueryParserConfig.Builder()
        .useClassicParser(parserGroup.getSelectedToggle().getUserData().equals(PARSER_CLASSIC))
        .defaultOperator(QueryParserConfig.Operator.valueOf(defOp.getValue()))
        .enablePositionIncrements(posIncr.isSelected())
        .allowLeadingWildcard(leadWildcard.isSelected())
        .splitOnWhitespace(splitWs.isSelected())
        .autoGeneratePhraseQueries(genPq.isSelected())
        .autoGenerateMultiTermSynonymsPhraseQuery(genMTS.isSelected())
        .phraseSlop(phraseSlop)
        .fuzzyMinSim(fuzzyMinSimFloat)
        .fuzzyPrefixLength(fuzzyPrefLenInt)
        .dateResolution(DateTools.Resolution.valueOf(dateRes.getValue()))
        .locale(new Locale(locale.getText()))
        .timeZone(TimeZone.getTimeZone(timeZone.getText()))
        .typeMap(typeMap)
        .build();
  }

  public String getDefField() {
    return defField.getValue();
  }

  public void populateFields(Collection<String> searchableFields, Collection<String> rangeSearchableFields) {
    defFieldList.clear();
    defFieldList.addAll(searchableFields);
    if (defFieldList.size() > 0) {
      defField.setValue(defFieldList.get(0));
    }

    pvFieldList.clear();
    pvFieldList.addAll(rangeSearchableFields.stream().map(PVField::of).collect(Collectors.toSet()));
    pvFieldsTable.setPrefHeight(Math.max(ROW_HEIGHT * pvFieldList.size(), 80));
  }
}
