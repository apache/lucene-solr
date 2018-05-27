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
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.search.Search;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class SortController {

  private static final String DEFAULT_ORDER = Order.ASC.name();

  private Search searchModel;

  @FXML
  private ChoiceBox<String> field1;

  private ObservableList<String> fieldList1;

  @FXML
  private ChoiceBox<String> field2;

  private ObservableList<String> fieldList2;

  @FXML
  private ChoiceBox<String> type1;

  private ObservableList<String> typeList1;

  @FXML
  private ChoiceBox<String> type2;

  private ObservableList<String> typeList2;

  @FXML
  private ChoiceBox<String> order1;

  private ObservableList<String> orderList1;

  @FXML
  private ChoiceBox<String> order2;

  private ObservableList<String> orderList2;

  @FXML
  private Button clear;

  @FXML
  private void initialize() {
    fieldList1 = FXCollections.observableArrayList();
    field1.setItems(fieldList1);
    field1.setOnAction(e -> runnableWrapper(() ->
        resetField(field1, type1, typeList1, order1)
    ));

    fieldList2 = FXCollections.observableArrayList();
    field2.setItems(fieldList2);
    field2.setOnAction(e -> runnableWrapper(() ->
        resetField(field2, type2, typeList2, order2)
    ));


    typeList1 = FXCollections.observableArrayList();
    type1.setItems(typeList1);

    typeList2 = FXCollections.observableArrayList();
    type2.setItems(typeList2);

    orderList1 = FXCollections.observableArrayList(Order.names());
    order1.setItems(orderList1);

    orderList2 = FXCollections.observableArrayList(Order.names());
    order2.setItems(orderList2);

    allClear();

    clear.setOnAction(e -> allClear());

  }

  private void allClear() {
    field1.setValue("");
    field2.setValue("");
    type1.setValue("");
    type2.setValue("");
    order1.setValue(DEFAULT_ORDER);
    order2.setValue(DEFAULT_ORDER);

    type1.setDisable(true);
    type2.setDisable(true);
    order1.setDisable(true);
    order2.setDisable(true);
  }

  private void resetField(ChoiceBox<String> field, ChoiceBox<String> type, ObservableList<String> typeList, ChoiceBox<String> order)
      throws LukeException {
    typeList.clear();
    if (Strings.isNullOrEmpty(field.getValue())) {
      type.setDisable(true);
      order.setValue(DEFAULT_ORDER);
      order.setDisable(true);
    } else {
      List<SortField> sortFields = searchModel.guessSortTypes(field.getValue());
      typeList.addAll(sortFields.stream()
          .map(sf -> {
            if (sf instanceof SortedNumericSortField) {
              return ((SortedNumericSortField) sf).getNumericType().name();
            } else {
              return sf.getType().name();
            }
          }).collect(Collectors.toList()));
      if (typeList.size() > 0) {
        type.setValue(typeList.get(0));
      }
      type.setDisable(false);
      order.setDisable(false);
    }
  }

  public void setSearchModel(Search searchModel) {
    this.searchModel = searchModel;
  }

  public void populateFields(Collection<String> fieldNames) {
    fieldList1.clear();
    fieldList1.add("");
    fieldList1.addAll(fieldNames);

    fieldList2.clear();
    fieldList2.add("");
    fieldList2.addAll(fieldNames);
  }

  public Sort getSort() throws LukeException {
    if (Strings.isNullOrEmpty(field1.getValue()) && Strings.isNullOrEmpty(field2.getValue())) {
      return null;
    }

    List<SortField> li = new ArrayList<>();
    if (!Strings.isNullOrEmpty(field1.getValue())) {
      searchModel.getSortType(field1.getValue(), type1.getValue(), isReverse(order1)).ifPresent(sf ->
          li.add(sf)
      );
    }
    if (!Strings.isNullOrEmpty(field2.getValue())) {
      searchModel.getSortType(field2.getValue(), type2.getValue(), isReverse(order2)).ifPresent(sf ->
          li.add(sf)
      );
    }
    return new Sort(li.toArray(new SortField[li.size()]));
  }

  private boolean isReverse(ChoiceBox order) {
    return Order.valueOf(order.getValue().toString()) == Order.DESC;
  }

  enum Order {
    ASC, DESC;

    static List<String> names() {
      return Arrays.stream(values()).map(Order::name).collect(Collectors.toList());
    }
  }
}
