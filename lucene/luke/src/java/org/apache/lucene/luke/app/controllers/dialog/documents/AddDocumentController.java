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

import com.google.common.base.Strings;
import com.google.inject.Inject;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.ChoiceBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.StringConverter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.DocumentsController;
import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.controllers.dialog.HelpController;
import org.apache.lucene.luke.app.controllers.dto.documents.NewField;
import org.apache.lucene.luke.app.util.DialogOpener;
import org.apache.lucene.luke.app.util.NumericUtils;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.app.util.MessageUtils;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class AddDocumentController implements DialogWindowController {

  private static Logger logger = LoggerFactory.getLogger(AddDocumentController.class);

  private final IndexHandler indexHandler;

  private IndexTools toolsModel;

  private LukeController parent;

  private DocumentsController documentsController;

  private Analyzer currentAnalyzer;

  @Inject
  public AddDocumentController(IndexToolsFactory toolsFactory, IndexHandler indexHandler) {
    this.indexHandler = indexHandler;

    LukeState state = indexHandler.getState();
    this.toolsModel = toolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
  }

  @FXML
  private Label analyzerName;

  @FXML
  private Hyperlink changeAnalyzer;

  @FXML
  private TableView<NewField> fieldsTable;

  @FXML
  private TableColumn<NewField, Boolean> delColumn;

  @FXML
  private TableColumn<NewField, String> nameColumn;

  @FXML
  private TableColumn<NewField, Class> typeColumn;

  @FXML
  private TableColumn<NewField, Hyperlink> optColumn;

  @FXML
  private TableColumn<NewField, String> valueColumn;

  private ObservableList<NewField> newFieldList;

  @FXML
  private Button add;

  @FXML
  private Button cancel;

  @FXML
  private TextArea info;

  @FXML
  private void initialize() {
    changeAnalyzer.setOnAction(e -> {
      parent.switchTab(LukeController.Tab.ANALYZER);
      closeWindow(cancel);
    });

    delColumn.setCellValueFactory(data -> {
      BooleanProperty prop = data.getValue().deletedProperty();
      prop.addListener((obs, oldV, newV) ->
          fieldsTable.getSelectionModel().getSelectedItem().setDeleted(newV)
      );
      return prop;
    });
    delColumn.setCellFactory(col -> {
      CheckBoxTableCell<NewField, Boolean> cell = new CheckBoxTableCell<>();
      cell.setAlignment(Pos.CENTER);
      return cell;
    });

    nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
    nameColumn.setCellFactory(TextFieldTableCell.forTableColumn());
    nameColumn.setOnEditCommit(e -> {
      int rowIdx = fieldsTable.getSelectionModel().getFocusedIndex();
      newFieldList.get(rowIdx).setName(e.getNewValue());
    });

    ImageView imageView = new ImageView(new Image("/img/icon_question_alt2.png"));
    imageView.setFitWidth(12);
    imageView.setFitHeight(12);
    Hyperlink helpLink = new Hyperlink(MessageUtils.getLocalizedMessage("label.help"), imageView);
    helpLink.setOnMouseClicked(e -> runnableWrapper(this::showTypeHelpDialog));
    Label flagLabel = new Label("Type");
    flagLabel.setPadding(new Insets(0, 10, 0, 0));
    FlowPane flowPane = new FlowPane();
    flowPane.setOrientation(Orientation.HORIZONTAL);
    flowPane.setMaxWidth(Double.MAX_VALUE);
    flowPane.setAlignment(Pos.CENTER);
    flowPane.getChildren().addAll(flagLabel, helpLink);
    typeColumn.setGraphic(flowPane);

    typeColumn.setCellValueFactory(data -> {
      ObjectProperty<Class> prop = data.getValue().getTypeProperty();
      prop.addListener((obs, oldV, newV) -> {
        NewField selectedItem = fieldsTable.getSelectionModel().getSelectedItem();
        selectedItem.setType(newV);
        selectedItem.resetFieldType(newV);
        selectedItem.setStored(selectedItem.getFieldType().stored());
      });
      return prop;
    });
    typeColumn.setCellFactory(col -> {
      ChoiceBoxTableCell<NewField, Class> cell = new ChoiceBoxTableCell<>();
      cell.setConverter(new StringConverter<Class>() {
        @Override
        public String toString(Class clazz) {
          return clazz.getSimpleName();
        }

        @Override
        public Class fromString(String className) {
          try {
            return Class.forName(className);
          } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
            parent.showUnknownErrorMessage();
          }
          return null;
        }
      });
      cell.getItems().addAll(presetFields());
      return cell;
    });

    optColumn.setCellValueFactory(new PropertyValueFactory<>("option"));
    optColumn.setCellFactory(col -> {
      TableCell<NewField, Hyperlink> cell = new TableCell<NewField, Hyperlink>() {
        @Override
        protected void updateItem(Hyperlink link, boolean empty) {
          setGraphic(link);
          if (!empty) {
            int rowIdx = getTableRow().getIndex();
            link.setOnAction(e -> runnableWrapper(() -> showOptionsDialog(newFieldList.get(rowIdx))));
          }
        }
      };
      return cell;
    });

    valueColumn.setCellValueFactory(new PropertyValueFactory<>("value"));
    valueColumn.setCellFactory(TextFieldTableCell.forTableColumn());
    valueColumn.setOnEditCommit(e -> {
      int rowIdx = fieldsTable.getSelectionModel().getFocusedIndex();
      NewField selectedItem = newFieldList.get(rowIdx);
      selectedItem.setValue(e.getNewValue());
      selectedItem.resetFieldType(selectedItem.getType());
    });

    newFieldList = FXCollections.observableArrayList(
        IntStream.range(0, 50).mapToObj(i -> NewField.newInstance()).collect(Collectors.toList())
    );
    fieldsTable.setItems(newFieldList);

    add.setOnAction(e -> runnableWrapper(this::addDocument));
    cancel.setOnAction(e -> closeWindow(cancel));
  }

  private Stage typeHelpDialog;

  private void showTypeHelpDialog() throws Exception {
    ObservableList<String> typesList = FXCollections.observableArrayList(
        "TextField",
        "StringField",
        "IntPoint",
        "LongPoint",
        "FloatPoint",
        "DoublePoint",
        "SortedDocValuesField",
        "SortedSetDocValuesField",
        "NumericDocValuesField",
        "SortedNumericDocValuesField",
        "StoredField",
        "Field"
    );
    ChoiceBox<String> types = new ChoiceBox<>(typesList);

    TextArea details = new TextArea();
    details.setEditable(false);
    details.setWrapText(true);
    details.setPrefHeight(300);
    types.setOnAction(e -> {
      String detail = MessageUtils.getLocalizedMessage("help.fieldtype." + types.getValue());
      details.setText(detail);
    });
    types.setValue(typesList.get(0));

    Label description = new Label("Brief description and Examples");

    VBox vBox = new VBox();
    vBox.getChildren().addAll(types, description, details);
    vBox.setFillWidth(true);
    vBox.setSpacing(10);

    typeHelpDialog = new DialogOpener<HelpController>(parent).show(
        typeHelpDialog,
        "About type",
        "/fxml/dialog/help.fxml",
        500, 400,
        (controller) -> {
          controller.setDescription("Select Field Class:");
          controller.setContent(vBox);
        }
    );
  }

  private Stage optionsDialog;

  private void showOptionsDialog(NewField nf) throws Exception {
    optionsDialog = new DialogOpener<IndexOptionsController>(parent).show(
        optionsDialog,
        "Index Options for field: " + nf.getName(),
        "/fxml/dialog/documents/index_options.fxml",
        500, 500,
        (controller) -> {
          controller.setNewField(nf);
        },
        "/styles/index_options.css"
    );
  }

  private void addDocument() throws LukeException {
    List<NewField> validFields = newFieldList.stream()
        .filter(nf -> !nf.isDeleted())
        .filter(nf -> !Strings.isNullOrEmpty(nf.getName()))
        .filter(nf -> !Strings.isNullOrEmpty(nf.getValue()))
        .collect(Collectors.toList());
    if (validFields.isEmpty()) {
      info.setText("Please add one or more fields. Name and Value are both required.");
      return;
    }

    Document doc = new Document();
    try {
      for (NewField nf : validFields) {
        doc.add(toIndexableField(nf));
        System.out.println(doc);
      }
    } catch (NumberFormatException e) {
      logger.error(e.getMessage(), e);
      throw new LukeException("Invalid value: " + e.getMessage(), e);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new LukeException(e.getMessage(), e);
    }

    try {
      toolsModel.addDocument(doc, currentAnalyzer);
      indexHandler.reOpen();
      documentsController.displayLatestDoc();
      parent.switchTab(LukeController.Tab.DOCUMENTS);
      info.setText(MessageUtils.getLocalizedMessage("add_document.message.success"));
      add.setDisable(true);
    } catch (LukeException e) {
      info.setText(MessageUtils.getLocalizedMessage("add_document.message.fail"));
      throw e;
    } catch (Exception e) {
      info.setText(MessageUtils.getLocalizedMessage("add_document.message.fail"));
      throw new LukeException(e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  private IndexableField toIndexableField(NewField nf) throws Exception {
    if (nf.getType().equals(TextField.class) || nf.getType().equals(StringField.class)) {
      Field.Store store = nf.isStored() ? Field.Store.YES : Field.Store.NO;
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, String.class, Field.Store.class);
      return constr.newInstance(nf.getName(), nf.getValue(), store);
    } else if (nf.getType().equals(IntPoint.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, int[].class);
      int[] values = NumericUtils.convertToIntArray(nf.getValue(), false);
      return constr.newInstance(nf.getName(), values);
    } else if (nf.getType().equals(LongPoint.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, long[].class);
      long[] values = NumericUtils.convertToLongArray(nf.getValue(), false);
      return constr.newInstance(nf.getName(), values);
    } else if (nf.getType().equals(FloatPoint.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, float[].class);
      float[] values = NumericUtils.convertToFloatArray(nf.getValue(), false);
      return constr.newInstance(nf.getName(), values);
    } else if (nf.getType().equals(DoublePoint.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, double[].class);
      double[] values = NumericUtils.convertToDoubleArray(nf.getValue(), false);
      return constr.newInstance(nf.getName(), values);
    } else if (nf.getType().equals(SortedDocValuesField.class) ||
        nf.getType().equals(SortedSetDocValuesField.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, BytesRef.class);
      return constr.newInstance(nf.getName(), new BytesRef(nf.getValue()));
    } else if (nf.getType().equals(NumericDocValuesField.class) ||
        nf.getType().equals(SortedNumericDocValuesField.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, long.class);
      long value = NumericUtils.tryConvertToLongValue(nf.getValue());
      return constr.newInstance(nf.getName(), value);
    } else if (nf.getType().equals(StoredField.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, String.class);
      return constr.newInstance(nf.getName(), nf.getValue());
    } else if (nf.getType().equals(Field.class)) {
      Constructor<IndexableField> constr = nf.getType().getConstructor(String.class, String.class, IndexableFieldType.class);
      return constr.newInstance(nf.getName(), nf.getValue(), nf.getFieldType());
    } else {
      // TODO: unknown field
      return new StringField(nf.getName(), nf.getValue(), Field.Store.YES);
    }
  }

  public void setParent(LukeController parent, DocumentsController documentsController) {
    this.parent = parent;
    this.documentsController = documentsController;
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.currentAnalyzer = analyzer;
    analyzerName.setText(analyzer.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  private static List<Class<? extends Field>> presetFields() {
    final Class[] presetFieldClasses = new Class[]{
        TextField.class, StringField.class,
        IntPoint.class, LongPoint.class, FloatPoint.class, DoublePoint.class,
        SortedDocValuesField.class, SortedSetDocValuesField.class,
        NumericDocValuesField.class, SortedNumericDocValuesField.class,
        StoredField.class, Field.class
    };
    return Arrays.asList(presetFieldClasses);
  }


}
