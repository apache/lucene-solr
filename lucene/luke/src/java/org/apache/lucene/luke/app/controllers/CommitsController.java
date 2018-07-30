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

package org.apache.lucene.luke.app.controllers;

import com.google.inject.Inject;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.luke.app.DirectoryObserver;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.dto.commits.File;
import org.apache.lucene.luke.app.controllers.dto.commits.Segment;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.commits.Commit;
import org.apache.lucene.luke.models.commits.Commits;
import org.apache.lucene.luke.models.commits.CommitsFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class CommitsController implements IndexObserver, DirectoryObserver {

  private final CommitsFactory commitsFactory;

  private Commits commitsModel;

  @FXML
  private ChoiceBox<Long> generation;

  private ObservableList<Long> generationList;

  @FXML
  private Label deleted;

  @FXML
  private Label segCount;

  @FXML
  private TextArea userData;

  @FXML
  private TableView<File> filesTable;

  @FXML
  private TableColumn<File, String> fileNameColumn;

  @FXML
  private TableColumn<File, String> fileSizeColumn;

  private ObservableList<File> fileList;

  @FXML
  private TableView<Segment> segmentsTable;

  @FXML
  private TableColumn<Segment, String> segNameColumn;

  @FXML
  private TableColumn<Segment, Integer> maxDocColumn;

  @FXML
  private TableColumn<Segment, Integer> delsColumn;

  @FXML
  private TableColumn<Segment, Long> delGenColumn;

  @FXML
  private TableColumn<Segment, String> versionColumn;

  @FXML
  private TableColumn<Segment, String> codecColumn;

  @FXML
  private TableColumn<Segment, String> segSizeColumn;

  private ObservableList<Segment> segmentList;

  @FXML
  private ToggleGroup detailsGroup;

  @FXML
  private RadioButton diagRadio;

  @FXML
  private RadioButton attRadio;

  @FXML
  private RadioButton codecRadio;

  @FXML
  private ListView<String> segDetails;

  @FXML
  private ObservableList<String> segDetailList;

  @Inject
  public CommitsController(CommitsFactory commitsFactory) {
    this.commitsFactory = commitsFactory;
  }

  @FXML
  private void initialize() {
    generationList = FXCollections.observableArrayList();
    generation.setItems(generationList);
    generation.setOnAction(e -> runnableWrapper(this::selectCommit));

    fileNameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
    fileSizeColumn.setCellValueFactory(new PropertyValueFactory<>("size"));
    fileList = FXCollections.observableArrayList();
    filesTable.setItems(fileList);

    segNameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
    maxDocColumn.setCellValueFactory(new PropertyValueFactory<>("maxDoc"));
    delsColumn.setCellValueFactory(new PropertyValueFactory<>("delCount"));
    delGenColumn.setCellValueFactory(new PropertyValueFactory<>("delGen"));
    versionColumn.setCellValueFactory(new PropertyValueFactory<>("luceneVer"));
    codecColumn.setCellValueFactory(new PropertyValueFactory<>("codecName"));
    segSizeColumn.setCellValueFactory(new PropertyValueFactory<>("size"));
    segmentList = FXCollections.observableArrayList();
    segmentsTable.setItems(segmentList);
    segmentsTable.setOnMouseClicked(e -> {
      setDisableRadios(false);
      runnableWrapper(this::showSegmentDetails);
    });

    diagRadio.setSelected(true);
    diagRadio.setOnAction(e -> runnableWrapper(this::showSegmentDetails));
    attRadio.setDisable(true);
    attRadio.setOnAction(e -> runnableWrapper(this::showSegmentDetails));
    codecRadio.setDisable(true);
    codecRadio.setOnAction(e -> runnableWrapper(this::showSegmentDetails));
    setDisableRadios(true);

    segDetailList = FXCollections.observableArrayList();
    segDetails.setItems(segDetailList);
  }

  @Override
  public void openDirectory(LukeState state) {
    commitsModel = commitsFactory.newInstance(state.getDirectory(), state.getIndexPath());
    populateCommitGenerations();
  }

  @Override
  public void openIndex(LukeState state) {
    if (state.hasDirectoryReader()) {
      DirectoryReader dr = (DirectoryReader) state.getIndexReader();
      commitsModel = commitsFactory.newInstance(dr, state.getIndexPath());
      populateCommitGenerations();
    }
  }

  @Override
  public void closeIndex() {
    close();
  }

  @Override
  public void closeDirectory() {
    close();
  }

  private void close() {
    commitsModel = null;

    deleted.setText("");
    segCount.setText("");
    userData.setText("");
    generation.setValue(null);
    generationList.clear();
    fileList.clear();
    segmentList.clear();
  }

  private void populateCommitGenerations() {
    generationList.clear();
    for (Commit commit : commitsModel.listCommits()) {
      generationList.add(commit.getGeneration());
    }
    if (generationList.size() > 0) {
      generation.setValue(generationList.get(0));
    }
  }

  private void selectCommit() throws LukeException {
    if (generation.getValue() == null) {
      return;
    }

    deleted.setText("");
    segCount.setText("");
    userData.setText("");
    fileList.clear();
    segmentList.clear();
    segDetailList.clear();

    setDisableRadios(true);

    long commitGen = generation.getValue();
    commitsModel.getCommit(commitGen).ifPresent(commit -> {
      deleted.setText(String.valueOf(commit.isDeleted()));
      segCount.setText(String.valueOf(commit.getSegCount()));
      userData.setText(commit.getUserData());
    });

    fileList.addAll(commitsModel.getFiles(commitGen).stream()
        .map(File::of).collect(Collectors.toList()));

    segmentList.addAll(commitsModel.getSegments(commitGen).stream()
        .map(Segment::of).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  private void showSegmentDetails() throws LukeException {
    if (generation.getValue() == null ||
        segmentsTable.getSelectionModel().getSelectedItem() == null) {
      return;
    }
    segDetailList.clear();

    long commitGen = generation.getValue();
    String segName = segmentsTable.getSelectionModel().getSelectedItem().getName();
    String selected = detailsGroup.getSelectedToggle().getUserData().toString();

    if (selected.equals("diag")) {
      segDetailList.addAll(commitsModel.getSegmentDiagnostics(commitGen, segName).entrySet().stream()
          .map(e -> e.getKey() + " = " + e.getValue()).collect(Collectors.toList()));
    } else if (selected.equals("att")) {
      segDetailList.addAll(commitsModel.getSegmentAttributes(commitGen, segName).entrySet().stream()
          .map(e -> e.getKey() + " = " + e.getValue()).collect(Collectors.toList()));
    } else if (selected.equals("codec")) {
      commitsModel.getSegmentCodec(commitGen, segName).ifPresent(codec -> {
        Map<String, String> map = new HashMap<>();
        map.put("Codec name", codec.getName());
        map.put("Codec class name", codec.getClass().getName());
        map.put("Compound format", codec.compoundFormat().getClass().getName());
        map.put("DocValues format", codec.docValuesFormat().getClass().getName());
        map.put("FieldInfos format", codec.fieldInfosFormat().getClass().getName());
        map.put("LiveDocs format", codec.liveDocsFormat().getClass().getName());
        map.put("Norms format", codec.normsFormat().getClass().getName());
        map.put("Points format", codec.pointsFormat().getClass().getName());
        map.put("Postings format", codec.postingsFormat().getClass().getName());
        map.put("SegmentInfo format", codec.segmentInfoFormat().getClass().getName());
        map.put("StoredFields format", codec.storedFieldsFormat().getClass().getName());
        map.put("TermVectors format", codec.termVectorsFormat().getClass().getName());
        segDetailList.addAll(
            map.entrySet().stream()
                .map(e -> e.getKey() + " = " + e.getValue())
                .collect(Collectors.toList())
        );
      });
    }

  }

  private void setDisableRadios(boolean value) {
    diagRadio.setDisable(value);
    attRadio.setDisable(value);
    codecRadio.setDisable(value);
  }

}
