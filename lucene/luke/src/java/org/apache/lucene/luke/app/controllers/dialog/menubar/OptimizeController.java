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

package org.apache.lucene.luke.app.controllers.dialog.menubar;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextFormatter;
import javafx.scene.layout.Pane;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.util.IndexTask;
import org.apache.lucene.luke.app.util.IntegerTextFormatter;
import org.apache.lucene.luke.app.util.TextAreaPrintStream;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class OptimizeController implements DialogWindowController {

  private static final Logger logger = LoggerFactory.getLogger(OptimizeController.class);

  private final IndexToolsFactory toolsFactory;

  private final IndexHandler indexHandler;

  private IndexTools toolsModel;

  @FXML
  private Label dirPath;

  @FXML
  private CheckBox expunge;

  @FXML
  private Spinner<Integer> numSegments;

  @FXML
  private Button optimize;

  @FXML
  private Button close;

  @FXML
  private Label status;

  @FXML
  private Pane indicatorPane;

  @FXML
  private TextArea info;

  private PrintStream ps;

  @Inject
  public OptimizeController(IndexToolsFactory toolsFactory, IndexHandler indexHandler) {
    this.toolsFactory = toolsFactory;
    this.indexHandler = indexHandler;
    initIndexTools(indexHandler.getState());
  }

  @FXML
  private void initialize() {
    dirPath.setText(indexHandler.getState().getIndexPath());

    SpinnerValueFactory.IntegerSpinnerValueFactory valueFactory =
        new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 50, 1, -1);
    numSegments.setValueFactory(valueFactory);
    TextFormatter<Integer> textFormatter = new IntegerTextFormatter(valueFactory.getConverter(), 1);
    valueFactory.valueProperty().bindBidirectional(textFormatter.valueProperty());
    numSegments.getEditor().setTextFormatter(textFormatter);
    numSegments.focusedProperty().addListener((obs, oldV, newV) -> {
      if (newV) {
        // won't not change value, but commit editor
        // https://stackoverflow.com/questions/32340476/manually-typing-in-text-in-javafx-spinner-is-not-updating-the-value-unless-user
        numSegments.increment(0);
      }
    });

    ps = new TextAreaPrintStream(info, new ByteArrayOutputStream(), logger);

    optimize.setOnAction(e -> optimize());
    close.setOnAction(e -> closeWindow(close));
  }

  private void optimize() {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Task task = new IndexTask<Void>(indicatorPane) {

      @Override
      protected Void call() {
        try {
          toolsModel.optimize(expunge.isSelected(), numSegments.getValue(), ps);
        } catch (Exception e) {
          Platform.runLater(() -> logger.error(e.getMessage(), e));
          throw e;
        } finally {
          ps.flush();
        }
        return null;
      }
    };

    task.setOnSucceeded(e -> runnableWrapper(() -> {
      indexHandler.reOpen();
      initIndexTools(indexHandler.getState());
    }));
    status.textProperty().bind(task.messageProperty());

    executor.submit(task);
    executor.shutdown();
  }

  private void initIndexTools(LukeState state) {
    this.toolsModel = toolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
  }

}
