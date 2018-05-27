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
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.Pane;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;
import org.apache.lucene.luke.app.util.IndexTask;
import org.apache.lucene.luke.app.util.TextAreaPrintStream;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class CheckIndexController implements DialogWindowController {

  private static Logger logger = LoggerFactory.getLogger(CheckIndexController.class);

  private final IndexHandler indexHandler;

  private IndexTools toolsModel;

  private LukeState state;

  @FXML
  private Label dirPath;

  @FXML
  private Label results;

  @FXML
  private Button check;

  @FXML
  private Button repair;

  @FXML
  private Label repairWarn;

  @FXML
  private Button close;

  @FXML
  private Label status;

  @FXML
  private Pane indicatorPane;

  @FXML
  private TextArea info;

  private PrintStream ps;

  private CheckIndex.Status st;

  @Inject
  public CheckIndexController(IndexToolsFactory toolsModelFactory, DirectoryHandler directoryHandler, IndexHandler indexHandler) {
    this.indexHandler = indexHandler;

    if (directoryHandler.directoryOpened()) {
      this.state = directoryHandler.getState();
      this.toolsModel = toolsModelFactory.newInstance(state.getDirectory());
    } else if (indexHandler.indexOpened()) {
      this.state = indexHandler.getState();
      this.toolsModel = toolsModelFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
    } else {
      throw new LukeException("Directory or IndexReader is not opened");
    }
  }

  @FXML
  private void initialize() {
    dirPath.setText(state.getIndexPath());

    ps = new TextAreaPrintStream(info, new ByteArrayOutputStream(), logger);

    check.setOnAction(e -> checkIndex());
    repair.setOnAction(e -> repairIndex());
    repair.setDisable(true);
    repairWarn.setDisable(true);
    close.setOnAction(e -> closeWindow(close));
  }

  private void checkIndex() {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Task<CheckIndex.Status> task = new IndexTask<CheckIndex.Status>(indicatorPane) {
      @Override
      protected CheckIndex.Status call() throws Exception {
        try {
          return toolsModel.checkIndex(ps);
        } catch (Exception e) {
          Platform.runLater(() -> logger.error(e.getMessage(), e));
          throw e;
        } finally {
          ps.flush();
        }
      }
    };

    task.setOnSucceeded(e -> {
      CheckIndex.Status st = task.getValue();
      this.st = st;
      results.setText(createResultsMessage(st));
      if (!st.clean) {
        repair.setDisable(false);
        repairWarn.setDisable(false);
      }
    });
    status.textProperty().bind(task.messageProperty());

    executor.submit(task);
    executor.shutdown();
  }

  private String createResultsMessage(@Nullable CheckIndex.Status status) {
    String msg;
    if (status == null) {
      msg = "?";
    } else if (status.clean) {
      msg = "OK";
    } else if (status.toolOutOfDate) {
      msg = "ERROR: Can't check - tool out-of-date";
    } else {
      StringBuilder sb = new StringBuilder("BAD:");
      if (status.cantOpenSegments) {
        sb.append(" Can't open segmengs.");
      }
      if (status.missingSegments) {
        sb.append(" Missing segments.");
      }
      if (status.missingSegmentVersion) {
        sb.append(" Missing segment version.");
      }
      if (status.numBadSegments > 0) {
        sb.append(" numBadSegments=");
        sb.append(status.numBadSegments);
      }
      if (status.totLoseDocCount > 0) {
        sb.append(" totLoseDocCount=");
        sb.append(status.totLoseDocCount);
      }
      msg = sb.toString();
    }
    return msg;
  }

  private void repairIndex() {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Task task = new IndexTask<Void>(indicatorPane) {
      @Override
      protected Void call() {
        try {
          toolsModel.repairIndex(st, ps);
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
      indexHandler.open(state.getIndexPath(), state.getDirImpl());
      results.setText("?");
      repair.setDisable(true);
      repairWarn.setDisable(true);
    }));
    status.textProperty().bind(task.messageProperty());

    executor.submit(task);
    executor.shutdown();
  }

}
