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

package org.apache.lucene.luke.app.controllers.dialog;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import org.apache.lucene.luke.models.LukeException;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class ConfirmController implements DialogWindowController {

  @FXML
  private Label message;

  @FXML
  private Button ok;

  @FXML
  private Button cancel;

  @FXML
  private void initialize() {
    ok.setOnAction(e -> runnableWrapper(() -> {
      callback.exec();
      closeWindow(ok);
    }));
    cancel.setOnAction(e -> closeWindow(cancel));
  }

  public void setContent(String content) {
    message.setText(content);
  }

  private Callback callback = () -> {
  };

  public void setCallback(Callback callback) {
    this.callback = callback;
  }

  @FunctionalInterface
  public interface Callback {
    void exec() throws LukeException;
  }
}
