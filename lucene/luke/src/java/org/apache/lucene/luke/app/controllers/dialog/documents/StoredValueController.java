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

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import org.apache.lucene.luke.app.controllers.dialog.DialogWindowController;

public class StoredValueController implements DialogWindowController {

  @FXML
  private Label field;

  @FXML
  private TextArea value;

  @FXML
  private Button copy;

  @FXML
  private Button close;


  @FXML
  private void initialize() {
    copy.setOnAction(e -> copyToClipboard());
    close.setOnAction(e -> closeWindow(close));
  }

  public void setValue(String fieldName, String stored) {
    field.setText(fieldName);
    value.setText(stored);
  }

  private void copyToClipboard() {
    Clipboard clipboard = Clipboard.getSystemClipboard();
    ClipboardContent content = new ClipboardContent();
    content.putString(value.getText());
    clipboard.setContent(content);
  }

}
