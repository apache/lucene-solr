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
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.AnchorPane;

public class HelpController implements DialogWindowController {

  @FXML
  private Label description;

  @FXML
  private AnchorPane content;

  @FXML
  private Button close;

  @FXML
  private void initialize() {
    close.setOnAction(e -> closeWindow(close));
  }

  public void setDescription(String desc) {
    description.setText(desc);
  }

  public void setContent(Node child) {
    AnchorPane.setTopAnchor(child, 0.0);
    AnchorPane.setBottomAnchor(child, 0.0);
    AnchorPane.setLeftAnchor(child, 0.0);
    AnchorPane.setRightAnchor(child, 0.0);
    content.getChildren().add(child);
  }
}
