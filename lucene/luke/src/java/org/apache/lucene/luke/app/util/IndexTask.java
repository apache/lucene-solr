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

package org.apache.lucene.luke.app.util;

import javafx.concurrent.Task;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.layout.Pane;

public abstract class IndexTask<T> extends Task<T> {

  private Pane indicatorPane;

  protected IndexTask(Pane indicatorPane) {
    this.indicatorPane = indicatorPane;
  }

  @Override
  protected void running() {
    updateMessage("Running...");
    ProgressIndicator pi = new ProgressIndicator();
    pi.setPrefHeight(20);
    pi.setPrefWidth(20);
    indicatorPane.getChildren().add(pi);
  }

  @Override
  protected void succeeded() {
    updateMessage("Done.");
    indicatorPane.getChildren().clear();
  }

  @Override
  protected void failed() {
    updateMessage(MessageUtils.getLocalizedMessage("message.error.unknown"));
    indicatorPane.getChildren().clear();
  }
}
