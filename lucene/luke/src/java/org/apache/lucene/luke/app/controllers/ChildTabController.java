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

import javafx.stage.Window;

public abstract class ChildTabController {

  private LukeController parent;

  protected LukeController getParent() {
    return this.parent;
  }

  void setParent(LukeController parent) {
    this.parent = parent;
  }

  protected void switchTab(LukeController.Tab tab) {
    parent.switchTab(tab);
  }

  protected OverviewController getOverviewController() {
    return parent.getOverviewController();
  }

  protected DocumentsController getDocumentsController() {
    return parent.getDocumentsController();
  }

  protected SearchController getSearchController() {
    return parent.getSearchController();
  }

  protected void clearStatusMessage() {
    parent.clearStatusMessage();
  }

  protected void showStatusMessage(String message) {
    parent.showStatusMessage(message);
  }

  public Window getPrimaryWindow() {
    return parent.getPrimaryWindow();
  }

  public String getStyleResourceName() {
    return parent.getStyleResourceName();
  }

}
