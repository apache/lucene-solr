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


import com.google.inject.Injector;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.apache.lucene.luke.app.LukeModule;
import org.apache.lucene.luke.app.controllers.ChildTabController;
import org.apache.lucene.luke.app.controllers.LukeController;

import java.util.function.Consumer;

public class DialogOpener<T> {

  private Window owner;

  private String styleResourceName;

  public DialogOpener(LukeController parent) {
    this.owner = parent.getPrimaryWindow();
    this.styleResourceName = parent.getStyleResourceName();
  }

  public DialogOpener(ChildTabController controller) {
    this.owner = controller.getPrimaryWindow();
    this.styleResourceName = controller.getStyleResourceName();
  }

  public Stage show(Stage stage, String title, String resourceName, int width, int height, Consumer<? super T> initializer,
                    String... styleSheets)
      throws Exception {
    FXMLLoader loader = new FXMLLoader(DialogOpener.class.getResource(resourceName), MessageUtils.getBundle());
    Injector injector = LukeModule.getIngector();
    loader.setControllerFactory(injector::getInstance);

    Parent root = loader.load();
    initializer.accept(loader.getController());

    if (stage == null) {
      stage = new Stage();
      stage.initOwner(owner);
    }

    stage.setTitle(title);
    stage.setScene(new Scene(root, width, height));
    stage.getScene().getStylesheets().addAll(
        getClass().getResource("/styles/luke.css").toExternalForm(),
        getClass().getResource(styleResourceName).toExternalForm());
    for (String styleSheet : styleSheets) {
      stage.getScene().getStylesheets().add(getClass().getResource(styleSheet).toExternalForm());
    }
    stage.show();
    // move this window to the front
    stage.toFront();
    return stage;
  }

}
