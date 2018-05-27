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

package org.apache.lucene.luke.app.desktop;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import org.apache.lucene.luke.app.DirectoryHandler;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.LukeModule;
import org.apache.lucene.luke.app.controllers.LukeController;
import org.apache.lucene.luke.app.util.MessageUtils;
import org.apache.lucene.luke.models.analysis.AnalysisFactory;
import org.apache.lucene.luke.models.commits.CommitsFactory;
import org.apache.lucene.luke.models.documents.DocumentsFactory;
import org.apache.lucene.luke.models.overview.OverviewFactory;
import org.apache.lucene.luke.models.search.SearchFactory;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;

import static org.apache.lucene.luke.app.util.ExceptionHandler.handle;

public class LukeMain extends Application {

  private LukeController mainController;

  @Override
  public void start(Stage primaryStage) throws Exception {

    Thread.setDefaultUncaughtExceptionHandler((thread, cause) ->
        handle(cause, mainController)
    );


    FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/luke.fxml"),
        MessageUtils.getBundle());
    Injector injector = LukeModule.getIngector();
    loader.setControllerFactory(injector::getInstance);

    Parent root = loader.load();
    this.mainController = loader.getController();
    primaryStage.setTitle(MessageUtils.getLocalizedMessage("window.title"));
    primaryStage.setScene(new Scene(root, 900, 650));
    primaryStage.getIcons().add(new Image("file:src/main/resources/img/lucene.gif"));
    primaryStage.show();

    mainController.resetStyles();
    mainController.showOpenIndexDialog();
  }

  public static void main(String[] args) {
    launch(args);
  }
}
