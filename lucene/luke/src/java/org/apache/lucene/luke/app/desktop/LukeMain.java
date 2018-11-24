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

import javax.swing.JFrame;
import javax.swing.JTextArea;
import javax.swing.UIManager;
import java.awt.GraphicsEnvironment;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TextAreaAppender;

import static org.apache.lucene.luke.app.desktop.util.ExceptionHandler.handle;

/** Luke entry class */
public class LukeMain {

  private static JFrame frame;

  public static JFrame getOwnerFrame() {
    return frame;
  }

  private static void createAndShowGUI() {
    Injector injector = DesktopModule.getIngector();

    // uncaught error handler
    MessageBroker messageBroker = injector.getInstance(MessageBroker.class);
    Thread.setDefaultUncaughtExceptionHandler((thread, cause) ->
        handle(cause, messageBroker)
    );

    // prepare log4j appender for Logs tab.
    JTextArea textArea = injector.getInstance(Key.get(JTextArea.class, Names.named("log_area")));
    TextAreaAppender.setTextArea(textArea);


    frame = injector.getInstance(JFrame.class);
    frame.setLocation(200, 100);
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.pack();
    frame.setVisible(true);

    // show open index dialog
    OpenIndexDialogFactory openIndexDialogFactory = injector.getInstance(OpenIndexDialogFactory.class);
    new DialogOpener<>(openIndexDialogFactory).open(MessageUtils.getLocalizedMessage("openindex.dialog.title"), 600, 420,
        (factory) -> {
        });
  }

  public static void main(String[] args) throws Exception {

    String lookAndFeelClassName = UIManager.getSystemLookAndFeelClassName();
    if (!lookAndFeelClassName.contains("AquaLookAndFeel") && !lookAndFeelClassName.contains("PlasticXPLookAndFeel")) {
      // may be running on linux platform
      lookAndFeelClassName = "javax.swing.plaf.metal.MetalLookAndFeel";
    }
    UIManager.setLookAndFeel(lookAndFeelClassName);

    GraphicsEnvironment genv = GraphicsEnvironment.getLocalGraphicsEnvironment();
    genv.registerFont(FontUtils.createElegantIconFont());

    javax.swing.SwingUtilities.invokeLater(LukeMain::createAndShowGUI);

  }
}
