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

package org.apache.lucene.luke.app.desktop.util;

import java.awt.Window;
import java.util.function.Consumer;
import javax.swing.JDialog;
import org.apache.lucene.luke.app.desktop.LukeMain;

/** An utility class for opening a dialog */
public class DialogOpener<T extends DialogOpener.DialogFactory> {

  private final T factory;

  public DialogOpener(T factory) {
    this.factory = factory;
  }

  public void open(
      String title, int width, int height, Consumer<? super T> initializer, String... styleSheets) {
    open(LukeMain.getOwnerFrame(), title, width, height, initializer, styleSheets);
  }

  public void open(
      Window owner,
      String title,
      int width,
      int height,
      Consumer<? super T> initializer,
      String... styleSheets) {
    initializer.accept(factory);
    JDialog dialog = factory.create(owner, title, width, height);
    dialog.setVisible(true);
  }

  /** factory interface to create a dialog */
  public interface DialogFactory {
    JDialog create(Window owner, String title, int width, int height);
  }
}
