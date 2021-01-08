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

import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import javax.swing.JLabel;
import org.apache.lucene.luke.models.LukeException;

/** JLabel extension for representing urls */
public final class URLLabel extends JLabel {

  private final URL link;

  public URLLabel(String text) {
    super(text);

    try {
      this.link = new URL(text);
    } catch (MalformedURLException e) {
      throw new LukeException(e.getMessage(), e);
    }

    setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

    addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            openUrl(link);
          }
        });
  }

  private void openUrl(URL link) {
    if (Desktop.isDesktopSupported()) {
      try {
        Desktop.getDesktop().browse(link.toURI());
      } catch (IOException | URISyntaxException e) {
        throw new LukeException(e.getMessage(), e);
      }
    }
  }
}
