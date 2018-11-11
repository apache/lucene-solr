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

import javax.swing.ImageIcon;
import java.awt.Image;

public class ImageUtils {

  private static final int DEFAULT_ICON_WIDTH = 20;

  private static final int DEFAULT_ICON_HEIGHT = 20;

  public static ImageIcon createImageIcon(String path, int width, int height) {
    return createImageIcon(path, "", width, height);
  }

  public static ImageIcon createImageIcon(String path, String description, int width, int height) {
    java.net.URL imgURL = ImageUtils.class.getResource(path);
    if (imgURL != null) {
      ImageIcon originalIcon = new ImageIcon(imgURL, description);
      ImageIcon icon = new ImageIcon(originalIcon.getImage().getScaledInstance(width, height, Image.SCALE_DEFAULT));
      return icon;
    } else {
      return null;
    }
  }

  private ImageUtils() {
  }
}
