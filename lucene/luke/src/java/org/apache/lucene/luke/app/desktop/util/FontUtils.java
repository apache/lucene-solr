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

import javax.swing.JLabel;
import java.awt.Font;
import java.awt.FontFormatException;
import java.awt.font.TextAttribute;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/** Font utilities */
public class FontUtils {

  public static final String TTF_RESOURCE_NAME = "org/apache/lucene/luke/app/desktop/font/ElegantIcons.ttf";

  @SuppressWarnings("unchecked")
  public static JLabel toLinkText(JLabel label) {
    label.setForeground(StyleConstants.LINK_COLOR);
    Font font = label.getFont();
    Map<TextAttribute, Object> attributes = (Map<TextAttribute, Object>) font.getAttributes();
    attributes.put(TextAttribute.UNDERLINE, TextAttribute.UNDERLINE_ON);
    label.setFont(font.deriveFont(attributes));
    return label;
  }

  public static Font createElegantIconFont() throws IOException, FontFormatException {
    InputStream is = FontUtils.class.getClassLoader().getResourceAsStream(TTF_RESOURCE_NAME);
    return Font.createFont(Font.TRUETYPE_FONT, is);
  }

  /**
   * Generates HTML text with embedded Elegant Icon Font.
   * See: https://www.elegantthemes.com/blog/resources/elegant-icon-font
   *
   * @param iconRef HTML numeric character reference of the icon
   */
  public static String elegantIconHtml(String iconRef) {
    return "<html><font face=\"ElegantIcons\">" + iconRef + "</font></html>";
  }

  /**
   * Generates HTML text with embedded Elegant Icon Font.
   *
   * @param iconRef HTML numeric character reference of the icon
   * @param text - HTML text
   */
  public static String elegantIconHtml(String iconRef, String text) {
    return "<html><font face=\"ElegantIcons\">" + iconRef + "</font>&nbsp;" + text + "</html>";
  }

  private FontUtils() {
  }

}
