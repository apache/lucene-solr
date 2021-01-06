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

import java.awt.Color;
import java.awt.Font;

/** Constants for the default styles */
public class StyleConstants {

  public static Font FONT_BUTTON_LARGE = new Font("SanSerif", Font.PLAIN, 15);

  public static Font FONT_MONOSPACE_LARGE = new Font("monospaced", Font.PLAIN, 12);

  public static Color LINK_COLOR = Color.decode("#0099ff");

  public static Color DISABLED_COLOR = Color.decode("#d9d9d9");

  public static int TABLE_ROW_HEIGHT_DEFAULT = 18;

  public static int TABLE_COLUMN_MARGIN_DEFAULT = 10;

  public static int TABLE_ROW_MARGIN_DEFAULT = 3;

  private StyleConstants() {}
}
