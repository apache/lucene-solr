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

import javax.swing.JTabbedPane;
import javax.swing.UIManager;
import java.awt.Graphics;

/** Tab utilities */
public class TabUtils {

  public static void forceTransparent(JTabbedPane tabbedPane) {
    String lookAndFeelClassName = UIManager.getLookAndFeel().getClass().getName();
    if (lookAndFeelClassName.contains("AquaLookAndFeel")) {
      // may be running on mac OS. nothing to do.
      return;
    }
    // https://coderanch.com/t/600541/java/JtabbedPane-transparency
    tabbedPane.setUI(new javax.swing.plaf.metal.MetalTabbedPaneUI() {
      protected void paintContentBorder(Graphics g, int tabPlacement, int selectedIndex) {
      }
    });
  }

  private TabUtils(){}
}
