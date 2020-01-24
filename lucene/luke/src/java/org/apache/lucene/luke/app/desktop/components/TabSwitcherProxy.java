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

package org.apache.lucene.luke.app.desktop.components;

/** An utility class for switching tabs. */
public class TabSwitcherProxy {

  private static final TabSwitcherProxy instance = new TabSwitcherProxy();

  private TabSwitcher switcher;

  public static TabSwitcherProxy getInstance() {
    return instance;
  }

  public void set(TabSwitcher switcher) {
    if (this.switcher == null) {
      this.switcher = switcher;
    }
  }

  public void switchTab(TabbedPaneProvider.Tab tab) {
    if (switcher == null) {
      throw new IllegalStateException();
    }
    switcher.switchTab(tab);
  }

  /** tab switcher */
  public interface TabSwitcher {
    void switchTab(TabbedPaneProvider.Tab tab);
  }

}
